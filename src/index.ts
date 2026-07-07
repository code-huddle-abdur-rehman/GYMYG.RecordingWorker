import 'dotenv/config';
import { Queue, Worker } from 'bullmq';
import { parseRedisUrl } from './redis.js';
import {
  cleanupRecordingTmpDir,
  getRecordingTmpDir,
  RecordingSessionManager,
} from './recordingSessionManager.js';
import { collectSystemResources, logFileDescriptorLimits } from './diagnostics.js';
import {
  classRecordingStartQueue,
  classRecordingStopQueue,
} from './queues.js';

// ── File-descriptor limit check ───────────────────────────────────────────────
// WebRTC (via Daily.co) opens a UDP socket for every ICE candidate pair
// (interface × STUN server × media track).  On a busy call that can be 50–200
// sockets per Chromium renderer.  If the process nofile soft-limit is the
// Linux default of 1024, Chromium will hit ERR_INSUFFICIENT_RESOURCES and crash
// the renderer within seconds of joining the call.
//
// The correct long-term fix is to add `LimitNOFILE=65536` to the systemd
// service unit (or run the process with `ulimit -n 65536`).  These logs record
// the current limits at startup so you can confirm they are high enough before
// recording starts.
await logFileDescriptorLimits('startup');
console.log(`[RecordingWorker] Recording temp dir: ${getRecordingTmpDir()}`);
await cleanupRecordingTmpDir();
void collectSystemResources().then((res) =>
  console.log('[RecordingWorker][diag] startup system resources', {
    platform: res.platform,
    loadAvg: res.loadAvg,
    totalMemMb: res.totalMemMb,
    freeMemMb: res.freeMemMb,
    nodeFdCount: res.nodeFdCount,
    nodeFdLimit: res.nodeFdLimit,
    diskUsage: res.diskUsage,
  }),
);

const redisUrl = process.env.REDIS_URL;
if (!redisUrl) {
  throw new Error('REDIS_URL is required');
}

const connection = parseRedisUrl(redisUrl);
const sessionManager = new RecordingSessionManager();
const joinAs = sessionManager.joinAs;
const startQueueName = classRecordingStartQueue(joinAs);
const stopQueueName = classRecordingStopQueue(joinAs);

console.log(`[RecordingWorker] JOIN_AS=${joinAs}`);
console.log(`[RecordingWorker] Start queue: ${startQueueName}`);
console.log(`[RecordingWorker] Stop queue: ${stopQueueName}`);

const startJobsInFlight = new Map<number, Promise<void>>();
const startQueue = new Queue(startQueueName, { connection });
const stopQueue = new Queue(stopQueueName, { connection });

let shuttingDown = false;

async function shutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[RecordingWorker] ${signal} received — closing browsers...`);
  await sessionManager.closeAllSessions();
  await Promise.all([startQueue.close(), stopQueue.close()]);
  process.exit(0);
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

async function waitForActiveSession(
  workoutClassId: number,
  maxWaitMs = 180000,
): Promise<boolean> {
  const deadline = Date.now() + maxWaitMs;

  while (Date.now() < deadline) {
    if (sessionManager.hasSession(workoutClassId)) {
      return true;
    }

    const inFlightStart = startJobsInFlight.get(workoutClassId);
    if (inFlightStart) {
      try {
        await inFlightStart;
      } catch {
        return false;
      }
      if (sessionManager.hasSession(workoutClassId)) {
        return true;
      }
    }

    const pendingJobs = await startQueue.getJobs(['waiting', 'active', 'delayed']);
    const classStartJobs = pendingJobs.filter(
      (job) => job.data?.workoutClassId === workoutClassId,
    );
    if (classStartJobs.length === 0) {
      break;
    }

    console.log(
      `[RecordingWorker] Waiting for start job to finish for class ${workoutClassId}...`,
    );
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }

  return sessionManager.hasSession(workoutClassId);
}

console.log('[RecordingWorker] Starting workers...');

const START_WORKER_OPTS = {
  connection,
  concurrency: 1,
  // Start jobs can involve browser launch/login and may run for minutes.
  // Keep a generous lock to reduce false stalls during transient Redis jitter.
  lockDuration: 300_000,
  stalledInterval: 60_000,
  maxStalledCount: 2,
};

const STOP_WORKER_OPTS = {
  connection,
  concurrency: 1,
  // Stop jobs can be lengthy (close contexts, collect audio, mux, upload to S3).
  // A longer lock window prevents "could not renew lock" on slow hosts.
  lockDuration: 900_000,
  stalledInterval: 60_000,
  maxStalledCount: 2,
};

const startWorker = new Worker(
  startQueueName,
  async (job) => {
    const { workoutClassId } = job.data as { workoutClassId: number; jobId: string };
    console.log(
      `[RecordingWorker] Start ${joinAs} recording for class ${workoutClassId}`,
    );
    const startPromise = sessionManager.startClassRecording(workoutClassId);
    startJobsInFlight.set(workoutClassId, startPromise);
    try {
      await startPromise;
      console.log(
        `[RecordingWorker] ${joinAs} recording session ready for class ${workoutClassId}`,
      );
    } catch (err) {
      console.error(
        `[RecordingWorker] Failed to start ${joinAs} recording for class ${workoutClassId}:`,
        err instanceof Error ? err.message : err,
      );
      await sessionManager.forceCloseSession(workoutClassId);
      throw err;
    } finally {
      startJobsInFlight.delete(workoutClassId);
    }
  },
  START_WORKER_OPTS,
);

const stopWorker = new Worker(
  stopQueueName,
  async (job) => {
    const { workoutClassId } = job.data as { workoutClassId: number; jobId: string };
    console.log(
      `[RecordingWorker] Stop ${joinAs} recording for class ${workoutClassId}`,
    );

    const hasSession = await waitForActiveSession(workoutClassId);
    if (!hasSession) {
      console.warn(
        `[RecordingWorker] No active ${joinAs} session for class ${workoutClassId} — was the worker running for the full class?`,
      );
      await sessionManager.notifySessionMissing(workoutClassId);
      return;
    }

    try {
      await sessionManager.stopClassRecording(workoutClassId);
    } catch (err) {
      console.error(
        `[RecordingWorker] Stop processing failed for class ${workoutClassId}:`,
        err instanceof Error ? err.message : err,
      );
      await sessionManager.forceCloseSession(workoutClassId);
      throw err;
    }
  },
  STOP_WORKER_OPTS,
);

startWorker.on('error', (err) => {
  console.error('[RecordingWorker] Start worker error:', err);
});
startWorker.on('stalled', (jobId) => {
  console.warn(`[RecordingWorker] Start worker stalled job ${jobId}`);
});
startWorker.on('failed', (job, err) => {
  console.error(`[RecordingWorker] Start worker failed job ${job?.id}:`, err);
});
startWorker.on('completed', (job) => {
  console.log(`[RecordingWorker] Start worker completed job ${job.id}`);
});

stopWorker.on('error', (err) => {
  console.error('[RecordingWorker] Stop worker error:', err);
});
stopWorker.on('stalled', (jobId) => {
  console.warn(`[RecordingWorker] Stop worker stalled job ${jobId}`);
});
stopWorker.on('failed', (job, err) => {
  console.error(`[RecordingWorker] Stop worker failed job ${job?.id}:`, err);
});
stopWorker.on('completed', (job) => {
  console.log(`[RecordingWorker] Stop worker completed job ${job.id}`);
});

console.log(
  `[RecordingWorker] Listening on ${startQueueName} and ${stopQueueName}`,
);
