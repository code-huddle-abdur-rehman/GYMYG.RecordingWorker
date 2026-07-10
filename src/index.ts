import 'dotenv/config';
import { Queue, Worker } from 'bullmq';
import { buildRedisConnection } from './redis.js';
import {
  cleanupRecordingTmpDir,
  getRecordingTmpDir,
  RecordingSessionManager,
} from './recordingSessionManager.js';
import { collectSystemResources, logFileDescriptorLimits } from './diagnostics.js';
import {
  classRecordingStartQueue,
  classRecordingStopQueue,
  RECORDING_ROLES,
  type RecordingRole,
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

const connection = buildRedisConnection(redisUrl);
const sessionManager = new RecordingSessionManager();
const joinAs = sessionManager.joinAs;
const queueRoles: RecordingRole[] =
  joinAs === 'all' ? [...RECORDING_ROLES] : [joinAs];

console.log(`[RecordingWorker] JOIN_AS=${joinAs}`);
for (const role of queueRoles) {
  console.log(
    `[RecordingWorker] Queues: ${classRecordingStartQueue(role)}, ${classRecordingStopQueue(role)}`,
  );
}

const startJobsInFlight = new Map<number, Promise<void>>();
const stopJobsInFlight = new Set<number>();
const queues: Queue[] = [];
const workers: Worker[] = [];

let shuttingDown = false;

async function shutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[RecordingWorker] ${signal} received — closing browsers...`);
  await sessionManager.closeAllSessions();
  await Promise.all([
    ...queues.map((queue) => queue.close()),
    ...workers.map((worker) => worker.close()),
  ]);
  process.exit(0);
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

async function waitForActiveSession(
  workoutClassId: number,
  startQueue: Queue,
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

for (const queueRole of queueRoles) {
  const startQueueName = classRecordingStartQueue(queueRole);
  const stopQueueName = classRecordingStopQueue(queueRole);
  const startQueue = new Queue(startQueueName, { connection });
  const stopQueue = new Queue(stopQueueName, { connection });
  queues.push(startQueue, stopQueue);

  const startWorker = new Worker(
    startQueueName,
    async (job) => {
      const { workoutClassId } = job.data as { workoutClassId: number; jobId: string };

      if (sessionManager.hasSession(workoutClassId)) {
        console.log(
          `[RecordingWorker] Class ${workoutClassId} already recording — ignoring duplicate ${queueRole} start job`,
        );
        return;
      }

      const existingStart = startJobsInFlight.get(workoutClassId);
      if (existingStart) {
        console.log(
          `[RecordingWorker] Start already in flight for class ${workoutClassId} — waiting on ${queueRole} start job`,
        );
        await existingStart;
        return;
      }

      console.log(
        `[RecordingWorker] Start ${joinAs} recording for class ${workoutClassId} (triggered by ${queueRole} queue)`,
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

      if (stopJobsInFlight.has(workoutClassId)) {
        console.log(
          `[RecordingWorker] Stop already in progress for class ${workoutClassId} — ignoring duplicate ${queueRole} stop job`,
        );
        return;
      }

      console.log(
        `[RecordingWorker] Stop ${joinAs} recording for class ${workoutClassId} (triggered by ${queueRole} queue)`,
      );

      const hasSession = await waitForActiveSession(workoutClassId, startQueue);
      if (!hasSession) {
        console.warn(
          `[RecordingWorker] No active ${joinAs} session for class ${workoutClassId} — was the worker running for the full class?`,
        );
        await sessionManager.notifySessionMissing(workoutClassId, queueRole);
        return;
      }

      stopJobsInFlight.add(workoutClassId);
      try {
        await sessionManager.stopClassRecording(workoutClassId);
      } catch (err) {
        console.error(
          `[RecordingWorker] Stop processing failed for class ${workoutClassId}:`,
          err instanceof Error ? err.message : err,
        );
        await sessionManager.forceCloseSession(workoutClassId);
        throw err;
      } finally {
        stopJobsInFlight.delete(workoutClassId);
      }
    },
    STOP_WORKER_OPTS,
  );

  workers.push(startWorker, stopWorker);

  startWorker.on('error', (err) => {
    console.error(`[RecordingWorker] Start worker error (${queueRole}):`, err);
  });
  startWorker.on('stalled', (jobId) => {
    console.warn(`[RecordingWorker] Start worker stalled job ${jobId} (${queueRole})`);
  });
  startWorker.on('failed', (job, err) => {
    console.error(
      `[RecordingWorker] Start worker failed job ${job?.id} (${queueRole}):`,
      err,
    );
  });
  startWorker.on('completed', (job) => {
    console.log(`[RecordingWorker] Start worker completed job ${job.id} (${queueRole})`);
  });

  stopWorker.on('error', (err) => {
    console.error(`[RecordingWorker] Stop worker error (${queueRole}):`, err);
  });
  stopWorker.on('stalled', (jobId) => {
    console.warn(`[RecordingWorker] Stop worker stalled job ${jobId} (${queueRole})`);
  });
  stopWorker.on('failed', (job, err) => {
    console.error(
      `[RecordingWorker] Stop worker failed job ${job?.id} (${queueRole}):`,
      err,
    );
  });
  stopWorker.on('completed', (job) => {
    console.log(`[RecordingWorker] Stop worker completed job ${job.id} (${queueRole})`);
  });

  startWorker.on('ready', () =>
    console.log(`[RecordingWorker][redis] start worker ready (${queueRole})`),
  );
  stopWorker.on('ready', () =>
    console.log(`[RecordingWorker][redis] stop worker ready (${queueRole})`),
  );

  void attachRedisConnectionLogging(`start-queue-${queueRole}`, startQueue.client);
  void attachRedisConnectionLogging(`stop-queue-${queueRole}`, stopQueue.client);
  void attachRedisConnectionLogging(`start-worker-${queueRole}`, startWorker.client);
  void attachRedisConnectionLogging(`stop-worker-${queueRole}`, stopWorker.client);
}

// ── Redis connection lifecycle logging ────────────────────────────────────────
// Surfaces the underlying ioredis connection state so a Redis failover is
// clearly visible in the logs (connect → close → reconnecting → ready), rather
// than only showing up as a burst of "worker error: READONLY" lines. The
// reconnectOnError handler in redis.ts logs the moment a failover is detected;
// these listeners show the recovery that follows.
type RedisEventClient = {
  on(event: string, listener: (...args: unknown[]) => void): unknown;
  status?: string;
};

async function attachRedisConnectionLogging(
  label: string,
  clientPromise: Promise<RedisEventClient>,
): Promise<void> {
  try {
    const client = await clientPromise;
    client.on('connect', () =>
      console.log(`[RecordingWorker][redis] ${label} socket connecting...`),
    );
    client.on('ready', () =>
      console.log(`[RecordingWorker][redis] ${label} connection ready`),
    );
    client.on('reconnecting', (delay: unknown) =>
      console.warn(
        `[RecordingWorker][redis] ${label} reconnecting${
          typeof delay === 'number' ? ` (next attempt in ${delay}ms)` : ''
        }...`,
      ),
    );
    client.on('close', () =>
      console.warn(`[RecordingWorker][redis] ${label} connection closed`),
    );
    client.on('end', () =>
      console.warn(
        `[RecordingWorker][redis] ${label} connection ended (no more reconnects)`,
      ),
    );
    client.on('error', (err: unknown) =>
      console.error(
        `[RecordingWorker][redis] ${label} connection error:`,
        err instanceof Error ? err.message : err,
      ),
    );
  } catch (err) {
    console.error(
      `[RecordingWorker][redis] Failed to attach connection logging for ${label}:`,
      err instanceof Error ? err.message : err,
    );
  }
}

console.log(
  `[RecordingWorker] Listening on ${queueRoles.length} start/stop queue pair(s)`,
);
