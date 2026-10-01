import 'dotenv/config';
import { Queue, Worker } from 'bullmq';
import { v4 as uuidv4 } from 'uuid';
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

// How long the BullMQ lock is held before it must be renewed.  This must
// comfortably exceed the worst-case start-job duration so the lock never
// expires while the processor is still running.
//
// Worst-case timeline:
//   navigate + waitForSessionJoined (attempt 1) : SESSION_JOIN_TIMEOUT_MS = 180 s
//   retry delay                                 :                           5 s
//   navigate + waitForSessionJoined (attempt 2) : SESSION_JOIN_TIMEOUT_MS = 180 s
//   ─────────────────────────────────────────────────────────────────────────────
//   total                                       :                         ≈ 365 s
//
// With lockDuration = 900 s the automatic renewal fires at ~450 s — well after
// the job is done in the typical case.  The per-job heartbeat below (every 60 s)
// provides a second layer of protection when the host is under heavy load
// (e.g. a parallel special-event Chromium + ffmpeg stop running at the same time).
const START_LOCK_DURATION = 900_000;

const START_WORKER_OPTS = {
  connection,
  concurrency: 1,
  lockDuration: START_LOCK_DURATION,
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
    async (job, token) => {
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

      // Heartbeat: extend the lock every 60 s so a slow join (Chromium launch,
      // Daily.co WebRTC negotiation) under host load never lets the lock expire
      // between the automatic LockManager renewal ticks.  This is especially
      // important when a parallel special-event stop (ffmpeg + S3) is competing
      // for CPU/memory on the same host.
      const lockHeartbeat = setInterval(() => {
        if (!token) return;
        job.extendLock(token, START_LOCK_DURATION).catch((err: unknown) => {
          console.warn(
            `[RecordingWorker] Failed to extend start lock for class ${workoutClassId}:`,
            err instanceof Error ? err.message : err,
          );
        });
      }, 60_000);

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
        clearInterval(lockHeartbeat);
        startJobsInFlight.delete(workoutClassId);
      }
    },
    START_WORKER_OPTS,
  );

  const stopWorker = new Worker(
    stopQueueName,
    async (job) => {
      const { workoutClassId, reason } = job.data as {
        workoutClassId: number;
        jobId: string;
        reason?: 'idle';
      };
      const isIdleStop = reason === 'idle';

      if (stopJobsInFlight.has(workoutClassId)) {
        console.log(
          `[RecordingWorker] Stop already in progress for class ${workoutClassId} — ignoring duplicate ${queueRole} stop job`,
        );
        return;
      }

      console.log(
        `[RecordingWorker] Stop ${joinAs} recording for class ${workoutClassId} (triggered by ${queueRole} queue, reason: ${reason ?? 'server'})`,
      );

      if (isIdleStop) {
        // Queued by this worker; the class may have been stopped normally or
        // a user may have come back while the job waited behind other stops.
        if (!sessionManager.hasSession(workoutClassId)) {
          console.log(
            `[RecordingWorker] Idle stop for class ${workoutClassId} skipped — session already closed`,
          );
          return;
        }
        if (!sessionManager.isIdleStopStillWanted(workoutClassId)) {
          console.log(
            `[RecordingWorker] Idle stop for class ${workoutClassId} cancelled — a real user rejoined`,
          );
          return;
        }
      }

      const hasSession =
        isIdleStop || (await waitForActiveSession(workoutClassId, startQueue));
      if (!hasSession) {
        if (sessionManager.wasSelfStopped(workoutClassId)) {
          console.log(
            `[RecordingWorker] Class ${workoutClassId} was already stopped by this worker (idle/watchdog) — ignoring late ${queueRole} stop job`,
          );
          return;
        }
        console.warn(
          `[RecordingWorker] No active ${joinAs} session for class ${workoutClassId} — was the worker running for the full class?`,
        );
        await sessionManager.notifySessionMissing(workoutClassId, queueRole);
        return;
      }

      stopJobsInFlight.add(workoutClassId);
      try {
        await sessionManager.stopClassRecording(
          workoutClassId,
          isIdleStop ? 'idle' : 'job',
        );
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

  // Idle stops go through this worker's own stop queue (the first one when
  // JOIN_AS=all, since any of them stops the whole session) so they share the
  // duplicate guard, lock settings and upload path of a normal stop.
  if (queueRole === queueRoles[0]) {
    sessionManager.setIdleStopHandler(async (workoutClassId) => {
      const jobId = uuidv4();
      await stopQueue.add(
        'stop',
        { workoutClassId, jobId, reason: 'idle' },
        {
          jobId: `recording-idle-stop-${queueRole}-${workoutClassId}-${jobId}`,
          priority: 1,
        },
      );
    });
  }

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
