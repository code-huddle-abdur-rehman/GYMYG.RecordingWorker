import 'dotenv/config';
import { Queue, Worker } from 'bullmq';
import { parseRedisUrl } from './redis.js';
import { RecordingSessionManager } from './recordingSessionManager.js';
import {
  classRecordingStartQueue,
  classRecordingStopQueue,
} from './queues.js';

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

new Worker(
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
  { connection, concurrency: 1 },
);

new Worker(
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

    await sessionManager.stopClassRecording(workoutClassId);
  },
  { connection, concurrency: 1 },
);

console.log(
  `[RecordingWorker] Listening on ${startQueueName} and ${stopQueueName}`,
);
