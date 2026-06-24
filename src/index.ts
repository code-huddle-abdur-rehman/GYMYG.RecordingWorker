import 'dotenv/config';
import { Queue, Worker } from 'bullmq';
import { parseRedisUrl } from './redis.js';
import { RecordingSessionManager } from './recordingSessionManager.js';

const QUEUE_CLASS_RECORDING_START = 'class-recording-start';
const QUEUE_CLASS_RECORDING_STOP = 'class-recording-stop';

const redisUrl = process.env.REDIS_URL;
if (!redisUrl) {
  throw new Error('REDIS_URL is required');
}

const connection = parseRedisUrl(redisUrl);
const sessionManager = new RecordingSessionManager();
const startJobsInFlight = new Map<number, Promise<void>>();
const startQueue = new Queue(QUEUE_CLASS_RECORDING_START, { connection });

let shuttingDown = false;

async function shutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[RecordingWorker] ${signal} received — closing browsers...`);
  await sessionManager.closeAllSessions();
  await startQueue.close();
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
  QUEUE_CLASS_RECORDING_START,
  async (job) => {
    const { workoutClassId } = job.data as { workoutClassId: number; jobId: string };
    console.log(`[RecordingWorker] Start recording for class ${workoutClassId}`);
    const startPromise = sessionManager.startClassRecording(workoutClassId);
    startJobsInFlight.set(workoutClassId, startPromise);
    try {
      await startPromise;
      console.log(`[RecordingWorker] Recording session ready for class ${workoutClassId}`);
    } catch (err) {
      console.error(
        `[RecordingWorker] Failed to start recording for class ${workoutClassId}:`,
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
  QUEUE_CLASS_RECORDING_STOP,
  async (job) => {
    const { workoutClassId } = job.data as { workoutClassId: number; jobId: string };
    console.log(`[RecordingWorker] Stop recording for class ${workoutClassId}`);

    const hasSession = await waitForActiveSession(workoutClassId);
    if (!hasSession) {
      console.warn(
        `[RecordingWorker] No active session for class ${workoutClassId} — was the worker running for the full class?`,
      );
      await sessionManager.notifySessionMissing(workoutClassId);
      return;
    }

    await sessionManager.stopClassRecording(workoutClassId);
  },
  { connection, concurrency: 1 },
);

console.log('[RecordingWorker] Listening for recording jobs (client + corporate trainer/coach bots)');
