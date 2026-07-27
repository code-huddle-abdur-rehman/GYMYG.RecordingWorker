import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { createWriteStream } from 'fs';
import { promises as fs } from 'fs';
import os from 'os';
import path from 'path';
import { chromium, Browser, BrowserContext, Page } from 'playwright';
import { v4 as uuidv4 } from 'uuid';

import {
  collectSystemResources,
  logFileDescriptorLimits,
  PageDiagnostics,
} from './diagnostics.js';
import { withTimeout } from './asyncUtils.js';
import { finalizeWebmContainer, muxVideoWithAudio, removeFileIfExists } from './media.js';
import type { JoinAsMode } from './queues.js';

type Perspective = 'client' | 'coach' | 'trainer';
type StaffRole = 'trainer' | 'coach';

interface JoinDetails {
  roomToken: string;
  roomUrl: string;
  classId: number;
  perspective: Perspective;
  webAppUrl: string;
  startsAt?: number;
  duration?: number;
}

interface StaffPresence {
  trainerInCall: boolean;
  coachInCall: boolean;
  coachLiveViewRole: 'coach1' | 'coach2' | null;
  leadTrainerUserId: string | null;
  assignedCoachUserId: string | null;
  participantUserIds?: string[];
}

interface RecordingEntry {
  context: BrowserContext;
  page: Page;
  perspective: Perspective;
  videoRecordingStartMs: number;
  diagnostics: PageDiagnostics;
  audioFilePath: string;
  flushAudio: () => Promise<void>;
  destroyAudio: () => void;
}

interface ActiveSession {
  entries: RecordingEntry[];
  browser: Browser;
  corporateSetupPromise: Promise<void>;
}

interface AuthSession {
  accessToken: string;
  refreshToken: string;
  user: Record<string, unknown>;
}

interface LoginResponse {
  tokens: {
    access_token: string;
    refresh_token: string;
  };
  user: Record<string, unknown>;
}

const STAFF_WAIT_TIMEOUT_MS = 300_000;
const STAFF_POLL_INTERVAL_MS = 5_000;
// Reduced from 300 s to 180 s so each join attempt fails fast under host load,
// keeping the total worst-case start-job duration (2 attempts + retry delay) at
// ~365 s — safely within the 900 s start lockDuration set in index.ts.
const SESSION_JOIN_TIMEOUT_MS = 180_000;
const RECORDING_JOIN_ATTEMPTS = 2;
const RECORDING_JOIN_RETRY_DELAY_MS = 5_000;
// Stopping the in-page MediaRecorder and pulling the captured audio back out as
// base64 scales with recording length: a multi-hour class produces a far larger
// blob than a short re-join segment, and on a loaded host (several concurrent
// classes → high load average) the single page.evaluate() call can take well
// over 20s. Too small a budget here silently drops the audio and uploads a
// video-only recording, so give it room to finish.
const STOP_AUDIO_COLLECT_TIMEOUT_MS = 60_000;
const STOP_PAGE_OP_TIMEOUT_MS = 30_000;
const STOP_CONTEXT_CLOSE_TIMEOUT_MS = 60_000;

function getRecordingResolution(): { width: number; height: number } {
  const raw = process.env.RECORDING_RESOLUTION ?? '960x540';
  const [w, h] = raw.split('x').map(Number);
  if (w > 0 && h > 0) return { width: w, height: h };
  return { width: 960, height: 540 };
}

/**
 * Base directory for recording video + mux temp files.
 *
 * On the recording servers `/tmp` is a small, RAM-backed tmpfs (455 MB–953 MB)
 * that is *shared* with Chromium's shared-memory/font data (because we launch
 * with --disable-dev-shm-usage). Writing multi-hundred-MB recording videos
 * there fills the tmpfs and makes Chromium's next shared-memory write fail with
 * "Disk quota exceeded (122)", crashing the renderer. So we deliberately write
 * recordings to the real root disk (which has plenty of free space) instead of
 * tmpfs. Override with RECORDING_TMP_DIR if a different volume is preferred.
 */
export function getRecordingTmpDir(): string {
  if (process.env.RECORDING_TMP_DIR) return process.env.RECORDING_TMP_DIR;
  // /var/tmp lives on the root filesystem (not tmpfs) on the Ubuntu recording
  // hosts, so large files here never pressure RAM or Chromium's temp space.
  if (process.platform === 'linux') return '/var/tmp/gymyg-recording-worker';
  return path.join(os.tmpdir(), 'gymyg-recording-worker');
}

/**
 * Removes leftover recording temp files. Safe to call at process startup: this
 * host runs a single recording worker, so at boot there are no in-progress
 * recordings and any files present are orphans from a previous crash/restart.
 */
export async function cleanupRecordingTmpDir(): Promise<void> {
  const dir = getRecordingTmpDir();
  try {
    await fs.rm(dir, { recursive: true, force: true });
    console.log(`[RecordingWorker] Cleared stale recording temp dir ${dir}`);
  } catch (err) {
    console.warn(
      `[RecordingWorker] Could not clear recording temp dir ${dir}:`,
      err,
    );
  }
}

function getChromiumLaunchArgs(): string[] {
  return [
    '--use-fake-ui-for-media-stream',
    '--use-fake-device-for-media-stream',
    '--autoplay-policy=no-user-gesture-required',
    // Shared-memory / sandbox.  --no-zygote prevents the zygote broker process
    // that can wedge or crash in some Linux/container environments.
    //
    // NOTE: we intentionally do NOT pass --disable-dev-shm-usage. That flag
    // redirects Chromium's shared memory to /tmp, which on these hosts is a
    // small RAM-backed tmpfs already occupied by recording files — filling it
    // makes Chromium's shm/font writes fail with "Disk quota exceeded (122)"
    // and crash the renderer. /dev/shm is sized to ~half of RAM (455 MB–953 MB
    // here) and sits empty, so letting Chromium use it is both correct and
    // avoids the tmpfs contention entirely.
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--no-zygote',
    // GPU — disabled because we run headless on a server without a display.
    // NOTE: do NOT add --disable-software-rasterizer here; without a GPU,
    //       Chromium falls back to the software rasterizer, and disabling it
    //       causes an immediate renderer crash.
    '--disable-gpu',
    '--disable-accelerated-2d-canvas',
    '--disable-webgl',
    '--disable-webgl2',
    // Compositor / surface features that crash on GPU-less Linux hosts.
    '--disable-features=VizDisplayCompositor,UseSurfaceLayerForVideo,AudioServiceOutOfProcess',
    // Consistent colour pipeline — avoids ICC-profile lookup crashes.
    '--force-color-profile=srgb',
    // Don't waste resources writing minidumps that won't be collected.
    '--disable-crash-reporter',
    // Restrict WebRTC ICE candidate gathering to the default public network
    // interface only.  Without this, Chromium opens a UDP socket for *every*
    // (interface × STUN server × media track) combination, which quickly
    // exhausts the process's file-descriptor limit and causes
    // ERR_INSUFFICIENT_RESOURCES / renderer crashes on EC2 instances that use
    // the default nofile limit of 1024.  Daily.co remains reachable via STUN
    // through the primary eth0 interface.
    '--webrtc-ip-handling-policy=default_public_interface_only',
    // Silence background services that spin up extra threads / processes
    // but are entirely unused by a headless recording bot.
    '--disable-background-networking',
    '--disable-default-apps',
    '--disable-extensions',
    '--disable-sync',
    '--disable-translate',
    '--metrics-recording-only',
    '--safebrowsing-disable-auto-update',
    '--disable-domain-reliability',
    '--disable-client-side-phishing-detection',
    '--disable-prompt-on-repost',
    '--no-first-run',
    '--no-default-browser-check',
    // Prevent the OS from throttling timers / tasks in the background renderer,
    // which can confuse WebRTC state machines and trigger hangs.
    '--disable-background-timer-throttling',
    '--disable-renderer-backgrounding',
    '--disable-backgrounding-occluded-windows',
    '--disable-ipc-flooding-protection',
  ];
}

function isPageDeadError(message: string): boolean {
  const normalized = message.toLowerCase();
  return (
    normalized.includes('crash') ||
    normalized.includes('target closed') ||
    normalized.includes('target page, context or browser has been closed') ||
    normalized.includes('execution context was destroyed') ||
    normalized.includes('protocol error') ||
    normalized.includes('page closed')
  );
}

function isRecordingJoinPageReady(): boolean {
  const joinBtn = document.querySelector('#joinCallButton');
  const workoutApp = document.querySelector('.app.relative.call-background');
  const audioReady =
    document.body?.dataset?.recordingAudioCaptureReady === 'true' ||
    document.querySelector('[data-recording-audio-capture-ready="true"]') !== null;
  return !joinBtn && (!!workoutApp || audioReady);
}

export class RecordingSessionManager {
  readonly joinAs: JoinAsMode;

  private activeSessions = new Map<number, ActiveSession>();
  private stoppingClassIds = new Set<number>();
  private s3 = new S3Client({
    region: process.env.AWS_REGION || 'us-east-1',
    followRegionRedirects: true,
    credentials: process.env.AWS_ACCESS_KEY_ID
      ? {
          accessKeyId: process.env.AWS_ACCESS_KEY_ID,
          secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
        }
      : undefined,
  });

  constructor() {
    this.joinAs = this.parseJoinAs();
  }

  private parseJoinAs(): JoinAsMode {
    const value = process.env.JOIN_AS?.trim().toLowerCase();
    if (!value || !['client', 'trainer', 'coach', 'all'].includes(value)) {
      throw new Error('JOIN_AS must be set to one of: client, trainer, coach, all');
    }
    return value as JoinAsMode;
  }

  private recordsClient(): boolean {
    return this.joinAs === 'client' || this.joinAs === 'all';
  }

  private recordsTrainer(): boolean {
    return this.joinAs === 'trainer' || this.joinAs === 'all';
  }

  private recordsCoach(): boolean {
    return this.joinAs === 'coach' || this.joinAs === 'all';
  }

  private recordsCorporate(): boolean {
    return this.recordsTrainer() || this.recordsCoach();
  }

  private get apiBase() {
    return (process.env.API_BASE_URL || 'http://localhost:3002/api').replace(/\/$/, '');
  }

  private get workerKey() {
    return process.env.RECORDING_WORKER_API_KEY || '';
  }

  private get webAppUrl() {
    return (process.env.WEB_APP_URL || 'http://localhost:3000').replace(/\/$/, '');
  }

  private get workoutPath() {
    return process.env.WORKOUT_PATH || '/workout';
  }

  private getRecordingBotCredentials(): { email: string; password: string } {
    const email = process.env.EMAIL?.trim();
    const password = process.env.PASSWORD;
    if (!email || !password) {
      throw new Error(
        'EMAIL and PASSWORD must be set for the recording bot to authenticate.',
      );
    }
    return { email, password };
  }

  private getCorporateCredentials(): { email: string; password: string } {
    const email = process.env.CORPORATE_EMAIL?.trim();
    const password = process.env.CORPORATE_PASSWORD;
    if (!email || !password) {
      throw new Error(
        'CORPORATE_EMAIL and CORPORATE_PASSWORD must be set for trainer/coach recording bots.',
      );
    }
    return { email, password };
  }

  private filterUserData(user: Record<string, unknown>): Record<string, unknown> {
    const allowedKeys = [
      'userId',
      'displayName',
      'email',
      'role',
      'profilePic',
      'country',
      'birthday',
      'phone',
      'instagramUser',
      'injuryNotes',
      'isComputerConnectEnabled',
      'credits',
      'isChatBotEnabled',
      'featuredAchievement',
    ];
    return Object.fromEntries(
      allowedKeys
        .filter((key) => key in user)
        .map((key) => [key, user[key]]),
    );
  }

  private async loginWithEndpoint(
    endpoint: string,
    email: string,
    password: string,
    label: string,
  ): Promise<AuthSession> {
    const resp = await fetch(`${this.apiBase}/users/${endpoint}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        timezone: 'UTC',
      },
      body: JSON.stringify({ email, password }),
    });

    if (!resp.ok) {
      throw new Error(
        `${label} login failed: ${resp.status} ${await resp.text()}`,
      );
    }

    const data = (await resp.json()) as LoginResponse;
    if (!data.tokens?.access_token || !data.tokens?.refresh_token || !data.user) {
      throw new Error(`${label} login returned an invalid response.`);
    }

    return {
      accessToken: data.tokens.access_token,
      refreshToken: data.tokens.refresh_token,
      user: this.filterUserData(data.user),
    };
  }

  private async loginRecordingBot(): Promise<AuthSession> {
    const { email, password } = this.getRecordingBotCredentials();
    return this.loginWithEndpoint('clientLogin', email, password, 'Recording bot client');
  }

  private async loginCorporate(): Promise<AuthSession> {
    const { email, password } = this.getCorporateCredentials();
    return this.loginWithEndpoint(
      'corporateAndInstructorLogin',
      email,
      password,
      'Corporate recording bot',
    );
  }

  private async seedAuthSession(
    context: BrowserContext,
    authSession: AuthSession,
  ): Promise<void> {
    await context.addInitScript((session) => {
      window.localStorage.setItem('accessToken', session.accessToken);
      window.localStorage.setItem('refreshToken', session.refreshToken);
      window.localStorage.setItem('user', JSON.stringify(session.user));
    }, authSession);
  }

  async startClassRecording(workoutClassId: number): Promise<void> {
    if (this.activeSessions.has(workoutClassId)) {
      console.log(`[RecordingWorker] Class ${workoutClassId} already recording`);
      return;
    }

    let browser: Browser | null = null;
    const entries: ActiveSession['entries'] = [];

    try {
      browser = await chromium.launch({
        headless: true,
        // Playwright injects --disable-dev-shm-usage by default; strip it so
        // Chromium uses the (empty, adequately-sized) /dev/shm instead of the
        // congested /tmp tmpfs. See getChromiumLaunchArgs for the rationale.
        ignoreDefaultArgs: ['--mute-audio', '--disable-dev-shm-usage'],
        args: getChromiumLaunchArgs(),
      });

      console.log(
        `[RecordingWorker] Launched Chromium for class ${workoutClassId} (${this.joinAs})`,
      );
      await logFileDescriptorLimits(`browser launch, class ${workoutClassId}`);

      // Surface browser-level disconnects (whole process gone) — distinct from
      // a single renderer page crash.
      browser.on('disconnected', () => {
        console.error(
          `[RecordingWorker] Chromium browser process disconnected for class ${workoutClassId} (${this.joinAs})`,
        );
      });

      if (this.recordsClient()) {
        const clientAuthSession = await this.loginRecordingBot();
        console.log('[RecordingWorker] Recording bot authenticated as client');

        const clientJoinDetails = await this.fetchJoinDetails(workoutClassId, 'client');
        const clientEntry = await this.openClientRecordingContext(
          browser,
          clientJoinDetails,
          workoutClassId,
          clientAuthSession,
        );
        entries.push(clientEntry);
        console.log(`[RecordingWorker] Client browser session open for class ${workoutClassId}`);
      }

      if (!browser) {
        throw new Error('Browser was unexpectedly closed after opening recording context');
      }

      const session: ActiveSession = {
        browser,
        entries,
        corporateSetupPromise: Promise.resolve(),
      };

      if (this.recordsCorporate()) {
        const corporateRoles: StaffRole[] = [];
        if (this.recordsTrainer()) corporateRoles.push('trainer');
        if (this.recordsCoach()) corporateRoles.push('coach');

        session.corporateSetupPromise = Promise.all(
          corporateRoles.map((role) =>
            this.startCorporateRecordingBots(workoutClassId, session, role),
          ),
        ).then(() => undefined);
      }

      this.activeSessions.set(workoutClassId, session);
      browser = null;

      console.log(
        `[RecordingWorker] ${this.joinAs} session registered for class ${workoutClassId}`,
      );
    } catch (err) {
      await this.closeEntries(entries);
      if (browser) {
        await browser.close().catch(() => undefined);
      }
      this.activeSessions.delete(workoutClassId);
      throw err;
    }
  }

  private async startCorporateRecordingBots(
    workoutClassId: number,
    session: ActiveSession,
    joinAs: 'trainer' | 'coach',
  ): Promise<void> {
    let corporateAuthSession: AuthSession | null = null;

    const openPerspective = async (
      perspective: 'trainer' | 'coach',
      liveViewRole: string,
    ) => {
      if (!corporateAuthSession) {
        corporateAuthSession = await this.loginCorporate();
        console.log('[RecordingWorker] Corporate recording bot authenticated');
      }
      const entry = await this.openCorporateRecordingContext(
        session.browser,
        workoutClassId,
        perspective,
        liveViewRole,
        corporateAuthSession,
      );
      session.entries.push(entry);
      console.log(
        `[RecordingWorker] ${perspective} corporate browser session open for class ${workoutClassId}`,
      );
    };

    // ── Initial join attempt ──────────────────────────────────────────────
    let wasInCall = false;

    const initialPresence = await this.waitForStaffPresence(
      workoutClassId,
      joinAs,
      STAFF_WAIT_TIMEOUT_MS,
    );

    if (!initialPresence) {
      console.warn(
        `[RecordingWorker] ${joinAs} not in call within ${STAFF_WAIT_TIMEOUT_MS / 1000}s for class ${workoutClassId} — will watch for re-join`,
      );
      await this.notifyFailed(workoutClassId, joinAs);
    } else {
      wasInCall = true;
      const liveViewRole =
        joinAs === 'trainer' ? 'trainer' : initialPresence.coachLiveViewRole;
      if (!liveViewRole) {
        console.warn(
          `[RecordingWorker] No live view role for ${joinAs} class ${workoutClassId}`,
        );
        await this.notifyFailed(workoutClassId, joinAs);
      } else {
        try {
          await openPerspective(joinAs, liveViewRole);
        } catch (err) {
          console.error(
            `[RecordingWorker] Failed to start ${joinAs} corporate bot for class ${workoutClassId}:`,
            err,
          );
          await this.notifyFailed(workoutClassId, joinAs);
        }
      }
    }

    console.log(
      `[RecordingWorker] Corporate ${joinAs} initial setup done for class ${workoutClassId}: ${session.entries.length} active context(s). Watching for re-joins...`,
    );

    // ── Re-join watcher ───────────────────────────────────────────────────
    // Keeps running until stopClassRecording signals via stoppingClassIds.
    // When the staff member leaves and rejoins, a fresh recording context is
    // opened and appended to session.entries so it gets uploaded on stop.
    while (!this.stoppingClassIds.has(workoutClassId)) {
      await new Promise<void>((resolve) =>
        setTimeout(resolve, STAFF_POLL_INTERVAL_MS),
      );
      if (this.stoppingClassIds.has(workoutClassId)) break;

      try {
        const presence = await this.fetchStaffPresence(workoutClassId);
        const isInCall =
          joinAs === 'trainer' ? presence.trainerInCall : presence.coachInCall;

        if (isInCall && !wasInCall) {
          const liveViewRole =
            joinAs === 'trainer' ? 'trainer' : presence.coachLiveViewRole;
          if (!liveViewRole) {
            console.warn(
              `[RecordingWorker] ${joinAs} rejoined class ${workoutClassId} but no live view role`,
            );
          } else {
            console.log(
              `[RecordingWorker] ${joinAs} rejoined class ${workoutClassId} — opening fresh recording context`,
            );
            try {
              await openPerspective(joinAs, liveViewRole);
              console.log(
                `[RecordingWorker] ${joinAs} re-join recording started for class ${workoutClassId}`,
              );
            } catch (err) {
              console.error(
                `[RecordingWorker] Failed to open re-join context for ${joinAs} class ${workoutClassId}:`,
                err,
              );
            }
          }
        }

        wasInCall = isInCall;
      } catch (err) {
        console.warn(
          `[RecordingWorker] Re-join poll error for ${joinAs} class ${workoutClassId}:`,
          err,
        );
      }
    }

    console.log(
      `[RecordingWorker] Corporate ${joinAs} watcher done for class ${workoutClassId}`,
    );
  }

  private async closeRecordingContext(
    entry: RecordingEntry,
    workoutClassId: number,
  ): Promise<{
    videoPath: string | null;
    audioFilePath: string;
    audioStartMs: number | null;
    screenshot?: Buffer;
  }> {
    const { page, context, perspective } = entry;
    const pageClosed = page.isClosed();

    if (pageClosed) {
      console.warn(
        `[RecordingWorker] ${perspective} page already closed for class ${workoutClassId} — skipping audio/screenshot`,
      );
    }

    const video = pageClosed ? null : page.video();

    console.log(
      `[RecordingWorker] Flushing ${perspective} audio stream for class ${workoutClassId}...`,
    );
    await entry.flushAudio();

    const audioStartMs = !pageClosed
      ? await page
          .evaluate(
            () =>
              (
                window as Window & { __recordingAudioStartMs?: number }
              ).__recordingAudioStartMs ?? null,
          )
          .catch(() => null)
      : null;

    const screenshot = pageClosed
      ? undefined
      : await withTimeout(
          page.screenshot({ type: 'png' }),
          STOP_PAGE_OP_TIMEOUT_MS,
          `${perspective} screenshot for class ${workoutClassId}`,
        ).catch((err) => {
          console.warn(
            `[RecordingWorker] Screenshot failed for ${perspective} class ${workoutClassId}:`,
            err instanceof Error ? err.message : err,
          );
          return undefined;
        });

    await entry.diagnostics.dispose().catch(() => undefined);

    console.log(
      `[RecordingWorker] Closing ${perspective} browser context for class ${workoutClassId}...`,
    );
    await withTimeout(
      context.close(),
      STOP_CONTEXT_CLOSE_TIMEOUT_MS,
      `${perspective} context close for class ${workoutClassId}`,
    ).catch((err) => {
      console.warn(
        `[RecordingWorker] Context close failed for ${perspective} class ${workoutClassId}:`,
        err instanceof Error ? err.message : err,
      );
    });

    if (!video) {
      return { videoPath: null, audioFilePath: entry.audioFilePath, audioStartMs, screenshot };
    }

    const videoPath = await withTimeout(
      video.path(),
      STOP_PAGE_OP_TIMEOUT_MS,
      `${perspective} video path for class ${workoutClassId}`,
    ).catch((err) => {
      console.warn(
        `[RecordingWorker] Could not resolve ${perspective} video path for class ${workoutClassId}:`,
        err instanceof Error ? err.message : err,
      );
      return null;
    });

    return { videoPath, audioFilePath: entry.audioFilePath, audioStartMs, screenshot };
  }

  private async finalizeRecordingEntry(
    workoutClassId: number,
    entry: RecordingEntry,
    videoPath: string,
    audioFilePath: string,
    audioStartMs: number | null,
    screenshot?: Buffer,
  ): Promise<void> {
    const tempDir = path.dirname(videoPath);
    console.log(
      `[RecordingWorker] Preparing ${entry.perspective} upload for class ${workoutClassId}...`,
    );
    const uploadPath = await this.prepareUploadVideo(
      videoPath,
      audioFilePath,
      entry.videoRecordingStartMs,
      audioStartMs,
    );
    try {
      console.log(
        `[RecordingWorker] Uploading ${entry.perspective} recording for class ${workoutClassId}...`,
      );
      await this.uploadRecording(
        workoutClassId,
        entry.perspective,
        uploadPath,
        screenshot,
      );
      console.log(
        `[RecordingWorker] Uploaded ${entry.perspective} recording for class ${workoutClassId}`,
      );
    } finally {
      await removeFileIfExists(videoPath);
      if (uploadPath !== videoPath) {
        await removeFileIfExists(uploadPath);
      }
      await fs.rm(tempDir, { recursive: true, force: true }).catch(() => undefined);
    }
  }

  async stopClassRecording(workoutClassId: number): Promise<void> {
    const session = this.activeSessions.get(workoutClassId);
    if (!session) {
      console.log(`[RecordingWorker] No active session for class ${workoutClassId}`);
      return;
    }

    this.stoppingClassIds.add(workoutClassId);

    if (this.recordsCorporate()) {
      console.log(
        `[RecordingWorker] Waiting for corporate bot setup to finish for class ${workoutClassId}...`,
      );
      await session.corporateSetupPromise.catch((err) => {
        console.warn(
          `[RecordingWorker] Corporate setup ended with error for class ${workoutClassId}:`,
          err,
        );
      });
    }

    this.activeSessions.delete(workoutClassId);
    this.stoppingClassIds.delete(workoutClassId);

    try {
      const entries = [...session.entries];
      console.log(
        `[RecordingWorker] Finalizing ${entries.length} recording context(s) for class ${workoutClassId}`,
      );

      // A class can hold several contexts for the same perspective (rejoins).
      // The server keeps a single recording row per perspective, so we must
      // NOT let one segment's failure overwrite another segment's successful
      // upload. Track which perspectives produced at least one good upload and
      // only report "failed" for perspectives where every segment failed.
      const succeededPerspectives = new Set<Perspective>();
      const attemptedPerspectives = new Set<Perspective>();

      for (const entry of entries) {
        attemptedPerspectives.add(entry.perspective);
        try {
          const { videoPath, audioFilePath, audioStartMs, screenshot } =
            await this.closeRecordingContext(entry, workoutClassId);
          if (!videoPath) {
            continue;
          }

          await this.finalizeRecordingEntry(
            workoutClassId,
            entry,
            videoPath,
            audioFilePath,
            audioStartMs,
            screenshot,
          );
          succeededPerspectives.add(entry.perspective);
        } catch (err) {
          console.error(
            `[RecordingWorker] Error finalizing ${entry.perspective} context for class ${workoutClassId}:`,
            err,
          );
          await entry.diagnostics.dispose().catch(() => undefined);
          await entry.context.close().catch(() => undefined);
        }
      }

      for (const perspective of attemptedPerspectives) {
        if (!succeededPerspectives.has(perspective)) {
          console.warn(
            `[RecordingWorker] No successful ${perspective} upload for class ${workoutClassId} — marking failed`,
          );
          await this.notifyFailed(workoutClassId, perspective);
        }
      }
    } finally {
      await withTimeout(
        session.browser.close(),
        STOP_CONTEXT_CLOSE_TIMEOUT_MS,
        `browser close for class ${workoutClassId}`,
      ).catch((err) => {
        console.error(
          `[RecordingWorker] Failed to close browser for class ${workoutClassId}:`,
          err,
        );
      });
      console.log(`[RecordingWorker] Browser closed for class ${workoutClassId}`);
      await fs
        .rm(path.join(getRecordingTmpDir(), String(workoutClassId)), {
          recursive: true,
          force: true,
        })
        .catch(() => undefined);
      console.log(
        `[RecordingWorker] Finished stop processing for class ${workoutClassId}`,
      );
    }
  }

  async closeAllSessions(): Promise<void> {
    const classIds = Array.from(this.activeSessions.keys());
    if (classIds.length === 0) {
      return;
    }
    console.log(
      `[RecordingWorker] Closing ${classIds.length} active browser session(s)...`,
    );
    await Promise.all(classIds.map((id) => this.forceCloseSession(id)));
  }

  async forceCloseSession(workoutClassId: number): Promise<void> {
    const session = this.activeSessions.get(workoutClassId);
    if (!session) {
      return;
    }
    this.stoppingClassIds.add(workoutClassId);
    this.activeSessions.delete(workoutClassId);
    await session.corporateSetupPromise.catch(() => undefined);
    this.stoppingClassIds.delete(workoutClassId);
    await this.closeEntries(session.entries);
    await session.browser.close().catch(() => undefined);
    console.log(`[RecordingWorker] Force-closed browser for class ${workoutClassId}`);
  }

  private async closeEntries(entries: ActiveSession['entries']): Promise<void> {
    await Promise.all(
      entries.map(async (entry) => {
        await entry.diagnostics.dispose().catch(() => undefined);
        await entry.context.close().catch(() => undefined);
      }),
    );
  }

  private async fetchJoinDetails(
    workoutClassId: number,
    perspective: Perspective,
  ): Promise<JoinDetails> {
    const url = `${this.apiBase}/class-recording/worker/join-details/${workoutClassId}/${perspective}`;
    const resp = await fetch(url, {
      headers: { 'x-recording-worker-key': this.workerKey },
    });
    if (!resp.ok) {
      throw new Error(`Failed to fetch join details: ${resp.status} ${await resp.text()}`);
    }
    return resp.json() as Promise<JoinDetails>;
  }

  private async fetchStaffPresence(workoutClassId: number): Promise<StaffPresence> {
    const url = `${this.apiBase}/class-recording/worker/staff-presence/${workoutClassId}`;
    const resp = await fetch(url, {
      headers: { 'x-recording-worker-key': this.workerKey },
    });
    if (!resp.ok) {
      throw new Error(
        `Failed to fetch staff presence: ${resp.status} ${await resp.text()}`,
      );
    }
    return resp.json() as Promise<StaffPresence>;
  }

  private async waitForStaffPresence(
    workoutClassId: number,
    staff: StaffRole,
    timeoutMs: number,
  ): Promise<StaffPresence | null> {
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
      if (this.stoppingClassIds.has(workoutClassId)) {
        console.log(
          `[RecordingWorker] Stop requested — aborting ${staff} presence wait for class ${workoutClassId}`,
        );
        return null;
      }

      try {
        const presence = await this.fetchStaffPresence(workoutClassId);
        const isPresent =
          staff === 'trainer' ? presence.trainerInCall : presence.coachInCall;
        if (isPresent) {
          console.log(
            `[RecordingWorker] ${staff} is in call for class ${workoutClassId}`,
            {
              leadTrainerUserId: presence.leadTrainerUserId,
              assignedCoachUserId: presence.assignedCoachUserId,
              participantCount: presence.participantUserIds?.length ?? 0,
            },
          );
          return presence;
        }

        console.log(
          `[RecordingWorker] Waiting for ${staff} to join class ${workoutClassId}...`,
          {
            trainerInCall: presence.trainerInCall,
            coachInCall: presence.coachInCall,
            leadTrainerUserId: presence.leadTrainerUserId,
            assignedCoachUserId: presence.assignedCoachUserId,
            participantsInRoom: presence.participantUserIds,
          },
        );
      } catch (err) {
        console.warn('[RecordingWorker] Staff presence poll failed:', err);
      }

      const remaining = deadline - Date.now();
      if (remaining <= 0) break;

      await new Promise((resolve) =>
        setTimeout(resolve, Math.min(STAFF_POLL_INTERVAL_MS, remaining)),
      );
    }

    return null;
  }

  private buildClientRecordingUrl(joinDetails: JoinDetails): string {
    const params = new URLSearchParams({
      classId: String(joinDetails.classId),
      recordingMode: 'true',
      recordingPerspective: joinDetails.perspective,
      roomToken: joinDetails.roomToken,
      roomUrl: joinDetails.roomUrl,
    });
    if (joinDetails.startsAt) {
      params.set('startsAt', String(joinDetails.startsAt));
    }
    if (joinDetails.duration) {
      params.set('duration', String(joinDetails.duration));
    }
    return `${this.webAppUrl}${this.workoutPath}?${params.toString()}`;
  }

  private buildCorporateRecordingUrl(
    classId: number,
    perspective: 'trainer' | 'coach',
    liveViewRole: string,
  ): string {
    const params = new URLSearchParams({
      classId: String(classId),
      recordingMode: 'true',
      recordingPerspective: perspective,
      role: liveViewRole,
    });
    return `${this.webAppUrl}${this.workoutPath}?${params.toString()}`;
  }

  private getScreencastAudioDelaySeconds(): number {
    const ms = Number(process.env.RECORDING_AUDIO_DELAY_MS ?? 300);
    if (!Number.isFinite(ms) || ms <= 0) {
      return 0;
    }
    return ms / 1000;
  }

  private attachPageLogging(
    page: Page,
    perspective: Perspective,
    workoutClassId: number,
  ): void {
    page.on('console', (msg) => {
      const type = msg.type();
      if (type !== 'error' && type !== 'warning') return;

      const text = msg.text();

      // These messages are expected on a headless server and carry no
      // actionable information for the recording worker.
      if (
        text.includes('setSinkId') ||            // no real audio-output sinks on server
        text.includes('GeolocationPositionError') || // no GPS on server
        text.includes('Geolocation disabled in recording bot') || // our own mock
        text.includes('LogRocket') ||            // 3rd-party session replay noise
        text.includes('setBandwidth()') ||       // Daily.co SDK deprecation warning
        text.includes('Failed to play audio') || // audio elements are intentionally suppressed
        text.includes('Audio element error') ||  // same — suppressed audio element
        text.includes('Invalid featureActive value') // Daily.co internal SDK noise
      ) {
        return;
      }

      console.log(
        `[RecordingWorker][${perspective}][console:${type}] class ${workoutClassId}: ${text}`,
      );
    });
    page.on('pageerror', (error) => {
      console.error(
        `[RecordingWorker][${perspective}][pageerror] class ${workoutClassId}:`,
        error.message,
      );
    });
    page.on('crash', () => {
      console.error(
        `[RecordingWorker] *** Page CRASHED *** for ${perspective} perspective, class ${workoutClassId}`,
      );
    });
  }

  private isRecoverableRecordingError(err: unknown): boolean {
    const msg = err instanceof Error ? err.message : String(err);
    return isPageDeadError(msg) || msg.includes('failed to join');
  }

  private async createRecordingContext(
    browser: Browser,
    workoutClassId: number,
    perspective: Perspective,
  ): Promise<{
    context: BrowserContext;
    page: Page;
    videoRecordingStartMs: number;
    diagnostics: PageDiagnostics;
    videoDir: string;
    audioFilePath: string;
    flushAudio: () => Promise<void>;
    destroyAudio: () => void;
  }> {
    const videoRecordingStartMs = Date.now();
    // Each context gets its OWN unique sub-directory (…/<classId>/<perspective>/<uuid>).
    // A class can produce several contexts for the same perspective (e.g. the
    // trainer leaves and re-joins → a fresh recording context is opened). If
    // those contexts shared one directory, finalizing the first one would
    // `fs.rm` the shared folder and delete the *other* context's video/audio
    // files mid-flight — causing ENOENT and a spurious "failed" upload. Keeping
    // each context isolated means one segment's cleanup can never touch another.
    const videoDir = path.join(
      getRecordingTmpDir(),
      String(workoutClassId),
      perspective,
      uuidv4(),
    );
    await fs.mkdir(videoDir, { recursive: true });

    const resolution = getRecordingResolution();

    const context = await browser.newContext({
      recordVideo: {
        dir: videoDir,
        size: resolution,
      },
      viewport: resolution,
      ignoreHTTPSErrors: true,
      permissions: [],
    });

    // Patch browser APIs that don't work in a headless environment and whose
    // failures trigger noisy retry loops or excessive resource usage.
    await context.addInitScript(() => {
      if (typeof HTMLMediaElement !== 'undefined') {
        // setSinkId selects a named audio-output device.  Headless Chrome has
        // no real output sinks, so every call throws AbortError and the app
        // retries every 2 s indefinitely.  Override with a silent no-op.
        HTMLMediaElement.prototype.setSinkId = function () {
          return Promise.resolve();
        };

        // Suppress audio-element playback.  Each active HTMLAudioElement holds
        // an audio-decoder pipeline and at least one UDP/TCP socket.  On a
        // live call with multiple participants the app creates one audio
        // element per participant; activating them all rapidly exhausts the
        // process's file-descriptor budget and causes ERR_INSUFFICIENT_RESOURCES.
        // The recording bot captures audio through __stopRecordingBotAudio()
        // (a WebAudio MediaRecorder path), so HTMLAudioElement playback is
        // not needed and is safe to suppress.
        const _origPlay = HTMLMediaElement.prototype.play;
        HTMLMediaElement.prototype.play = function () {
          if (this instanceof HTMLAudioElement) {
            return Promise.resolve();
          }
          // Video elements must still play for the screen-capture recording to
          // render participant video feeds through the compositor.
          return _origPlay.call(this);
        };
      }

      // Silence geolocation so the app doesn't log position errors.
      if (typeof navigator !== 'undefined' && navigator.geolocation) {
        navigator.geolocation.getCurrentPosition = (
          _success: PositionCallback,
          error?: PositionErrorCallback | null,
        ) => {
          if (error) {
            error({
              code: 1,
              message: 'Geolocation disabled in recording bot',
              PERMISSION_DENIED: 1,
              POSITION_UNAVAILABLE: 2,
              TIMEOUT: 3,
            } as GeolocationPositionError);
          }
        };
        navigator.geolocation.watchPosition = () => 0;
      }
    });

    (context as { __perspective?: Perspective }).__perspective = perspective;

    const page = await context.newPage();
    page.on('dialog', async (dialog) => {
      console.log(`[RecordingWorker] Dismissing browser dialog: ${dialog.message()}`);
      await dialog.dismiss().catch(() => undefined);
    });
    this.attachPageLogging(page, perspective, workoutClassId);

    // Rolling diagnostics buffer + crash reporter for this page. Any renderer
    // crash now emits a full report (fd usage, failed requests, renderer
    // metrics, recent console output) instead of a bare "Page CRASHED" line.
    const diagnostics = new PageDiagnostics(page, perspective, workoutClassId);
    diagnostics.attach();
    page.on('crash', () => {
      void diagnostics.logCrashReport('page "crash" event fired');
    });

    // ── Chunked audio streaming ───────────────────────────────────────────────
    // Each 1-second MediaRecorder timeslice (~16 KB) is written to disk
    // immediately via exposeFunction, avoiding the end-of-class blob transfer
    // that could exceed STOP_AUDIO_COLLECT_TIMEOUT_MS on loaded hosts.
    const audioFilePath = path.join(videoDir, `audio-${uuidv4()}.webm`);
    const audioStream = createWriteStream(audioFilePath, { flags: 'a' });
    audioStream.on('error', (err) =>
      console.error(
        `[RecordingWorker] Audio stream error for class ${workoutClassId} (${perspective}):`,
        err,
      ),
    );
    let totalAudioBytes = 0;
    let audioStreamClosed = false;
    const audioStreamDone = new Promise<void>((resolve) => {
      audioStream.on('finish', resolve);
      audioStream.on('error', () => resolve());
    });

    await context.exposeFunction('__saveAudioChunk', async (b64: string) => {
      if (!b64 || audioStreamClosed) return;
      const chunk = Buffer.from(b64, 'base64');
      totalAudioBytes += chunk.byteLength;
      audioStream.write(chunk);
    });

    await context.exposeFunction('__finishAudioRecording', async () => {
      if (!audioStreamClosed) {
        audioStreamClosed = true;
        audioStream.end();
      }
      await audioStreamDone;
      console.log(
        `[RecordingWorker] Audio stream closed for class ${workoutClassId} (${perspective}) — ` +
          `${(totalAudioBytes / 1024).toFixed(1)} KB on disk`,
      );
    });

    const flushAudio = async () => {
      if (!page.isClosed()) {
        await withTimeout(
          page.evaluate(async () => {
            await (
              window as Window & {
                __stopRecordingBotAudio?: () => Promise<unknown>;
              }
            ).__stopRecordingBotAudio?.();
          }),
          STOP_AUDIO_COLLECT_TIMEOUT_MS,
          `${perspective} audio flush for class ${workoutClassId}`,
        ).catch((err) =>
          console.warn(
            `[RecordingWorker] Audio flush failed for ${perspective} class ${workoutClassId}:`,
            err,
          ),
        );
      } else if (!audioStreamClosed) {
        audioStreamClosed = true;
        audioStream.end();
      }
      await audioStreamDone;
    };

    const destroyAudio = () => {
      audioStreamClosed = true;
      audioStream.destroy();
    };

    return { context, page, videoRecordingStartMs, diagnostics, videoDir, audioFilePath, flushAudio, destroyAudio };
  }

  private async openClientRecordingContext(
    browser: Browser,
    joinDetails: JoinDetails,
    workoutClassId: number,
    authSession: AuthSession,
  ): Promise<RecordingEntry> {
    let lastError: unknown;

    for (let attempt = 1; attempt <= RECORDING_JOIN_ATTEMPTS; attempt++) {
      const { context, page, videoRecordingStartMs, diagnostics, videoDir, audioFilePath, flushAudio, destroyAudio } =
        await this.createRecordingContext(browser, workoutClassId, 'client');
      // Sample resource usage throughout the join — this is when crashes occur.
      diagnostics.startSampling();

      try {
        await this.seedAuthSession(context, authSession);

        const recordingUrl = this.buildClientRecordingUrl(joinDetails);
        console.log(`[RecordingWorker] Navigating client bot to ${recordingUrl}`);

        await page.goto(recordingUrl, { waitUntil: 'domcontentloaded', timeout: 120000 });

        if (page.url().includes('/authentication/clientLogin')) {
          throw new Error(
            'Recording bot was redirected to client login. Check EMAIL/PASSWORD credentials.',
          );
        }

        await this.waitForSessionJoined(page, 'client', false, diagnostics);
        diagnostics.stopSampling();
        return { context, page, perspective: 'client', videoRecordingStartMs, diagnostics, audioFilePath, flushAudio, destroyAudio };
      } catch (err) {
        await diagnostics.logCrashReport(
          `client join attempt ${attempt} failed: ${err instanceof Error ? err.message : String(err)}`,
        );
        await diagnostics.dispose();
        destroyAudio();
        await context.close().catch(() => undefined);
        // Discard the partial recording so it doesn't accumulate on disk.
        await fs.rm(videoDir, { recursive: true, force: true }).catch(() => undefined);
        lastError = err;
        if (attempt < RECORDING_JOIN_ATTEMPTS && this.isRecoverableRecordingError(err)) {
          console.warn(
            `[RecordingWorker] Client join attempt ${attempt}/${RECORDING_JOIN_ATTEMPTS} failed for class ${workoutClassId}, retrying in ${RECORDING_JOIN_RETRY_DELAY_MS / 1000}s:`,
            err instanceof Error ? err.message : err,
          );
          await new Promise((resolve) => setTimeout(resolve, RECORDING_JOIN_RETRY_DELAY_MS));
          continue;
        }
        throw err;
      }
    }

    throw lastError;
  }

  private async openCorporateRecordingContext(
    browser: Browser,
    workoutClassId: number,
    perspective: 'trainer' | 'coach',
    liveViewRole: string,
    authSession: AuthSession,
  ): Promise<RecordingEntry> {
    let lastError: unknown;

    for (let attempt = 1; attempt <= RECORDING_JOIN_ATTEMPTS; attempt++) {
      const { context, page, videoRecordingStartMs, diagnostics, videoDir, audioFilePath, flushAudio, destroyAudio } =
        await this.createRecordingContext(browser, workoutClassId, perspective);
      diagnostics.startSampling();

      try {
        await this.seedAuthSession(context, authSession);

        const recordingUrl = this.buildCorporateRecordingUrl(
          workoutClassId,
          perspective,
          liveViewRole,
        );
        console.log(
          `[RecordingWorker] Navigating ${perspective} corporate bot to ${recordingUrl}`,
        );

        await page.goto(recordingUrl, { waitUntil: 'domcontentloaded', timeout: 120000 });

        if (
          page.url().includes('/authentication/instructorLogin') ||
          page.url().includes('/authentication/clientLogin')
        ) {
          throw new Error(
            'Corporate recording bot was redirected to login. Check CORPORATE_EMAIL/CORPORATE_PASSWORD credentials.',
          );
        }

        await this.waitForSessionJoined(page, perspective, true, diagnostics);
        diagnostics.stopSampling();
        return { context, page, perspective, videoRecordingStartMs, diagnostics, audioFilePath, flushAudio, destroyAudio };
      } catch (err) {
        await diagnostics.logCrashReport(
          `${perspective} corporate join attempt ${attempt} failed: ${err instanceof Error ? err.message : String(err)}`,
        );
        await diagnostics.dispose();
        destroyAudio();
        await context.close().catch(() => undefined);
        // Discard the partial recording so it doesn't accumulate on disk.
        await fs.rm(videoDir, { recursive: true, force: true }).catch(() => undefined);
        lastError = err;
        if (attempt < RECORDING_JOIN_ATTEMPTS && this.isRecoverableRecordingError(err)) {
          console.warn(
            `[RecordingWorker] ${perspective} corporate join attempt ${attempt}/${RECORDING_JOIN_ATTEMPTS} failed for class ${workoutClassId}, retrying in ${RECORDING_JOIN_RETRY_DELAY_MS / 1000}s:`,
            err instanceof Error ? err.message : err,
          );
          await new Promise((resolve) => setTimeout(resolve, RECORDING_JOIN_RETRY_DELAY_MS));
          continue;
        }
        throw err;
      }
    }

    throw lastError;
  }

  private async waitForSessionJoined(
    page: Page,
    perspective: Perspective,
    waitForLiveViewReady: boolean,
    pageDiagnostics?: PageDiagnostics,
  ): Promise<void> {
    // Build a promise that rejects the instant the renderer crashes.
    // This lets us short-circuit timed locator waits (which Playwright does NOT
    // abort on crash — they silently wait out the full timeout) so we fail fast
    // and free up the retry budget immediately.
    let crashHandler: (() => void) | undefined;
    const crashRejector = new Promise<never>((_, reject) => {
      crashHandler = () =>
        reject(
          new Error(
            `Recording bot page crashed while joining the ${perspective} session — check server resources`,
          ),
        );
      page.once('crash', crashHandler);
    });
    // Suppress Node's unhandled-rejection warning for the cases where the caller
    // catches the error before (or without) the crash ever firing.
    crashRejector.catch(() => undefined);

    const removeCrashHandler = () => {
      if (crashHandler) {
        page.off('crash', crashHandler);
        crashHandler = undefined;
      }
    };

    try {
      const joinButton = page.locator('#joinCallButton');
      try {
        // Race the button wait against the crash rejector so we don't spend the
        // full 15 s timeout polling a renderer that has already died.
        await Promise.race([
          joinButton.waitFor({ state: 'visible', timeout: 15000 }),
          crashRejector,
        ]);
        await joinButton.click();
        console.log(`[RecordingWorker] Clicked Join Call for ${perspective} bot`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (isPageDeadError(msg)) throw err;
        console.log(
          `[RecordingWorker] Join Call button not shown for ${perspective} — assuming auto-join`,
        );
      }

      await page
        .waitForFunction(
          () => {
            const joinBtn = document.querySelector('#joinCallButton');
            const workoutApp = document.querySelector('.app.relative.call-background');
            return !joinBtn && !!workoutApp;
          },
          { timeout: SESSION_JOIN_TIMEOUT_MS },
        )
        .catch(async (err: unknown) => {
          const msg = err instanceof Error ? err.message : String(err);
          // Playwright throws a specific message when the renderer crashes.
          if (isPageDeadError(msg)) {
            throw new Error(
              `Recording bot page crashed while joining the ${perspective} session — check server resources`,
            );
          }

          // Genuine timeout: capture diagnostic state to help future debugging.
          const diagnostics = await page
            .evaluate(() => ({
              url: window.location.href,
              callState: (window as unknown as Record<string, unknown>).__callState ?? null,
              hasJoinBtn: !!document.querySelector('#joinCallButton'),
              hasCallBg: !!document.querySelector('.app.relative.call-background'),
              hasAudioReady: !!document.querySelector('[data-recording-audio-capture-ready="true"]'),
              bodyDataset: Object.fromEntries(
                Object.entries((document.body as HTMLElement & { dataset: DOMStringMap }).dataset),
              ),
            }))
            .catch(() => null);
          console.error(
            `[RecordingWorker] ${perspective} join timed out after ${SESSION_JOIN_TIMEOUT_MS / 1000}s — page diagnostics:`,
            diagnostics,
          );
          await pageDiagnostics?.logCrashReport(
            `${perspective} join timed out after ${SESSION_JOIN_TIMEOUT_MS / 1000}s`,
          );
          throw new Error(
            `Recording bot failed to join the ${perspective} session within ${SESSION_JOIN_TIMEOUT_MS / 1000}s`,
          );
        });
    } finally {
      // Remove the crash listener so the pending crashRejector is never settled
      // after this point — avoids surprise rejections in the caller.
      removeCrashHandler();
    }

    console.log(`[RecordingWorker] ${perspective} bot joined the live session`);

    if (waitForLiveViewReady) {
      try {
        await page.waitForSelector('[data-recording-live-view-ready="true"]', {
          timeout: 60000,
        });
        console.log(`[RecordingWorker] ${perspective} corporate live view is ready`);
      } catch {
        console.warn(
          `[RecordingWorker] ${perspective} live view readiness marker not detected — continuing recording anyway`,
        );
      }
    }

    try {
      await page.waitForSelector('[data-recording-audio-capture-ready="true"]', {
        timeout: 60000,
      });
      console.log(`[RecordingWorker] ${perspective} page audio capture is ready`);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      if (isPageDeadError(msg)) {
        throw new Error(
          `Recording bot page crashed after joining the ${perspective} session — check server resources`,
        );
      }
      console.warn(
        `[RecordingWorker] ${perspective} audio capture readiness marker not detected — continuing recording anyway`,
      );
    }

    await page.waitForTimeout(3000).catch((err: unknown) => {
      const msg = err instanceof Error ? err.message : String(err);
      if (isPageDeadError(msg)) {
        throw new Error(
          `Recording bot page crashed after joining the ${perspective} session — check server resources`,
        );
      }
    });
  }

  private async collectAudioFromPage(
    page: Page,
    perspective: Perspective,
  ): Promise<{ buffer: Buffer | null; audioStartMs: number | null }> {
    if (page.isClosed()) {
      console.warn(
        `[RecordingWorker] Skipping ${perspective} audio collection — page is closed`,
      );
      return { buffer: null, audioStartMs: null };
    }

    try {
      await withTimeout(
        page.evaluate(async () => {
          const resume = (window as Window & {
            __resumeRecordingBotAudio?: () => Promise<void>;
          }).__resumeRecordingBotAudio;
          if (typeof resume === 'function') {
            await resume();
          }
        }),
        STOP_AUDIO_COLLECT_TIMEOUT_MS,
        `${perspective} audio resume`,
      );

      const audioResult = await withTimeout(
        page.evaluate(async () => {
          const win = window as Window & {
            __stopRecordingBotAudio?: () => Promise<string | null>;
            __recordingAudioStartMs?: number;
          };
          const audioStartMs =
            typeof win.__recordingAudioStartMs === 'number'
              ? win.__recordingAudioStartMs
              : null;
          if (typeof win.__stopRecordingBotAudio !== 'function') {
            return { reason: 'hook-missing' as const, data: null, audioStartMs };
          }
          const data = await win.__stopRecordingBotAudio();
          if (!data) {
            return { reason: 'empty-blob' as const, data: null, audioStartMs };
          }
          return { reason: 'ok' as const, data, audioStartMs };
        }),
        STOP_AUDIO_COLLECT_TIMEOUT_MS,
        `${perspective} audio stop`,
      );

      if (audioResult.reason === 'hook-missing') {
        console.warn(
          `[RecordingWorker] No page audio hook for ${perspective} bot (never reached STATE_JOINED or capture not started)`,
        );
        return { buffer: null, audioStartMs: audioResult.audioStartMs };
      }

      if (audioResult.reason === 'empty-blob') {
        console.warn(
          `[RecordingWorker] Page audio blob empty for ${perspective} bot (no audio sources captured)`,
        );
        return { buffer: null, audioStartMs: audioResult.audioStartMs };
      }

      const audioBuffer = Buffer.from(audioResult.data, 'base64');
      console.log(
        `[RecordingWorker] Collected ${perspective} page audio (${audioBuffer.length} bytes)`,
      );
      return { buffer: audioBuffer, audioStartMs: audioResult.audioStartMs };
    } catch (err) {
      console.warn(`[RecordingWorker] Failed to collect ${perspective} page audio:`, err);
      return { buffer: null, audioStartMs: null };
    }
  }

  private async prepareUploadVideo(
    videoPath: string,
    audioFilePath: string,
    videoRecordingStartMs: number,
    audioStartMs: number | null,
  ): Promise<string> {
    const tempDir = path.dirname(videoPath);

    const audioStat = await fs.stat(audioFilePath).catch(() => null);
    if (!audioStat || audioStat.size === 0) {
      // No audio captured — still run a container-finalization pass so ffmpeg
      // writes the Duration and Cues (seek-index) elements.  Without this the
      // file has Duration: N/A and browsers treat it as a live stream (no
      // end-time, cannot seek to the end).  This is especially common when
      // context.close() timed out and Chromium never finalized the WebM file.
      const finalizedPath = path.join(tempDir, `finalized-${uuidv4()}.webm`);
      try {
        await finalizeWebmContainer(videoPath, finalizedPath);
        console.log('[RecordingWorker] Finalized video-only container (duration metadata written)');
        return finalizedPath;
      } catch (err) {
        console.warn(
          '[RecordingWorker] Container finalization failed, uploading raw video:',
          err instanceof Error ? err.message : err,
        );
        return videoPath;
      }
    }

    const audioPath = audioFilePath; // already streamed to disk chunk-by-chunk
    const outputPath = path.join(tempDir, `muxed-${uuidv4()}.webm`);

    const videoTrimSeconds =
      audioStartMs != null
        ? Math.max(0, (audioStartMs - videoRecordingStartMs) / 1000)
        : 0;
    const audioDelaySeconds = this.getScreencastAudioDelaySeconds();

    if (videoTrimSeconds > 0 || audioDelaySeconds > 0) {
      console.log(
        `[RecordingWorker] A/V sync: trim video ${videoTrimSeconds.toFixed(2)}s, delay audio ${audioDelaySeconds.toFixed(2)}s`,
      );
    } else if (audioStartMs == null) {
      console.warn(
        '[RecordingWorker] Audio start timestamp missing — muxing without video trim',
      );
    }

    try {
      await muxVideoWithAudio(videoPath, audioPath, outputPath, {
        videoTrimSeconds,
        audioDelaySeconds,
      });
      console.log('[RecordingWorker] Muxed page audio into recording video');
      return outputPath;
    } catch (err) {
      console.warn(
        '[RecordingWorker] Failed to mux audio into video, falling back to finalized video-only:',
        err instanceof Error ? err.message : err,
      );
      // Mux failed — still try to finalize the container so the video is seekable.
      const finalizedPath = path.join(tempDir, `finalized-${uuidv4()}.webm`);
      try {
        await finalizeWebmContainer(videoPath, finalizedPath);
        console.log('[RecordingWorker] Finalized container after mux failure');
        return finalizedPath;
      } catch {
        return videoPath;
      }
    } finally {
      await removeFileIfExists(audioPath);
    }
  }

  private async notifyFailed(
    workoutClassId: number,
    perspective: Perspective,
  ): Promise<void> {
    const url = `${this.apiBase}/class-recording/worker/failed/${workoutClassId}/${perspective}`;
    await fetch(url, {
      method: 'POST',
      headers: { 'x-recording-worker-key': this.workerKey },
    }).catch((err) => console.error('[RecordingWorker] Failed to notify failure:', err));
  }

  private async uploadRecording(
    workoutClassId: number,
    perspective: Perspective,
    videoPath: string,
    screenshot?: Buffer,
  ): Promise<void> {
    const bucket = process.env.BUCKET_NAME;
    if (!bucket) {
      console.warn('[RecordingWorker] BUCKET_NAME not set, skipping S3 upload');
      await this.notifyFailed(workoutClassId, perspective);
      return;
    }

    const prefix = process.env.CLASS_RECORDING_PREFIX || 'classRecordings';
    const fileId = uuidv4();
    const s3Key = `${prefix}/${workoutClassId}/${perspective}/${fileId}.webm`;
    const thumbnailKey = `${prefix}/${workoutClassId}/${perspective}/${fileId}-thumb.png`;

    // Stream the file via multipart upload instead of loading it all into memory.
    // PutObjectCommand with a large Buffer causes EPIPE / connection-reset errors
    // on long recordings (hundreds of MB) because a single HTTP PUT times out
    // mid-stream.  The Upload class splits the file into ~10 MB parts, retries
    // each part independently, and does not hold the whole file in RAM.
    const fileStream = (await import('fs')).createReadStream(videoPath);
    const upload = new Upload({
      client: this.s3,
      queueSize: 4,
      partSize: 10 * 1024 * 1024,
      params: {
        Bucket: bucket,
        Key: s3Key,
        Body: fileStream,
        ContentType: 'video/webm',
        ACL: 'public-read',
      },
    });
    await upload.done();

    let uploadedThumbnailKey: string | undefined;
    if (screenshot) {
      try {
        await this.s3.send(
          new PutObjectCommand({
            Bucket: bucket,
            Key: thumbnailKey,
            Body: screenshot,
            ContentType: 'image/png',
            ACL: 'public-read',
          }),
        );
        uploadedThumbnailKey = thumbnailKey;
      } catch (err) {
        console.warn(
          `[RecordingWorker] Failed to upload thumbnail for class ${workoutClassId}:`,
          err,
        );
      }
    }

    await this.notifyComplete(workoutClassId, {
      perspective,
      s3Key,
      thumbnailS3Key: uploadedThumbnailKey,
    });

    await fs.unlink(videoPath).catch(() => undefined);
  }

  private async notifyComplete(
    workoutClassId: number,
    payload: {
      perspective: Perspective;
      s3Key: string;
      thumbnailS3Key?: string;
    },
  ): Promise<void> {
    const url = `${this.apiBase}/class-recording/worker/complete/${workoutClassId}`;
    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-recording-worker-key': this.workerKey,
      },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      throw new Error(`Failed to notify complete: ${resp.status}`);
    }
  }

  hasSession(workoutClassId: number): boolean {
    return this.activeSessions.has(workoutClassId);
  }

  async notifySessionMissing(
    workoutClassId: number,
    perspective: Perspective,
  ): Promise<void> {
    const url = `${this.apiBase}/class-recording/worker/session-missing/${workoutClassId}/${perspective}`;
    await fetch(url, {
      method: 'POST',
      headers: { 'x-recording-worker-key': this.workerKey },
    }).catch((err) =>
      console.error('[RecordingWorker] Failed to notify missing session:', err),
    );
  }
}
