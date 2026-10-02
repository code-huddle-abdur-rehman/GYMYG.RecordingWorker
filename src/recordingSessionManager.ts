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
export type StopReason = 'job' | 'idle' | 'watchdog';

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
  // Only returned when requested; absent on servers that predate idle exit.
  realParticipantCount?: number;
}

interface RecordingEntry {
  context: BrowserContext;
  page: Page;
  perspective: Perspective;
  videoRecordingStartMs: number;
  diagnostics: PageDiagnostics;
  audioFilePath: string;
  presenceReporterId: string;
  flushAudio: () => Promise<void>;
  destroyAudio: () => void;
}

interface ActiveSession {
  entries: RecordingEntry[];
  browser: Browser;
  corporateSetupPromise: Promise<void>;
  // Segments closed mid-class (a trainer/coach left), uploading in the
  // background. Stop waits for them before its own uploads and cleanup.
  segmentFinalizations: Promise<void>[];
  // Per perspective: whether any mid-class segment uploaded successfully.
  segmentResults: Map<Perspective, boolean>;
}

interface PresenceReport {
  count: number;
  at: number;
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
// Budget for the audio write stream to flush its buffered chunks to disk once
// end() has been called. This MUST be bounded: an unbounded wait here blocks
// closeRecordingContext, which blocks stopClassRecording before it reaches the
// finally that closes Chromium. Since the stop worker runs at concurrency 1,
// one such hang permanently occupies the only stop slot, so every subsequent
// class leaks a live Chromium (still in the WebRTC call, still encoding) until
// the process is restarted.
const STOP_AUDIO_STREAM_DRAIN_TIMEOUT_MS = 30_000;
// Outer guard for the whole flush (in-page collect + stream drain), set above
// the sum of its two internal budgets so it only fires if something new inside
// flushAudio blocks unexpectedly.
const STOP_AUDIO_FLUSH_TIMEOUT_MS =
  STOP_AUDIO_COLLECT_TIMEOUT_MS + STOP_AUDIO_STREAM_DRAIN_TIMEOUT_MS + 30_000;
// Cap on waiting for the corporate watcher loops to settle during a stop. The
// loops exit within one STAFF_POLL_INTERVAL_MS tick of stoppingClassIds being
// set; the extra headroom covers a loop caught mid-join.
const STOP_CORPORATE_SETTLE_TIMEOUT_MS = 60_000;
// setTimeout fires immediately for delays above this (2^31 - 1 ms, ~24.8 days).
const MAX_TIMER_DELAY_MS = 2_147_483_647;

/**
 * Reads a millisecond duration from the environment, falling back to the
 * default for unset, empty or invalid values. Number('') is 0 and
 * Number('abc') is NaN, and setTimeout treats both as ~0 ms, so a blank env
 * var would otherwise fire these timers immediately on every session.
 */
function readDurationMsEnv(
  name: string,
  fallbackMs: number,
  { allowZero = false }: { allowZero?: boolean } = {},
): number {
  const raw = process.env[name]?.trim();
  if (!raw) return fallbackMs;
  const value = Number(raw);
  const valid =
    Number.isFinite(value) &&
    (allowZero ? value >= 0 : value > 0) &&
    value <= MAX_TIMER_DELAY_MS;
  if (!valid) {
    console.warn(
      `[RecordingWorker] Invalid ${name}=${JSON.stringify(raw)} — using default ${fallbackMs} ms`,
    );
    return fallbackMs;
  }
  return value;
}

// Absolute wall-clock cap on a registered session, enforced from the moment it
// is registered. The only other thing that closes a browser is an inbound stop
// job, so a dropped/lost/never-enqueued stop job would otherwise leave a fully
// recording Chromium alive forever. Deliberately anchored to elapsed time
// rather than the class's own startsAt/duration: those are opaque pass-through
// values here (the server decides their unit), and a mis-parsed unit could
// abort a live recording, whereas a generous fixed cap cannot.
const SESSION_MAX_LIFETIME_MS = readDurationMsEnv(
  'RECORDING_SESSION_MAX_LIFETIME_MS',
  4 * 60 * 60 * 1000,
);
// Bots are billed Daily participants, so once no real user has been in the call
// for this long the session stops, uploads and leaves. 0 disables the feature.
const IDLE_TIMEOUT_MS = readDurationMsEnv('RECORDING_IDLE_TIMEOUT_MS', 10 * 60 * 1000, {
  allowZero: true,
});
// The page re-reports its count every 30 s. A page silent for longer than this
// (crashed, frozen) means the room state is unknown, which never counts as idle;
// the session watchdog covers dead pages instead.
const IDLE_REPORT_STALE_MS = readDurationMsEnv('RECORDING_IDLE_REPORT_STALE_MS', 60_000);
// How long to remember a class this worker stopped by itself (idle / watchdog),
// so the server's delayed auto-stop that arrives later is treated as a no-op
// instead of being reported as a missing session.
const SELF_STOPPED_TTL_MS = 6 * 60 * 60 * 1000;

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
  // Classes inside startClassRecording, i.e. not yet in activeSessions. Their
  // bot pages may already be reporting presence.
  private startingClassIds = new Set<number>();
  private sessionWatchdogs = new Map<number, ReturnType<typeof setTimeout>>();
  // Idle exit: latest real-user count reported by each bot page, per class.
  private presenceReports = new Map<number, Map<string, PresenceReport>>();
  private idleTimers = new Map<number, ReturnType<typeof setTimeout>>();
  // Classes whose idle stop job is queued but not yet processed. A real user
  // joining in the meantime removes the class, which cancels that job.
  private idleStopRequested = new Set<number>();
  // classId → expiry time (ms).
  private selfStoppedClassIds = new Map<number, number>();
  private idleStopHandler: ((workoutClassId: number) => Promise<void>) | null = null;
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
    this.startingClassIds.add(workoutClassId);

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
        segmentFinalizations: [],
        segmentResults: new Map(),
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
      this.armSessionWatchdog(workoutClassId);
      browser = null;

      console.log(
        `[RecordingWorker] ${this.joinAs} session registered for class ${workoutClassId}`,
      );
      // The client bot may already have reported an empty room while joining,
      // before the session existed to arm a timer for.
      this.evaluateIdleState(workoutClassId);
    } catch (err) {
      await this.closeEntries(entries);
      if (browser) {
        await this.closeBrowserHard(browser, `class ${workoutClassId} (start failed)`);
      }
      this.clearSessionWatchdog(workoutClassId);
      this.clearIdleTracking(workoutClassId);
      this.activeSessions.delete(workoutClassId);
      throw err;
    } finally {
      this.startingClassIds.delete(workoutClassId);
    }
  }

  private async startCorporateRecordingBots(
    workoutClassId: number,
    session: ActiveSession,
    joinAs: 'trainer' | 'coach',
  ): Promise<void> {
    let corporateAuthSession: AuthSession | null = null;
    // The mirror bot for the staff member's current stay in the call, closed
    // when they leave so it is not left recording (and billed) on its own.
    let currentEntry: RecordingEntry | null = null;

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
      currentEntry = entry;
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
    } else if (!this.isSessionLive(workoutClassId, session)) {
      // Stop gave up waiting on this setup and closed the browser already.
      console.log(
        `[RecordingWorker] Corporate ${joinAs} setup abandoned for class ${workoutClassId} — session already stopped`,
      );
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
    // Keeps running until this session is stopped (see isSessionLive).
    // When the staff member leaves, their mirror bot is closed and its segment
    // uploaded in the background. When they rejoin, a fresh recording context
    // is opened and appended to session.entries so it gets uploaded on stop.
    while (this.isSessionLive(workoutClassId, session)) {
      await new Promise<void>((resolve) =>
        setTimeout(resolve, STAFF_POLL_INTERVAL_MS),
      );
      if (!this.isSessionLive(workoutClassId, session)) break;

      try {
        const presence = await this.fetchStaffPresence(workoutClassId);
        // The stop may have begun (and snapshotted the segment list) while the
        // request was in flight; acting now would race its uploads and cleanup.
        if (!this.isSessionLive(workoutClassId, session)) break;
        const isInCall =
          joinAs === 'trainer' ? presence.trainerInCall : presence.coachInCall;

        if (!isInCall && wasInCall && currentEntry) {
          console.log(
            `[RecordingWorker] ${joinAs} left class ${workoutClassId} — closing their mirror bot and uploading its segment`,
          );
          this.finalizeDetachedSegment(workoutClassId, session, currentEntry);
          currentEntry = null;
        }

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
    // flushAudio is internally bounded, but keep an outer guard so this call
    // site can never become the one unbounded await in the stop path again.
    await withTimeout(
      entry.flushAudio(),
      STOP_AUDIO_FLUSH_TIMEOUT_MS,
      `${perspective} audio flush (outer) for class ${workoutClassId}`,
    ).catch((err) => {
      console.warn(
        `[RecordingWorker] Audio flush exceeded its outer budget for ${perspective} class ${workoutClassId}, continuing:`,
        err instanceof Error ? err.message : err,
      );
    });

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

  /**
   * Closes a mirror bot whose staff member left the call, uploading its
   * segment in the background so the watcher loop keeps polling for a rejoin.
   */
  private finalizeDetachedSegment(
    workoutClassId: number,
    session: ActiveSession,
    entry: RecordingEntry,
  ): void {
    const index = session.entries.indexOf(entry);
    if (index !== -1) session.entries.splice(index, 1);
    this.removePresenceReport(workoutClassId, entry.presenceReporterId);

    const work = (async () => {
      let succeeded = false;
      try {
        const { videoPath, audioFilePath, audioStartMs, screenshot } =
          await this.closeRecordingContext(entry, workoutClassId);
        if (videoPath) {
          await this.finalizeRecordingEntry(
            workoutClassId,
            entry,
            videoPath,
            audioFilePath,
            audioStartMs,
            screenshot,
          );
          succeeded = true;
        }
      } catch (err) {
        console.error(
          `[RecordingWorker] Error finalizing departed ${entry.perspective} segment for class ${workoutClassId}:`,
          err,
        );
        await entry.diagnostics.dispose().catch(() => undefined);
        await entry.context.close().catch(() => undefined);
      }
      session.segmentResults.set(
        entry.perspective,
        succeeded || session.segmentResults.get(entry.perspective) === true,
      );
    })();
    session.segmentFinalizations.push(work);
  }

  async stopClassRecording(
    workoutClassId: number,
    reason: StopReason = 'job',
  ): Promise<void> {
    const session = this.activeSessions.get(workoutClassId);
    if (!session) {
      console.log(`[RecordingWorker] No active session for class ${workoutClassId}`);
      return;
    }
    // A stop job and a watchdog stop can overlap; only the first may proceed.
    if (this.stoppingClassIds.has(workoutClassId)) {
      console.log(
        `[RecordingWorker] Stop already in progress for class ${workoutClassId} — ignoring ${reason} stop`,
      );
      return;
    }

    console.log(
      `[RecordingWorker] Stopping class ${workoutClassId} (reason: ${reason})`,
    );
    this.stoppingClassIds.add(workoutClassId);
    this.clearSessionWatchdog(workoutClassId);
    this.clearIdleTracking(workoutClassId);
    if (reason !== 'job') {
      this.markSelfStopped(workoutClassId);
    }

    if (this.recordsCorporate()) {
      console.log(
        `[RecordingWorker] Waiting for corporate bot setup to finish for class ${workoutClassId}...`,
      );
      // Bounded: the watcher loop honours stoppingClassIds within one poll
      // tick, but if it is mid-openPerspective (untimed newContext /
      // addInitScript / exposeFunction calls against a wedged browser) this
      // promise may never settle — which would strand the stop before the
      // finally that closes Chromium, on a concurrency-1 worker.
      await withTimeout(
        session.corporateSetupPromise,
        STOP_CORPORATE_SETTLE_TIMEOUT_MS,
        `corporate setup settle for class ${workoutClassId}`,
      ).catch((err) => {
        console.warn(
          `[RecordingWorker] Corporate setup ended with error / did not settle for class ${workoutClassId}:`,
          err instanceof Error ? err.message : err,
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

      // Segments closed mid-class upload first, so the live segment's upload
      // below is the one the server keeps. They must also finish before the
      // finally removes this class's temp directory.
      if (session.segmentFinalizations.length > 0) {
        console.log(
          `[RecordingWorker] Waiting for ${session.segmentFinalizations.length} departed segment upload(s) for class ${workoutClassId}`,
        );
        await Promise.allSettled(session.segmentFinalizations);
      }
      for (const [perspective, succeeded] of session.segmentResults) {
        attemptedPerspectives.add(perspective);
        if (succeeded) succeededPerspectives.add(perspective);
      }

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
      await this.closeBrowserHard(session.browser, `class ${workoutClassId}`);
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
    this.clearSessionWatchdog(workoutClassId);
    this.clearIdleTracking(workoutClassId);
    this.activeSessions.delete(workoutClassId);
    // Bounded so this path (used as the watchdog fallback and by shutdown)
    // always reaches the browser close below.
    await withTimeout(
      session.corporateSetupPromise,
      STOP_CORPORATE_SETTLE_TIMEOUT_MS,
      `corporate setup settle for class ${workoutClassId} (force)`,
    ).catch(() => undefined);
    this.stoppingClassIds.delete(workoutClassId);
    await this.closeEntries(session.entries);
    await this.closeBrowserHard(session.browser, `class ${workoutClassId} (force)`);
    console.log(`[RecordingWorker] Force-closed browser for class ${workoutClassId}`);
  }

  /**
   * Arms the safety net for a freshly registered session.
   *
   * An inbound stop job is otherwise the *only* thing that closes a browser, so
   * any lost stop job (API-side failure, Redis eviction, an API deploy mid
   * class, or a wedged stop worker) leaves a fully recording Chromium alive
   * indefinitely. This guarantees every session is eventually reclaimed.
   */
  private armSessionWatchdog(workoutClassId: number): void {
    this.clearSessionWatchdog(workoutClassId);

    const timer = setTimeout(() => {
      this.sessionWatchdogs.delete(workoutClassId);
      if (
        !this.activeSessions.has(workoutClassId) ||
        this.stoppingClassIds.has(workoutClassId)
      ) {
        return;
      }
      console.error(
        `[RecordingWorker] Watchdog: class ${workoutClassId} still active after ` +
          `${Math.round(SESSION_MAX_LIFETIME_MS / 60_000)} min with no stop job — stopping and uploading what was recorded`,
      );
      // Normal stop first so the recording is uploaded and the server
      // notified; every step in it is time-boxed. Force-close only if it fails.
      void this.stopClassRecording(workoutClassId, 'watchdog').catch(async (err) => {
        console.error(
          `[RecordingWorker] Watchdog stop failed for class ${workoutClassId} — force-closing:`,
          err,
        );
        await this.forceCloseSession(workoutClassId).catch((closeErr) =>
          console.error(
            `[RecordingWorker] Watchdog force-close failed for class ${workoutClassId}:`,
            closeErr,
          ),
        );
      });
    }, SESSION_MAX_LIFETIME_MS);

    // Never let a pending watchdog be the reason the process stays alive.
    timer.unref?.();
    this.sessionWatchdogs.set(workoutClassId, timer);
  }

  /**
   * True while `session` is the registered, not-stopping session for the class.
   * Background loops check this rather than stoppingClassIds alone: a stop
   * clears stoppingClassIds once it has waited (bounded) for those loops, so a
   * loop it gave up on would otherwise never see the signal and run forever.
   */
  private isSessionLive(workoutClassId: number, session: ActiveSession): boolean {
    return (
      this.activeSessions.get(workoutClassId) === session &&
      !this.stoppingClassIds.has(workoutClassId)
    );
  }

  private clearSessionWatchdog(workoutClassId: number): void {
    const timer = this.sessionWatchdogs.get(workoutClassId);
    if (!timer) return;
    clearTimeout(timer);
    this.sessionWatchdogs.delete(workoutClassId);
  }

  // ── Idle exit ───────────────────────────────────────────────────────────
  // Each bot page reports how many real (non-bot, non-admin) users are in the
  // call. When none has been present for IDLE_TIMEOUT_MS, the class is stopped
  // through this worker's own stop queue, so the recording is still uploaded.

  /** Wires the idle stop to the stop queue (owned by index.ts). */
  setIdleStopHandler(handler: (workoutClassId: number) => Promise<void>): void {
    this.idleStopHandler = handler;
  }

  /** False once a real user has returned after the idle stop was queued. */
  isIdleStopStillWanted(workoutClassId: number): boolean {
    return this.idleStopRequested.has(workoutClassId);
  }

  /** True if this worker stopped the class itself (idle or watchdog) recently. */
  wasSelfStopped(workoutClassId: number): boolean {
    const now = Date.now();
    for (const [classId, expiresAt] of this.selfStoppedClassIds) {
      if (expiresAt <= now) this.selfStoppedClassIds.delete(classId);
    }
    return this.selfStoppedClassIds.has(workoutClassId);
  }

  private markSelfStopped(workoutClassId: number): void {
    this.selfStoppedClassIds.set(workoutClassId, Date.now() + SELF_STOPPED_TTL_MS);
  }

  private recordPresenceReport(
    workoutClassId: number,
    reporterId: string,
    perspective: Perspective,
    count: number,
  ): void {
    if (!Number.isFinite(count) || count < 0) return;
    // Pages keep reporting until their context is closed, which during a stop
    // is after clearIdleTracking; storing those would leak one entry per class.
    const acceptsReports =
      this.startingClassIds.has(workoutClassId) ||
      (this.activeSessions.has(workoutClassId) &&
        !this.stoppingClassIds.has(workoutClassId));
    if (!acceptsReports) return;
    let reports = this.presenceReports.get(workoutClassId);
    if (!reports) {
      reports = new Map();
      this.presenceReports.set(workoutClassId, reports);
    }
    const previous = reports.get(reporterId);
    reports.set(reporterId, { count, at: Date.now() });
    if (previous?.count !== count) {
      console.log(
        `[RecordingWorker] Idle: ${perspective} bot sees ${count} real user(s) in class ${workoutClassId}`,
      );
    }
    this.evaluateIdleState(workoutClassId);
  }

  private removePresenceReport(workoutClassId: number, reporterId: string): void {
    const reports = this.presenceReports.get(workoutClassId);
    if (!reports) return;
    reports.delete(reporterId);
    if (reports.size === 0) this.presenceReports.delete(workoutClassId);
  }

  /**
   * Highest real-user count among recent reports (several bots can watch the
   * same call), or null when no page has reported recently (state unknown).
   */
  private getFreshRealCount(workoutClassId: number): number | null {
    const reports = this.presenceReports.get(workoutClassId);
    if (!reports) return null;
    const cutoff = Date.now() - IDLE_REPORT_STALE_MS;
    let highest: number | null = null;
    for (const report of reports.values()) {
      if (report.at >= cutoff) highest = Math.max(highest ?? 0, report.count);
    }
    return highest;
  }

  private evaluateIdleState(workoutClassId: number): void {
    if (IDLE_TIMEOUT_MS <= 0) return;
    if (
      !this.activeSessions.has(workoutClassId) ||
      this.stoppingClassIds.has(workoutClassId)
    ) {
      return;
    }

    const realCount = this.getFreshRealCount(workoutClassId);
    if (realCount === null) return;

    if (realCount > 0) {
      if (this.idleStopRequested.delete(workoutClassId)) {
        console.log(
          `[RecordingWorker] Idle: real user returned to class ${workoutClassId} — cancelling queued idle stop`,
        );
      }
      if (this.clearIdleTimer(workoutClassId)) {
        console.log(
          `[RecordingWorker] Idle: real user present in class ${workoutClassId} — idle timer reset`,
        );
      }
      return;
    }

    if (
      this.idleTimers.has(workoutClassId) ||
      this.idleStopRequested.has(workoutClassId)
    ) {
      return;
    }
    this.armIdleTimer(workoutClassId);
  }

  private armIdleTimer(workoutClassId: number): void {
    this.clearIdleTimer(workoutClassId);
    console.log(
      `[RecordingWorker] Idle: no real users in class ${workoutClassId} — ` +
        `stopping in ${Math.round(IDLE_TIMEOUT_MS / 60_000)} min unless someone joins`,
    );
    const timer = setTimeout(() => {
      this.idleTimers.delete(workoutClassId);
      void this.onIdleTimeout(workoutClassId).catch((err) =>
        console.error(
          `[RecordingWorker] Idle: timeout handling failed for class ${workoutClassId}:`,
          err,
        ),
      );
    }, IDLE_TIMEOUT_MS);
    timer.unref?.();
    this.idleTimers.set(workoutClassId, timer);
  }

  private clearIdleTimer(workoutClassId: number): boolean {
    const timer = this.idleTimers.get(workoutClassId);
    if (!timer) return false;
    clearTimeout(timer);
    this.idleTimers.delete(workoutClassId);
    return true;
  }

  private clearIdleTracking(workoutClassId: number): void {
    this.clearIdleTimer(workoutClassId);
    this.idleStopRequested.delete(workoutClassId);
    this.presenceReports.delete(workoutClassId);
  }

  private isIdleCandidate(workoutClassId: number): boolean {
    return (
      this.activeSessions.has(workoutClassId) &&
      !this.stoppingClassIds.has(workoutClassId) &&
      !this.idleTimers.has(workoutClassId) &&
      this.getFreshRealCount(workoutClassId) === 0
    );
  }

  private async onIdleTimeout(workoutClassId: number): Promise<void> {
    if (
      !this.activeSessions.has(workoutClassId) ||
      this.stoppingClassIds.has(workoutClassId)
    ) {
      return;
    }
    if (this.getFreshRealCount(workoutClassId) === null) {
      console.warn(
        `[RecordingWorker] Idle: timer fired for class ${workoutClassId} but no bot has reported in ` +
          `${Math.round(IDLE_REPORT_STALE_MS / 1000)}s — state unknown, not stopping`,
      );
      return;
    }
    if (!this.isIdleCandidate(workoutClassId)) return;

    // One server-side check before acting, so a stale page cannot end a live
    // recording.
    let serverRealCount: number | undefined;
    try {
      const presence = await this.fetchStaffPresence(workoutClassId, {
        includeRealParticipantCount: true,
      });
      serverRealCount = presence.realParticipantCount;
    } catch (err) {
      console.warn(
        `[RecordingWorker] Idle: could not confirm presence for class ${workoutClassId} — re-arming:`,
        err instanceof Error ? err.message : err,
      );
      if (this.isIdleCandidate(workoutClassId)) this.armIdleTimer(workoutClassId);
      return;
    }

    // State may have moved while the request was in flight.
    if (!this.isIdleCandidate(workoutClassId)) return;

    if (serverRealCount === undefined) {
      console.warn(
        `[RecordingWorker] Idle: server did not return a real participant count for class ${workoutClassId} — relying on page reports`,
      );
    } else if (serverRealCount > 0) {
      console.warn(
        `[RecordingWorker] Idle: pages report 0 real users in class ${workoutClassId} but the server sees ${serverRealCount} — re-arming`,
      );
      this.armIdleTimer(workoutClassId);
      return;
    }

    if (!this.idleStopHandler) {
      console.warn(
        `[RecordingWorker] Idle: no stop handler registered — cannot stop class ${workoutClassId}`,
      );
      return;
    }

    console.log(
      `[RecordingWorker] Idle: class ${workoutClassId} has had no real users for ` +
        `${Math.round(IDLE_TIMEOUT_MS / 60_000)} min — queueing stop (reason: idle)`,
    );
    this.idleStopRequested.add(workoutClassId);
    try {
      await this.idleStopHandler(workoutClassId);
    } catch (err) {
      this.idleStopRequested.delete(workoutClassId);
      console.error(
        `[RecordingWorker] Idle: failed to queue stop for class ${workoutClassId} — re-arming:`,
        err,
      );
      if (this.isIdleCandidate(workoutClassId)) this.armIdleTimer(workoutClassId);
    }
  }

  /**
   * Single bounded path for closing a session browser.
   *
   * Every close site must be time-boxed: an unbounded browser.close() against
   * a wedged Chromium blocks the caller, and on the concurrency-1 stop worker
   * that strands the only stop slot, so every later class leaks a live
   * Chromium (still in the WebRTC call, still encoding) until restart.
   *
   * Known gap: if close() times out, the OS process may survive and this
   * process cannot reap it — chromium.launch() gives no public handle on the
   * browser's ChildProcess (Playwright exposes process() only on
   * BrowserServer/ElectronApplication). A true hard-kill needs either
   * launchServer() + connect() (which changes how recordVideo artifacts are
   * resolved) or an out-of-band pid reaper. The timeout below at least keeps
   * the worker itself alive and loudly flags the orphan.
   */
  private async closeBrowserHard(browser: Browser, label: string): Promise<void> {
    await withTimeout(
      browser.close(),
      STOP_CONTEXT_CLOSE_TIMEOUT_MS,
      `browser close for ${label}`,
    ).catch((err) => {
      console.error(
        `[RecordingWorker] Browser close failed/timed out for ${label} — a Chromium process may now be orphaned:`,
        err instanceof Error ? err.message : err,
      );
    });
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

  private async fetchStaffPresence(
    workoutClassId: number,
    options?: { includeRealParticipantCount?: boolean },
  ): Promise<StaffPresence> {
    // The real participant count costs the server a DB lookup, so it is only
    // requested for the idle confirmation, not on every watcher poll.
    const query = options?.includeRealParticipantCount
      ? '?includeRealParticipantCount=true'
      : '';
    const url = `${this.apiBase}/class-recording/worker/staff-presence/${workoutClassId}${query}`;
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
    presenceReporterId: string;
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
    // Track stream errors so they can be surfaced to the page caller, which
    // lets the page-side retry mechanism decide whether to retry or give up.
    let audioStreamError: Error | null = null;
    audioStream.on('error', (err) => {
      audioStreamError = err;
      console.error(
        `[RecordingWorker] Audio stream error for class ${workoutClassId} (${perspective}):`,
        err,
      );
    });
    let totalAudioBytes = 0;
    let audioStreamClosed = false;
    const audioStreamDone = new Promise<void>((resolve) => {
      audioStream.on('finish', resolve);
      audioStream.on('error', () => resolve());
      // destroy() (see destroyAudio) emits 'close' but never 'finish', so
      // without this listener the promise would be permanently unresolvable
      // after a destroy.
      audioStream.on('close', () => resolve());
    });

    // Idempotent: safe to call from both the page-driven hook and the
    // worker-side flush, whichever gets there first.
    const endAudioStream = () => {
      if (audioStreamClosed) return;
      audioStreamClosed = true;
      audioStream.end();
    };

    await context.exposeFunction('__saveAudioChunk', async (b64: string) => {
      if (!b64 || audioStreamClosed) return;
      // Propagate any prior stream error back to the page so the retry loop
      // on the page side can act on it rather than silently dropping the chunk.
      if (audioStreamError) throw audioStreamError;
      const chunk = Buffer.from(b64, 'base64');
      // Write with a callback so write errors are returned as a rejection to
      // the page caller.  Backpressure is signalled by write() returning false;
      // the callback still fires once the buffer drains, so awaiting it is
      // sufficient to honour backpressure without a separate drain listener.
      await new Promise<void>((resolve, reject) => {
        const ok = audioStream.write(chunk, (err) => {
          if (err) reject(err);
          else resolve();
        });
        if (!ok) {
          // Backpressure detected — resolution happens via the callback above.
        }
      });
      totalAudioBytes += chunk.byteLength;
    });

    await context.exposeFunction('__finishAudioRecording', async () => {
      endAudioStream();
      await audioStreamDone;
      console.log(
        `[RecordingWorker] Audio stream closed for class ${workoutClassId} (${perspective}) — ` +
          `${(totalAudioBytes / 1024).toFixed(1)} KB on disk`,
      );
    });

    // Idle exit: the page (useRecordingBotPresenceReporter) pushes the number
    // of real users in the call on every change and every 30 s.
    const presenceReporterId = uuidv4();
    await context.exposeFunction('__reportRealParticipantCount', (count: number) => {
      this.recordPresenceReport(workoutClassId, presenceReporterId, perspective, Number(count));
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
      }

      // Always end the stream from the worker side, even when the page is still
      // open. Normally the in-page hook ends it via __finishAudioRecording, but
      // if that evaluate timed out, threw, or the hook was never installed (the
      // page never reached the joined state), nothing would ever call end() —
      // and the wait below would never settle.
      endAudioStream();

      await withTimeout(
        audioStreamDone,
        STOP_AUDIO_STREAM_DRAIN_TIMEOUT_MS,
        `${perspective} audio stream drain for class ${workoutClassId}`,
      ).catch((err) => {
        // Give up rather than block: a truncated audio file still muxes, and
        // proceeding is strictly better than wedging the stop worker.
        console.warn(
          `[RecordingWorker] Audio stream did not drain for ${perspective} class ${workoutClassId}, continuing:`,
          err instanceof Error ? err.message : err,
        );
      });
    };

    const destroyAudio = () => {
      audioStreamClosed = true;
      audioStream.destroy();
    };

    return { context, page, videoRecordingStartMs, diagnostics, videoDir, audioFilePath, presenceReporterId, flushAudio, destroyAudio };
  }

  private async openClientRecordingContext(
    browser: Browser,
    joinDetails: JoinDetails,
    workoutClassId: number,
    authSession: AuthSession,
  ): Promise<RecordingEntry> {
    let lastError: unknown;

    for (let attempt = 1; attempt <= RECORDING_JOIN_ATTEMPTS; attempt++) {
      const { context, page, videoRecordingStartMs, diagnostics, videoDir, audioFilePath, presenceReporterId, flushAudio, destroyAudio } =
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
        return { context, page, perspective: 'client', videoRecordingStartMs, diagnostics, audioFilePath, presenceReporterId, flushAudio, destroyAudio };
      } catch (err) {
        await diagnostics.logCrashReport(
          `client join attempt ${attempt} failed: ${err instanceof Error ? err.message : String(err)}`,
        );
        await diagnostics.dispose();
        destroyAudio();
        this.removePresenceReport(workoutClassId, presenceReporterId);
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
      const { context, page, videoRecordingStartMs, diagnostics, videoDir, audioFilePath, presenceReporterId, flushAudio, destroyAudio } =
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
        return { context, page, perspective, videoRecordingStartMs, diagnostics, audioFilePath, presenceReporterId, flushAudio, destroyAudio };
      } catch (err) {
        await diagnostics.logCrashReport(
          `${perspective} corporate join attempt ${attempt} failed: ${err instanceof Error ? err.message : String(err)}`,
        );
        await diagnostics.dispose();
        destroyAudio();
        this.removePresenceReport(workoutClassId, presenceReporterId);
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
