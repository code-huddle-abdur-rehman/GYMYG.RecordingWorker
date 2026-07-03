import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { promises as fs } from 'fs';
import os from 'os';
import path from 'path';
import { chromium, Browser, BrowserContext, Page } from 'playwright';
import { v4 as uuidv4 } from 'uuid';

import { muxVideoWithAudio, removeFileIfExists, writeTempFile } from './media.js';

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

interface ActiveSession {
  entries: Array<{
    context: BrowserContext;
    page: Page;
    perspective: Perspective;
    videoRecordingStartMs: number;
  }>;
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

export class RecordingSessionManager {
  readonly joinAs: Perspective;

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

  private parseJoinAs(): Perspective {
    const value = process.env.JOIN_AS?.trim().toLowerCase();
    if (!value || !['client', 'trainer', 'coach'].includes(value)) {
      throw new Error('JOIN_AS must be set to one of: client, trainer, coach');
    }
    return value as Perspective;
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
        ignoreDefaultArgs: ['--mute-audio'],
        args: [
          '--use-fake-ui-for-media-stream',
          '--use-fake-device-for-media-stream',
          '--autoplay-policy=no-user-gesture-required',
          '--disable-dev-shm-usage',
          '--disable-gpu',
          '--no-sandbox',
        ],
      });

      if (this.joinAs === 'client') {
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

      if (this.joinAs === 'trainer' || this.joinAs === 'coach') {
        session.corporateSetupPromise = this.startCorporateRecordingBots(
          workoutClassId,
          session,
          this.joinAs,
        );
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

    const trainerTask = (async () => {
      try {
        const presence = await this.waitForStaffPresence(
          workoutClassId,
          'trainer',
          STAFF_WAIT_TIMEOUT_MS,
        );
        if (!presence) {
          console.warn(
            `[RecordingWorker] Trainer not in call within ${STAFF_WAIT_TIMEOUT_MS / 1000}s for class ${workoutClassId} — skipping trainer recording`,
          );
          await this.notifyFailed(workoutClassId, 'trainer');
          return;
        }
        await openPerspective('trainer', 'trainer');
      } catch (err) {
        console.error(
          `[RecordingWorker] Failed to start trainer corporate bot for class ${workoutClassId}:`,
          err,
        );
        await this.notifyFailed(workoutClassId, 'trainer');
      }
    })();

    const coachTask = (async () => {
      try {
        const presence = await this.waitForStaffPresence(
          workoutClassId,
          'coach',
          STAFF_WAIT_TIMEOUT_MS,
        );
        if (!presence?.coachLiveViewRole) {
          console.warn(
            `[RecordingWorker] Coach not in call within ${STAFF_WAIT_TIMEOUT_MS / 1000}s for class ${workoutClassId} — skipping coach recording`,
          );
          await this.notifyFailed(workoutClassId, 'coach');
          return;
        }
        await openPerspective('coach', presence.coachLiveViewRole);
      } catch (err) {
        console.error(
          `[RecordingWorker] Failed to start coach corporate bot for class ${workoutClassId}:`,
          err,
        );
        await this.notifyFailed(workoutClassId, 'coach');
      }
    })();

    if (joinAs === 'trainer') {
      await trainerTask;
    } else {
      await coachTask;
    }

    console.log(
      `[RecordingWorker] Corporate ${joinAs} setup finished for class ${workoutClassId}: ${session.entries.length} active context(s)`,
    );
  }

  async stopClassRecording(workoutClassId: number): Promise<void> {
    const session = this.activeSessions.get(workoutClassId);
    if (!session) {
      console.log(`[RecordingWorker] No active session for class ${workoutClassId}`);
      return;
    }

    this.stoppingClassIds.add(workoutClassId);

    console.log(
      `[RecordingWorker] Waiting for corporate bot setup to finish for class ${workoutClassId}...`,
    );
    await session.corporateSetupPromise.catch((err) => {
      console.warn(
        `[RecordingWorker] Corporate setup ended with error for class ${workoutClassId}:`,
        err,
      );
    });

    this.activeSessions.delete(workoutClassId);
    this.stoppingClassIds.delete(workoutClassId);

    try {
      for (const entry of session.entries) {
        try {
          const { buffer: audioBuffer, audioStartMs } = await this.collectAudioFromPage(
            entry.page,
            entry.perspective,
          );
          const video = entry.page.video();
          const screenshot = await entry.page
            .screenshot({ type: 'png' })
            .catch(() => undefined);
          await entry.context.close();
          const videoPath = video ? await video.path() : null;
          if (videoPath) {
            const tempDir = path.dirname(videoPath);
            const uploadPath = await this.prepareUploadVideo(
              videoPath,
              audioBuffer,
              entry.videoRecordingStartMs,
              audioStartMs,
            );
            try {
              await this.uploadRecording(
                workoutClassId,
                entry.perspective,
                uploadPath,
                screenshot,
              );
            } finally {
              await removeFileIfExists(videoPath);
              if (uploadPath !== videoPath) {
                await removeFileIfExists(uploadPath);
              }
              await fs.rm(tempDir, { recursive: true, force: true }).catch(() => undefined);
            }
          } else {
            await this.notifyFailed(workoutClassId, entry.perspective);
          }
        } catch (err) {
          console.error('[RecordingWorker] Error closing context:', err);
          await entry.context.close().catch(() => undefined);
          await this.notifyFailed(workoutClassId, entry.perspective);
        }
      }
    } finally {
      await session.browser.close().catch((err) => {
        console.error(
          `[RecordingWorker] Failed to close browser for class ${workoutClassId}:`,
          err,
        );
      });
      console.log(`[RecordingWorker] Browser closed for class ${workoutClassId}`);
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
      entries.map((entry) => entry.context.close().catch(() => undefined)),
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

  private async createRecordingContext(
    browser: Browser,
    workoutClassId: number,
    perspective: Perspective,
  ): Promise<{ context: BrowserContext; page: Page; videoRecordingStartMs: number }> {
    const videoRecordingStartMs = Date.now();
    const videoDir = path.join(
      os.tmpdir(),
      'gymyg-recording-worker',
      String(workoutClassId),
      perspective,
    );
    await fs.mkdir(videoDir, { recursive: true });

    const context = await browser.newContext({
      recordVideo: {
        dir: videoDir,
        size: { width: 1280, height: 720 },
      },
      viewport: { width: 1280, height: 720 },
      ignoreHTTPSErrors: true,
      permissions: [],
    });

    (context as { __perspective?: Perspective }).__perspective = perspective;

    const page = await context.newPage();
    page.on('dialog', async (dialog) => {
      console.log(`[RecordingWorker] Dismissing browser dialog: ${dialog.message()}`);
      await dialog.dismiss().catch(() => undefined);
    });

    return { context, page, videoRecordingStartMs };
  }

  private async openClientRecordingContext(
    browser: Browser,
    joinDetails: JoinDetails,
    workoutClassId: number,
    authSession: AuthSession,
  ): Promise<{
    context: BrowserContext;
    page: Page;
    perspective: Perspective;
    videoRecordingStartMs: number;
  }> {
    const { context, page, videoRecordingStartMs } = await this.createRecordingContext(
      browser,
      workoutClassId,
      'client',
    );

    await this.seedAuthSession(context, authSession);

    const recordingUrl = this.buildClientRecordingUrl(joinDetails);
    console.log(`[RecordingWorker] Navigating client bot to ${recordingUrl}`);

    await page.goto(recordingUrl, { waitUntil: 'domcontentloaded', timeout: 120000 });

    if (page.url().includes('/authentication/clientLogin')) {
      throw new Error(
        'Recording bot was redirected to client login. Check EMAIL/PASSWORD credentials.',
      );
    }

    await this.waitForSessionJoined(page, 'client', false);
    return { context, page, perspective: 'client', videoRecordingStartMs };
  }

  private async openCorporateRecordingContext(
    browser: Browser,
    workoutClassId: number,
    perspective: 'trainer' | 'coach',
    liveViewRole: string,
    authSession: AuthSession,
  ): Promise<{
    context: BrowserContext;
    page: Page;
    perspective: Perspective;
    videoRecordingStartMs: number;
  }> {
    const { context, page, videoRecordingStartMs } = await this.createRecordingContext(
      browser,
      workoutClassId,
      perspective,
    );

    try {
      await this.seedAuthSession(context, authSession);

      const recordingUrl = this.buildCorporateRecordingUrl(
        workoutClassId,
        perspective,
        liveViewRole,
      );
      console.log(`[RecordingWorker] Navigating ${perspective} corporate bot to ${recordingUrl}`);

      await page.goto(recordingUrl, { waitUntil: 'domcontentloaded', timeout: 120000 });

      if (
        page.url().includes('/authentication/instructorLogin') ||
        page.url().includes('/authentication/clientLogin')
      ) {
        throw new Error(
          'Corporate recording bot was redirected to login. Check CORPORATE_EMAIL/CORPORATE_PASSWORD credentials.',
        );
      }

      await this.waitForSessionJoined(page, perspective, true);
      return { context, page, perspective, videoRecordingStartMs };
    } catch (err) {
      await context.close().catch(() => undefined);
      throw err;
    }
  }

  private async waitForSessionJoined(
    page: Page,
    perspective: Perspective,
    waitForLiveViewReady: boolean,
  ): Promise<void> {
    const joinButton = page.locator('#joinCallButton');
    try {
      await joinButton.waitFor({ state: 'visible', timeout: 15000 });
      await joinButton.click();
      console.log(`[RecordingWorker] Clicked Join Call for ${perspective} bot`);
    } catch {
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
        { timeout: 120000 },
      )
      .catch(() => {
        throw new Error(
          `Recording bot failed to join the ${perspective} session within 120s`,
        );
      });

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
    } catch {
      console.warn(
        `[RecordingWorker] ${perspective} audio capture readiness marker not detected — continuing recording anyway`,
      );
    }

    await page.waitForTimeout(3000);
  }

  private async collectAudioFromPage(
    page: Page,
    perspective: Perspective,
  ): Promise<{ buffer: Buffer | null; audioStartMs: number | null }> {
    try {
      await page.evaluate(async () => {
        const resume = (window as Window & {
          __resumeRecordingBotAudio?: () => Promise<void>;
        }).__resumeRecordingBotAudio;
        if (typeof resume === 'function') {
          await resume();
        }
      });

      const audioResult = await page.evaluate(async () => {
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
      });

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
    audioBuffer: Buffer | null,
    videoRecordingStartMs: number,
    audioStartMs: number | null,
  ): Promise<string> {
    if (!audioBuffer || audioBuffer.length === 0) {
      console.warn('[RecordingWorker] Uploading video without audio track');
      return videoPath;
    }

    const tempDir = path.dirname(videoPath);
    const audioPath = await writeTempFile(tempDir, `audio-${uuidv4()}.webm`, audioBuffer);
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
        '[RecordingWorker] Failed to mux audio into video, uploading video-only file:',
        err,
      );
      return videoPath;
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

    const fileBuffer = await fs.readFile(videoPath);
    await this.s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: s3Key,
        Body: fileBuffer,
        ContentType: 'video/webm',
        ACL: 'public-read',
      }),
    );

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

  async notifySessionMissing(workoutClassId: number): Promise<void> {
    const url = `${this.apiBase}/class-recording/worker/session-missing/${workoutClassId}/${this.joinAs}`;
    await fetch(url, {
      method: 'POST',
      headers: { 'x-recording-worker-key': this.workerKey },
    }).catch((err) =>
      console.error('[RecordingWorker] Failed to notify missing session:', err),
    );
  }
}
