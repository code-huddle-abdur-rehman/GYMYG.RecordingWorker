import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { promises as fs } from 'fs';
import path from 'path';
import { chromium, Browser, BrowserContext, Page } from 'playwright';
import { v4 as uuidv4 } from 'uuid';

import { muxVideoWithAudio, removeFileIfExists, writeTempFile } from './media.js';

type Perspective = 'client' | 'coach' | 'trainer';

interface JoinDetails {
  roomToken: string;
  roomUrl: string;
  classId: number;
  perspective: Perspective;
  webAppUrl: string;
}

interface ActiveSession {
  entries: Array<{ context: BrowserContext; page: Page; perspective: Perspective }>;
  browser: Browser;
}

interface ClientAuthSession {
  accessToken: string;
  refreshToken: string;
  user: Record<string, unknown>;
}

interface ClientLoginResponse {
  tokens: {
    access_token: string;
    refresh_token: string;
  };
  user: Record<string, unknown>;
}

export class RecordingSessionManager {
  private activeSessions = new Map<number, ActiveSession>();
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

  private async loginRecordingBot(): Promise<ClientAuthSession> {
    const { email, password } = this.getRecordingBotCredentials();
    const resp = await fetch(`${this.apiBase}/users/clientLogin`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        timezone: 'UTC',
      },
      body: JSON.stringify({ email, password }),
    });

    if (!resp.ok) {
      throw new Error(
        `Recording bot client login failed: ${resp.status} ${await resp.text()}`,
      );
    }

    const data = (await resp.json()) as ClientLoginResponse;
    if (!data.tokens?.access_token || !data.tokens?.refresh_token || !data.user) {
      throw new Error('Recording bot client login returned an invalid response.');
    }

    return {
      accessToken: data.tokens.access_token,
      refreshToken: data.tokens.refresh_token,
      user: this.filterUserData(data.user),
    };
  }

  private async seedAuthSession(
    context: BrowserContext,
    authSession: ClientAuthSession,
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
        headless: false,
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

      const perspectives: Perspective[] = ['client'];
      const authSession = await this.loginRecordingBot();
      console.log('[RecordingWorker] Recording bot authenticated as client');

      for (const perspective of perspectives) {
        const joinDetails = await this.fetchJoinDetails(workoutClassId, perspective);
        const entry = await this.openRecordingContext(
          browser,
          joinDetails,
          workoutClassId,
          perspective,
          authSession,
        );
        entries.push(entry);
      }

      this.activeSessions.set(workoutClassId, { browser, entries });
      browser = null;
      console.log(`[RecordingWorker] Client browser session open for class ${workoutClassId}`);
    } catch (err) {
      await this.closeEntries(entries);
      if (browser) {
        await browser.close().catch(() => undefined);
      }
      this.activeSessions.delete(workoutClassId);
      throw err;
    }
  }

  async stopClassRecording(workoutClassId: number): Promise<void> {
    const session = this.activeSessions.get(workoutClassId);
    if (!session) {
      console.log(`[RecordingWorker] No active session for class ${workoutClassId}`);
      return;
    }

    this.activeSessions.delete(workoutClassId);

    try {
      for (const entry of session.entries) {
        try {
          const audioBuffer = await this.collectAudioFromPage(entry.page);
          const video = entry.page.video();
          const screenshot = await entry.page
            .screenshot({ type: 'png' })
            .catch(() => undefined);
          await entry.context.close();
          const videoPath = video ? await video.path() : null;
          if (videoPath) {
            const uploadPath = await this.prepareUploadVideo(videoPath, audioBuffer);
            await this.uploadRecording(
              workoutClassId,
              entry.perspective,
              uploadPath,
              screenshot,
            );
            if (uploadPath !== videoPath) {
              await removeFileIfExists(uploadPath);
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
    this.activeSessions.delete(workoutClassId);
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

  private buildRecordingUrl(joinDetails: JoinDetails): string {
    const params = new URLSearchParams({
      classId: String(joinDetails.classId),
      recordingMode: 'true',
      recordingPerspective: joinDetails.perspective,
      roomToken: joinDetails.roomToken,
      roomUrl: joinDetails.roomUrl,
    });
    return `${this.webAppUrl}${this.workoutPath}?${params.toString()}`;
  }

  private async openRecordingContext(
    browser: Browser,
    joinDetails: JoinDetails,
    workoutClassId: number,
    perspective: Perspective,
    authSession: ClientAuthSession,
  ): Promise<{ context: BrowserContext; page: Page; perspective: Perspective }> {
    const videoDir = path.join(process.cwd(), 'recordings', String(workoutClassId));
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

    await this.seedAuthSession(context, authSession);

    (context as any).__perspective = perspective;

    const page = await context.newPage();
    page.on('dialog', async (dialog) => {
      console.log(`[RecordingWorker] Dismissing browser dialog: ${dialog.message()}`);
      await dialog.dismiss().catch(() => undefined);
    });

    const recordingUrl = this.buildRecordingUrl(joinDetails);
    console.log(`[RecordingWorker] Navigating ${perspective} bot to ${recordingUrl}`);

    await page.goto(recordingUrl, { waitUntil: 'domcontentloaded', timeout: 120000 });

    if (page.url().includes('/authentication/clientLogin')) {
      throw new Error(
        'Recording bot was redirected to client login. Check EMAIL/PASSWORD credentials.',
      );
    }

    await this.waitForClientSessionJoined(page, perspective);
    return { context, page, perspective };
  }

  private async waitForClientSessionJoined(
    page: Page,
    perspective: Perspective,
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
    await page.waitForTimeout(3000);
  }

  private async collectAudioFromPage(page: Page): Promise<Buffer | null> {
    try {
      const base64Audio = await page.evaluate(async () => {
        const stop = (window as Window & {
          __stopRecordingBotAudio?: () => Promise<string | null>;
        }).__stopRecordingBotAudio;
        if (typeof stop !== 'function') {
          return null;
        }
        return stop();
      });

      if (!base64Audio) {
        console.warn('[RecordingWorker] No page audio captured for recording bot');
        return null;
      }

      return Buffer.from(base64Audio, 'base64');
    } catch (err) {
      console.warn('[RecordingWorker] Failed to collect page audio:', err);
      return null;
    }
  }

  private async prepareUploadVideo(
    videoPath: string,
    audioBuffer: Buffer | null,
  ): Promise<string> {
    if (!audioBuffer || audioBuffer.length === 0) {
      console.warn('[RecordingWorker] Uploading video without audio track');
      return videoPath;
    }

    const tempDir = path.dirname(videoPath);
    const audioPath = await writeTempFile(tempDir, `audio-${uuidv4()}.webm`, audioBuffer);
    const outputPath = path.join(tempDir, `muxed-${uuidv4()}.webm`);

    try {
      await muxVideoWithAudio(videoPath, audioPath, outputPath);
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
    const url = `${this.apiBase}/class-recording/worker/session-missing/${workoutClassId}`;
    await fetch(url, {
      method: 'POST',
      headers: { 'x-recording-worker-key': this.workerKey },
    }).catch((err) =>
      console.error('[RecordingWorker] Failed to notify missing session:', err),
    );
  }
}
