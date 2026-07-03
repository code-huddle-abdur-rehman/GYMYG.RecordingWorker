import { execFile } from 'child_process';
import { promises as fs } from 'fs';
import { createRequire } from 'module';
import path from 'path';
import { promisify } from 'util';

const execFileAsync = promisify(execFile);
const require = createRequire(import.meta.url);

function getFfmpegPath(): string {
  const ffmpegPath = require('ffmpeg-static') as string | null;
  return ffmpegPath || 'ffmpeg';
}

export interface MuxSyncOptions {
  /** Skip this many seconds from the video start (video begins before audio). */
  videoTrimSeconds?: number;
  /** Delay audio by this many seconds to compensate for screencast capture lag. */
  audioDelaySeconds?: number;
}

function buildMuxArgs(
  videoPath: string,
  audioPath: string,
  outputPath: string,
  sync?: MuxSyncOptions,
  reencodeAudio = false,
): string[] {
  const args = ['-y'];
  const videoTrim = Math.max(0, sync?.videoTrimSeconds ?? 0);
  const audioDelay = Math.max(0, sync?.audioDelaySeconds ?? 0);

  if (videoTrim > 0) {
    args.push('-ss', videoTrim.toFixed(3));
  }
  args.push('-i', videoPath);

  if (audioDelay > 0) {
    args.push('-itsoffset', audioDelay.toFixed(3));
  }
  args.push('-i', audioPath);

  args.push('-c:v', 'copy');
  if (reencodeAudio) {
    args.push('-c:a', 'libopus', '-b:a', '128k');
  } else {
    args.push('-c:a', 'copy');
  }
  args.push('-shortest', outputPath);
  return args;
}

export async function muxVideoWithAudio(
  videoPath: string,
  audioPath: string,
  outputPath: string,
  sync?: MuxSyncOptions,
): Promise<void> {
  const ffmpeg = getFfmpegPath();
  try {
    await execFileAsync(
      ffmpeg,
      buildMuxArgs(videoPath, audioPath, outputPath, sync, false),
    );
  } catch {
    await execFileAsync(
      ffmpeg,
      buildMuxArgs(videoPath, audioPath, outputPath, sync, true),
    );
  }
}

export async function writeTempFile(
  dir: string,
  filename: string,
  data: Buffer,
): Promise<string> {
  const filePath = path.join(dir, filename);
  await fs.writeFile(filePath, data);
  return filePath;
}

export async function removeFileIfExists(filePath: string): Promise<void> {
  await fs.unlink(filePath).catch(() => undefined);
}
