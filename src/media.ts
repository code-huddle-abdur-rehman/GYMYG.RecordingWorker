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

export async function muxVideoWithAudio(
  videoPath: string,
  audioPath: string,
  outputPath: string,
): Promise<void> {
  const ffmpeg = getFfmpegPath();
  try {
    await execFileAsync(ffmpeg, [
      '-y',
      '-i',
      videoPath,
      '-i',
      audioPath,
      '-c:v',
      'copy',
      '-c:a',
      'copy',
      '-shortest',
      outputPath,
    ]);
  } catch {
    await execFileAsync(ffmpeg, [
      '-y',
      '-i',
      videoPath,
      '-i',
      audioPath,
      '-c:v',
      'copy',
      '-c:a',
      'libopus',
      '-b:a',
      '128k',
      '-shortest',
      outputPath,
    ]);
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
