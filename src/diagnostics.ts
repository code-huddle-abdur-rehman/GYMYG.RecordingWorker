import { promises as fs } from 'fs';
import os from 'os';
import type { CDPSession, Page, Request } from 'playwright';

type Perspective = 'client' | 'coach' | 'trainer';

interface ConsoleRecord {
  at: string;
  type: string;
  text: string;
}

interface FailedRequestRecord {
  at: string;
  method: string;
  resourceType: string;
  url: string;
  errorText: string;
}

interface RlimitInfo {
  soft: number | 'unlimited';
  hard: number | 'unlimited';
}

interface ProcessFdInfo {
  pid: number;
  name: string;
  fdCount: number | null;
  limit: RlimitInfo | null;
}

interface DiskUsage {
  totalMb: number;
  freeMb: number;
  usedPct: number;
}

interface SystemResourceSnapshot {
  platform: string;
  loadAvg: number[];
  totalMemMb: number;
  freeMemMb: number;
  processRssMb: number | null;
  nodeFdCount: number | null;
  nodeFdLimit: RlimitInfo | null;
  chromiumProcesses: ProcessFdInfo[];
  chromiumFdTotal: number | null;
  diskUsage: Record<string, DiskUsage>;
}

interface PageMetricSnapshot {
  jsHeapUsedMb: number | null;
  jsHeapTotalMb: number | null;
  documents: number | null;
  frames: number | null;
  nodes: number | null;
  jsEventListeners: number | null;
  layoutCount: number | null;
}

// Rolling-buffer sizes. Kept small so a long-running bot never accumulates
// unbounded memory just for diagnostics.
const MAX_CONSOLE_RECORDS = 200;
const MAX_ERROR_RECORDS = 100;
const MAX_FAILED_REQUEST_RECORDS = 200;

// Network error codes that indicate the renderer is running out of OS-level
// resources (sockets / file descriptors) rather than RAM. These are the errors
// that precede an ERR_INSUFFICIENT_RESOURCES renderer crash.
const RESOURCE_EXHAUSTION_ERRORS = [
  'ERR_INSUFFICIENT_RESOURCES',
  'ERR_NETWORK_IO_SUSPENDED',
  'ERR_OUT_OF_MEMORY',
];

function nowIso(): string {
  return new Date().toISOString();
}

function parseRlimit(value: string): number | 'unlimited' {
  return value === 'unlimited' ? 'unlimited' : parseInt(value, 10);
}

async function readProcessRlimit(pid: number | 'self'): Promise<RlimitInfo | null> {
  try {
    const raw = await fs.readFile(`/proc/${pid}/limits`, 'utf-8');
    const match = raw.match(/Max open files\s+(\S+)\s+(\S+)/);
    if (!match) return null;
    return { soft: parseRlimit(match[1]), hard: parseRlimit(match[2]) };
  } catch {
    return null;
  }
}

async function countProcessFds(pid: number | 'self'): Promise<number | null> {
  try {
    const entries = await fs.readdir(`/proc/${pid}/fd`);
    return entries.length;
  } catch {
    return null;
  }
}

async function readProcessRssMb(pid: number | 'self'): Promise<number | null> {
  try {
    const raw = await fs.readFile(`/proc/${pid}/status`, 'utf-8');
    const match = raw.match(/VmRSS:\s+(\d+)\s+kB/);
    if (!match) return null;
    return Math.round(parseInt(match[1], 10) / 1024);
  } catch {
    return null;
  }
}

async function readProcessName(pid: number): Promise<string> {
  try {
    const comm = await fs.readFile(`/proc/${pid}/comm`, 'utf-8');
    return comm.trim();
  } catch {
    return 'unknown';
  }
}

// Chromium process names as they appear in /proc/<pid>/comm. `headless_shell`
// is the new headless binary; `chrome`/`chromium` cover older builds.
const CHROMIUM_COMM_PATTERN = /^(chrome|chromium|headless_shell|nacl_helper)/i;

/**
 * Scan /proc for every Chromium-family process (browser, renderer,
 * network-service, GPU, utility) and count each one's open file descriptors
 * against its own soft limit. ERR_INSUFFICIENT_RESOURCES is triggered per
 * process when it runs out of fds/sockets, so the per-process breakdown — not
 * just a host total — is what pinpoints the exhausted renderer.
 */
async function collectChromiumProcessFds(): Promise<{
  processes: ProcessFdInfo[];
  total: number | null;
}> {
  try {
    const procEntries = await fs.readdir('/proc');
    const pids = procEntries
      .filter((entry) => /^\d+$/.test(entry))
      .map((entry) => parseInt(entry, 10));

    const processes: ProcessFdInfo[] = [];
    let total = 0;
    await Promise.all(
      pids.map(async (pid) => {
        const name = await readProcessName(pid);
        if (!CHROMIUM_COMM_PATTERN.test(name)) return;
        const [fdCount, limit] = await Promise.all([
          countProcessFds(pid),
          readProcessRlimit(pid),
        ]);
        if (fdCount != null) total += fdCount;
        processes.push({ pid, name, fdCount, limit });
      }),
    );
    processes.sort((a, b) => (b.fdCount ?? 0) - (a.fdCount ?? 0));
    return { processes, total: processes.length ? total : null };
  } catch {
    return { processes: [], total: null };
  }
}

async function readDiskUsage(dirPath: string): Promise<DiskUsage | null> {
  try {
    const stats = await fs.statfs(dirPath);
    const blockSize = stats.bsize;
    const totalMb = Math.round((stats.blocks * blockSize) / 1024 / 1024);
    const freeMb = Math.round((stats.bavail * blockSize) / 1024 / 1024);
    if (totalMb === 0) return null;
    const usedPct = Math.round(((totalMb - freeMb) / totalMb) * 100);
    return { totalMb, freeMb, usedPct };
  } catch {
    return null;
  }
}

/**
 * Reports free space on the volumes that matter for a crash: the RAM-backed
 * tmpfs mounts (/tmp, /dev/shm) that Chromium uses for shared memory, and the
 * disk where recordings are written. A full tmpfs is what produces the
 * "Disk quota exceeded (122)" font-service FATAL that crashes the renderer.
 */
async function collectDiskUsage(): Promise<Record<string, DiskUsage>> {
  const candidates = Array.from(
    new Set(
      [
        os.tmpdir(),
        '/tmp',
        '/dev/shm',
        '/var/tmp',
        process.env.RECORDING_TMP_DIR,
      ].filter((p): p is string => Boolean(p)),
    ),
  );
  const result: Record<string, DiskUsage> = {};
  await Promise.all(
    candidates.map(async (dirPath) => {
      const usage = await readDiskUsage(dirPath);
      if (usage) result[dirPath] = usage;
    }),
  );
  return result;
}

export async function collectSystemResources(): Promise<SystemResourceSnapshot> {
  const [nodeFdCount, nodeFdLimit, processRssMb, chromium, diskUsage] =
    await Promise.all([
      countProcessFds('self'),
      readProcessRlimit('self'),
      readProcessRssMb('self'),
      collectChromiumProcessFds(),
      collectDiskUsage(),
    ]);

  return {
    diskUsage,
    platform: process.platform,
    loadAvg: os.loadavg().map((n) => Number(n.toFixed(2))),
    totalMemMb: Math.round(os.totalmem() / 1024 / 1024),
    freeMemMb: Math.round(os.freemem() / 1024 / 1024),
    processRssMb,
    nodeFdCount,
    nodeFdLimit,
    chromiumProcesses: chromium.processes,
    chromiumFdTotal: chromium.total,
  };
}

/**
 * Logs the effective open-file (RLIMIT_NOFILE) limits for the Node process and,
 * once a browser is launched, for the Chromium process tree. Call at startup
 * and after each browser launch so the logs make it obvious whether the fd
 * limit is high enough for WebRTC before a crash happens.
 */
export async function logFileDescriptorLimits(context: string): Promise<void> {
  const limit = await readProcessRlimit('self');
  if (!limit) return; // non-Linux
  const softNum = limit.soft === 'unlimited' ? Infinity : limit.soft;
  console.log(
    `[RecordingWorker][diag] (${context}) open-file limit — soft: ${limit.soft}, hard: ${limit.hard}`,
  );
  if (softNum < 8192) {
    console.warn(
      `[RecordingWorker][diag] ⚠ LOW open-file soft limit (${limit.soft}). ` +
        'WebRTC recording bots that subscribe to many tracks will hit ' +
        'ERR_INSUFFICIENT_RESOURCES and crash the renderer. ' +
        'Fix: set LimitNOFILE=65536 on the systemd unit (or raise the hard ' +
        'limit) so `ulimit -n 65536` in the start script can take effect, then ' +
        'daemon-reload and restart the service.',
    );
  }
}

/**
 * Per-page diagnostics collector. Records a rolling window of console output,
 * page errors, and failed network requests, and can produce a rich crash
 * report (system fd usage + renderer metrics + recent activity) on demand.
 */
export class PageDiagnostics {
  private consoleRecords: ConsoleRecord[] = [];
  private errorRecords: string[] = [];
  private failedRequests: FailedRequestRecord[] = [];
  private failureCounts = new Map<string, number>();
  private requestsStarted = 0;
  private requestsFinished = 0;
  private requestsFailed = 0;
  private inFlight = new Set<Request>();
  private peakInFlight = 0;
  private cdpSession: CDPSession | null = null;
  private cdpEnabled = false;
  private samplingTimer: NodeJS.Timeout | null = null;
  private reported = false;
  private readonly startedAt = Date.now();

  constructor(
    private readonly page: Page,
    private readonly perspective: Perspective,
    private readonly classId: number,
  ) {}

  private get tag(): string {
    return `[RecordingWorker][${this.perspective}] class ${this.classId}`;
  }

  private push<T>(buffer: T[], record: T, max: number): void {
    buffer.push(record);
    if (buffer.length > max) buffer.shift();
  }

  attach(): void {
    this.page.on('console', (msg) => {
      const type = msg.type();
      if (type === 'error' || type === 'warning' || type === 'info') {
        this.push(
          this.consoleRecords,
          { at: nowIso(), type, text: msg.text() },
          MAX_CONSOLE_RECORDS,
        );
      }
    });

    this.page.on('pageerror', (error) => {
      this.push(
        this.errorRecords,
        `${nowIso()} ${error.name}: ${error.message}`,
        MAX_ERROR_RECORDS,
      );
    });

    this.page.on('request', (request) => {
      this.requestsStarted += 1;
      this.inFlight.add(request);
      if (this.inFlight.size > this.peakInFlight) {
        this.peakInFlight = this.inFlight.size;
      }
    });

    this.page.on('requestfinished', (request) => {
      this.requestsFinished += 1;
      this.inFlight.delete(request);
    });

    this.page.on('requestfailed', (request) => {
      this.requestsFailed += 1;
      this.inFlight.delete(request);
      const errorText = request.failure()?.errorText ?? 'unknown';
      const normalized = errorText.replace(/^net::/, '');
      this.failureCounts.set(
        normalized,
        (this.failureCounts.get(normalized) ?? 0) + 1,
      );
      this.push(
        this.failedRequests,
        {
          at: nowIso(),
          method: request.method(),
          resourceType: request.resourceType(),
          url: request.url(),
          errorText,
        },
        MAX_FAILED_REQUEST_RECORDS,
      );

      // Surface resource-exhaustion failures the moment they happen so the log
      // shows the lead-up to the crash, not just the crash itself.
      if (RESOURCE_EXHAUSTION_ERRORS.some((code) => errorText.includes(code))) {
        const count = this.failureCounts.get(normalized) ?? 0;
        console.error(
          `${this.tag}: RESOURCE-EXHAUSTION network failure #${count} (${errorText}) ` +
            `on ${request.resourceType()} ${request.method()} ${request.url()} — ` +
            `in-flight requests: ${this.inFlight.size}, peak: ${this.peakInFlight}`,
        );
      }
    });
  }

  private async getCdpSession(): Promise<CDPSession | null> {
    if (this.cdpSession) return this.cdpSession;
    try {
      this.cdpSession = await this.page.context().newCDPSession(this.page);
      await this.cdpSession.send('Performance.enable').catch(() => undefined);
      this.cdpEnabled = true;
      return this.cdpSession;
    } catch {
      return null;
    }
  }

  private async collectPageMetrics(): Promise<PageMetricSnapshot | null> {
    if (!this.cdpEnabled) {
      // If CDP is unavailable (e.g. page already crashed) fall back to null.
      const session = await this.getCdpSession();
      if (!session) return null;
    }
    try {
      const session = this.cdpSession;
      if (!session) return null;
      const { metrics } = (await session.send('Performance.getMetrics')) as {
        metrics: Array<{ name: string; value: number }>;
      };
      const byName = new Map(metrics.map((m) => [m.name, m.value]));
      const toMb = (bytes?: number) =>
        bytes != null ? Math.round(bytes / 1024 / 1024) : null;
      return {
        jsHeapUsedMb: toMb(byName.get('JSHeapUsedSize')),
        jsHeapTotalMb: toMb(byName.get('JSHeapTotalSize')),
        documents: byName.get('Documents') ?? null,
        frames: byName.get('Frames') ?? null,
        nodes: byName.get('Nodes') ?? null,
        jsEventListeners: byName.get('JSEventListeners') ?? null,
        layoutCount: byName.get('LayoutCount') ?? null,
      };
    } catch {
      return null;
    }
  }

  private failureCountsObject(): Record<string, number> {
    return Object.fromEntries(
      [...this.failureCounts.entries()].sort((a, b) => b[1] - a[1]),
    );
  }

  networkSummary(): Record<string, unknown> {
    return {
      requestsStarted: this.requestsStarted,
      requestsFinished: this.requestsFinished,
      requestsFailed: this.requestsFailed,
      inFlight: this.inFlight.size,
      peakInFlight: this.peakInFlight,
      failureCounts: this.failureCountsObject(),
    };
  }

  /**
   * Starts periodic resource sampling so the logs capture the growth in fd
   * usage / heap / in-flight requests leading up to a crash. Automatically
   * stops itself once a crash report is emitted.
   */
  startSampling(intervalMs = 15_000): void {
    if (this.samplingTimer) return;
    this.samplingTimer = setInterval(() => {
      void this.logSample();
    }, intervalMs);
    // Don't keep the event loop alive solely for sampling.
    this.samplingTimer.unref?.();
  }

  stopSampling(): void {
    if (this.samplingTimer) {
      clearInterval(this.samplingTimer);
      this.samplingTimer = null;
    }
  }

  private async logSample(): Promise<void> {
    try {
      const [metrics, system] = await Promise.all([
        this.collectPageMetrics(),
        collectSystemResources(),
      ]);
      console.log(`${this.tag}: resource sample`, {
        uptimeSec: Math.round((Date.now() - this.startedAt) / 1000),
        network: this.networkSummary(),
        pageMetrics: metrics,
        system: {
          loadAvg: system.loadAvg,
          freeMemMb: system.freeMemMb,
          nodeFdCount: system.nodeFdCount,
          nodeFdLimit: system.nodeFdLimit,
          chromiumFdTotal: system.chromiumFdTotal,
          chromiumProcessCount: system.chromiumProcesses.length,
          diskUsage: system.diskUsage,
        },
      });
    } catch {
      // Never let sampling throw into the recording flow.
    }
  }

  /**
   * Emits a single, comprehensive crash/failure report with everything needed
   * to diagnose the cause without reproducing: recent console output, page
   * errors, failed-request breakdown, renderer metrics, and the full Chromium
   * process-tree fd usage against the OS limit.
   */
  async logCrashReport(reason: string): Promise<void> {
    if (this.reported) return; // Only the first (root) failure is the useful one.
    this.reported = true;
    this.stopSampling();

    let currentUrl = 'unknown';
    try {
      currentUrl = this.page.url();
    } catch {
      /* page may be gone */
    }

    const [metrics, system] = await Promise.all([
      this.collectPageMetrics().catch(() => null),
      collectSystemResources().catch(() => null),
    ]);

    console.error(
      `\n================ ${this.tag}: CRASH DIAGNOSTIC REPORT ================`,
    );
    console.error(`${this.tag}: reason: ${reason}`);
    console.error(`${this.tag}: url: ${currentUrl}`);
    console.error(
      `${this.tag}: uptime before failure: ${Math.round(
        (Date.now() - this.startedAt) / 1000,
      )}s`,
    );
    console.error(`${this.tag}: network summary:`, this.networkSummary());

    if (metrics) {
      console.error(`${this.tag}: renderer metrics:`, metrics);
    } else {
      console.error(
        `${this.tag}: renderer metrics unavailable (CDP unreachable — page likely already dead)`,
      );
    }

    if (system) {
      console.error(`${this.tag}: system resources:`, {
        platform: system.platform,
        loadAvg: system.loadAvg,
        totalMemMb: system.totalMemMb,
        freeMemMb: system.freeMemMb,
        nodeRssMb: system.processRssMb,
        nodeFdCount: system.nodeFdCount,
        nodeFdLimit: system.nodeFdLimit,
        chromiumFdTotal: system.chromiumFdTotal,
        diskUsage: system.diskUsage,
      });

      // A nearly-full tmpfs (/tmp or /dev/shm) is what makes Chromium's
      // shared-memory/font writes fail with "Disk quota exceeded (122)" and
      // crash the renderer, so call it out explicitly.
      for (const [mount, usage] of Object.entries(system.diskUsage)) {
        if (usage.usedPct >= 90 || usage.freeMb <= 50) {
          console.error(
            `${this.tag}: VERDICT — low temp/disk space on ${mount}: ` +
              `${usage.freeMb}MB free of ${usage.totalMb}MB (${usage.usedPct}% used). ` +
              'This causes Chromium "Disk quota exceeded (122)" renderer crashes. ' +
              'Move large temp files off this volume or grow it.',
          );
        }
      }
      if (system.chromiumProcesses.length) {
        console.error(
          `${this.tag}: chromium process fd usage (top consumers):`,
          system.chromiumProcesses.slice(0, 10).map((p) => ({
            pid: p.pid,
            name: p.name,
            fdCount: p.fdCount,
            fdLimitSoft: p.limit?.soft,
          })),
        );
      }

      // Explicit verdict so the on-call engineer doesn't have to interpret raw
      // numbers under pressure.
      const softLimit =
        system.chromiumProcesses[0]?.limit?.soft ?? system.nodeFdLimit?.soft;
      const softNum = softLimit === 'unlimited' ? Infinity : softLimit;
      if (
        system.chromiumFdTotal != null &&
        typeof softNum === 'number' &&
        system.chromiumFdTotal > softNum * 0.8
      ) {
        console.error(
          `${this.tag}: VERDICT — file-descriptor exhaustion. Chromium is using ` +
            `${system.chromiumFdTotal} fds against a soft limit of ${softLimit}. ` +
            'Raise LimitNOFILE and/or reduce the number of subscribed media tracks.',
        );
      }
    }

    const resourceFailures = [...this.failureCounts.entries()].filter(([code]) =>
      RESOURCE_EXHAUSTION_ERRORS.some((e) => code.includes(e)),
    );
    if (resourceFailures.length) {
      console.error(
        `${this.tag}: VERDICT — resource-exhaustion network failures detected:`,
        Object.fromEntries(resourceFailures),
        '(this is a socket/fd/decoder resource issue, not RAM)',
      );
    }

    if (this.failedRequests.length) {
      console.error(
        `${this.tag}: last ${Math.min(this.failedRequests.length, 25)} failed requests:`,
      );
      for (const req of this.failedRequests.slice(-25)) {
        console.error(
          `${this.tag}:   [${req.at}] ${req.errorText} ${req.resourceType} ${req.method} ${req.url}`,
        );
      }
    }

    if (this.errorRecords.length) {
      console.error(
        `${this.tag}: last ${Math.min(this.errorRecords.length, 15)} page errors:`,
      );
      for (const err of this.errorRecords.slice(-15)) {
        console.error(`${this.tag}:   ${err}`);
      }
    }

    if (this.consoleRecords.length) {
      console.error(
        `${this.tag}: last ${Math.min(this.consoleRecords.length, 30)} console messages:`,
      );
      for (const rec of this.consoleRecords.slice(-30)) {
        console.error(`${this.tag}:   [${rec.at}][${rec.type}] ${rec.text}`);
      }
    }

    console.error(
      `================ ${this.tag}: END CRASH DIAGNOSTIC REPORT ================\n`,
    );
  }

  async dispose(): Promise<void> {
    this.stopSampling();
    if (this.cdpSession) {
      await this.cdpSession.detach().catch(() => undefined);
      this.cdpSession = null;
    }
  }
}
