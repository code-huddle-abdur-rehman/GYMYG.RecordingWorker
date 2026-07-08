import type { RedisOptions } from 'ioredis';

/**
 * Parse the raw fields out of a REDIS_URL (redis:// or rediss://).
 * This only extracts host/port/db/password/tls — the resilience/failover
 * behaviour is layered on in {@link buildRedisConnection}.
 */
export function parseRedisUrl(url: string): {
  host: string;
  port: number;
  db?: number;
  password?: string;
  tls?: { rejectUnauthorized: boolean };
} {
  const parsed = new URL(url);
  const config: {
    host: string;
    port: number;
    db?: number;
    password?: string;
    tls?: { rejectUnauthorized: boolean };
  } = {
    host: parsed.hostname,
    port: parseInt(parsed.port) || 6379,
  };

  if (parsed.pathname && parsed.pathname.length > 1) {
    const dbNumber = parseInt(parsed.pathname.substring(1));
    if (!isNaN(dbNumber)) {
      config.db = dbNumber;
    }
  }

  if (parsed.password) {
    config.password = parsed.password;
  }

  if (parsed.protocol === 'rediss:') {
    config.tls = { rejectUnauthorized: false };
  }

  return config;
}

/**
 * Redis reply substrings that mean "you are talking to the wrong / a demoted
 * node after a failover". When we see any of these we must tear down the
 * current socket and reconnect so ioredis re-resolves the ElastiCache primary
 * endpoint DNS and lands on the newly-promoted master.
 *
 * Why this matters: on an ElastiCache (or any Sentinel/replica) failover the
 * node ioredis is attached to is demoted from master to replica. The TCP
 * connection stays up, so ioredis keeps sending writes to a read-only replica
 * and every BullMQ command fails with READONLY forever — until the process is
 * restarted by hand. Reconnecting on these errors makes recovery automatic.
 */
const FAILOVER_ERROR_MARKERS = [
  'READONLY',
  "READONLY You can't write against a read only replica",
  'UNBLOCKED',
  'MASTERDOWN',
  'CLUSTERDOWN',
  'LOADING',
];

function isFailoverError(err: Error): boolean {
  const message = (err?.message ?? '').toUpperCase();
  return FAILOVER_ERROR_MARKERS.some((marker) =>
    message.includes(marker.toUpperCase()),
  );
}

/**
 * Build the BullMQ/ioredis connection options with automatic failover
 * recovery. Use this for every Queue and Worker so a Redis primary failover
 * heals itself instead of wedging the worker on a read-only replica.
 */
export function buildRedisConnection(url: string): RedisOptions {
  const base = parseRedisUrl(url);

  return {
    ...base,
    // Required by BullMQ: blocking commands (bzpopmin) must never be aborted
    // by a per-request retry cap, otherwise the worker throws on transient
    // Redis jitter.
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
    // Keep the socket healthy across NAT/idle timeouts on long-lived workers.
    keepAlive: 10_000,
    connectTimeout: 15_000,
    // Reconnect forever with a bounded backoff. `times` grows each attempt.
    retryStrategy: (times: number): number => Math.min(times * 200, 5_000),
    // The core failover fix: on a READONLY/UNBLOCKED/etc. reply, disconnect and
    // reconnect (re-resolving the primary endpoint DNS) and resend the failed
    // command. Returning 2 means "reconnect AND resend the pending command".
    reconnectOnError: (err: Error): boolean | 1 | 2 => {
      if (isFailoverError(err)) {
        console.warn(
          `[RecordingWorker] Redis failover detected (${err.message}); reconnecting to primary...`,
        );
        return 2;
      }
      return false;
    },
  };
}
