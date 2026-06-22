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
