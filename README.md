# GYMYG Recording Worker

Headless Playwright worker that consumes BullMQ jobs from the main API server and records workout classes from Client, Coach, and Trainer perspectives.

## Setup

```bash
cp .env.example .env
npm install
npx playwright install chromium
npm run dev
```

## Environment

| Variable | Description |
|---|---|
| `REDIS_URL` | Same Redis as GYMYG.Server (BullMQ) |
| `API_BASE_URL` | Backend API base including `/api` prefix |
| `RECORDING_WORKER_API_KEY` | Must match server `RECORDING_WORKER_API_KEY` |
| `WEB_APP_URL` | GYMYG web app URL |
| `WORKOUT_PATH` | Path to workout page (default `/workout`) |
| `BUCKET_NAME` | S3 bucket for uploads |
| `CLASS_RECORDING_PREFIX` | S3 key prefix (default `classRecordings`) |
| `JOIN_AS` | Which perspective to record: `client`, `trainer`, `coach`, or `all` (all three in one process) |
| `EMAIL` / `PASSWORD` | Dedicated **client** account for the client recording bot |
| `CORPORATE_EMAIL` / `CORPORATE_PASSWORD` | **Admin** account for corporate trainer/coach mirror bots |
| `RECORDING_IDLE_TIMEOUT_MS` | Stop, upload and leave once no real user (bots and live-view admins excluded) has been in the call this long (default `600000`, 10 min). `0` disables it |
| `RECORDING_IDLE_REPORT_STALE_MS` | A bot page silent for longer than this counts as "state unknown", never idle (default `60000`) |
| `RECORDING_SESSION_MAX_LIFETIME_MS` | Watchdog: stop and upload a session still running after this long with no stop job (default `14400000`, 4 h) |

Set `JOIN_AS=all` to run a single worker that records client, trainer, and coach perspectives together (~2GB+ RAM per active class). Otherwise deploy one worker per perspective (`client`, `trainer`, `coach`).

- `class-recording-start` — opens up to **three** Playwright contexts per class:
  - **Client** — joins immediately via recording bot token
  - **Trainer** — waits up to 5 minutes for the assigned trainer to join, then opens corporate admin mirror view (`?role=trainer`)
  - **Coach** — waits up to 5 minutes for the assigned coach to join, then opens corporate admin mirror view (`?role=coach1` or `coach2`)
- `class-recording-stop` — closes all contexts, uploads videos to S3, notifies API

If trainer or coach never joins within 5 minutes, that perspective is skipped and marked failed; the client recording continues independently.

Deploy separately from the main NestJS EC2 instance (e.g. ECS Fargate). Plan for ~2GB+ RAM per active recorded class (3 browser contexts).
