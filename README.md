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

## Queues

- `class-recording-start` — opens **one** Playwright context (client perspective only)
- `class-recording-stop` — closes the client context, uploads video to S3, notifies API

Trainer and coach perspectives are recorded from their **real browser sessions** in the web app (not worker bots).

Deploy separately from the main NestJS EC2 instance (e.g. ECS Fargate).
