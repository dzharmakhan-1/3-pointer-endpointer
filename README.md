# NYC Yellow Taxi Data Processor

FastAPI + Celery pipeline that ingests parquet taxi trip files, cleans them, and loads them into Postgres. It also includes a Groq-powered dashboard screenshot analysis endpoint.

## What it does
- Uploads `.parquet` files and stores them in S3
- Queues a Celery task to clean and load data into Postgres
- Auto-loads a taxi zones dimension table on startup
- Analyzes dashboard screenshots with Groq and optionally saves results to Postgres

## Stack
- API: FastAPI + Uvicorn
- Worker: Celery
- Broker: RabbitMQ
- Storage: S3 + Postgres
- AI: Groq API (vision model)

## Services (docker-compose)
- `api`: FastAPI app on `http://localhost:8000`
- `worker`: Celery worker
- `rabbitmq`: RabbitMQ broker (`5672` + management UI `15672`)
- `postgres`: Postgres on host port `5434`

## Configuration
Create a `.env` file (values shown are placeholders):

```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DB=taxi_db
DATABASE_URL=postgresql://postgres:your_password@postgres:5432/taxi_db

GROQ_API_KEY=your_groq_key

AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=your_region
```

Defaults used in code:
- S3 bucket: `taxi-bucket-vladyka`
- S3 prefix: `raw_taxi_data/`
- Table: `yellow_taxi_trips`

If your bucket or table differs, update `app.py`.

## Running locally
```bash
docker-compose up --build
```

API base URL: `http://localhost:8000`

## Key endpoints
- `GET /health`: liveness probe
- `GET /ready`: readiness probe (DB check)
- `POST /upload`: upload a parquet file to S3 and queue processing
- `GET /task-status/{task_id}`: check Celery task status
- `POST /analyze-dashboard`: analyze one or more dashboard images

### Example: upload parquet
```bash
curl -F "file=@your_file.parquet" http://localhost:8000/upload
```

### Example: analyze dashboard screenshots
```bash
curl -F "files=@dash1.png" -F "files=@dash2.png" http://localhost:8000/analyze-dashboard
```

## Data flow (short)
1. `POST /upload` -> S3 upload
2. Celery task reads parquet from S3
3. Task cleans and loads rows into Postgres
4. API can report task status

## Notes
- `dim_taxi_zones` is auto-loaded on startup if missing or empty.
- The worker expects RabbitMQ and Postgres to be reachable via Docker service names.
