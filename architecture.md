# Architecture

This service is a containerized data pipeline with an API layer, background worker, and shared infrastructure services.

## Components
- FastAPI app (`app.py`): HTTP API, S3 upload, readiness checks, Groq analysis
- Celery worker (`tasks.py`): parquet download, cleaning, and DB load
- RabbitMQ: Celery broker
- Postgres: analytics storage
- S3: raw parquet storage
- Groq API: vision model for dashboard analysis

## Data flow
1. Client uploads a parquet file to `POST /upload`.
2. API stores the file in S3 and enqueues a Celery task.
3. Worker downloads parquet from S3, cleans data, and writes to Postgres.
4. Client polls `GET /task-status/{task_id}` until success or failure.

## Dashboard analysis flow
1. Client uploads one or more dashboard images to `POST /analyze-dashboard`.
2. API sends images + prompt to Groq.
3. API returns a rendered HTML report and optionally stores JSON in Postgres.

## Startup behavior
- On startup, the API checks for `dim_taxi_zones` and loads it if missing or empty.

## Deployment topology (docker-compose)
```
             +---------------------+
             |  Client (curl/UI)   |
             +----------+----------+
                        |
                        v
                 +-------------+
                 |  FastAPI    |
                 |  (api)      |
                 +------+------+ 
                        |  enqueue
                        v
                  +-----------+
                  | RabbitMQ |
                  +-----+-----+
                        |
                        v
                  +-----------+
                  |  Celery   |
                  |  worker   |
                  +-----+-----+
                        |
           +------------+-------------+
           v                          v
       +-------+                  +--------+
       |  S3   |                  | Postgres|
       +-------+                  +--------+
```

## Key configuration points
- API/worker use `DATABASE_URL` to connect to Postgres.
- Celery broker points at `rabbitmq` service name.
- S3 bucket and key prefix are defined in `app.py`.
