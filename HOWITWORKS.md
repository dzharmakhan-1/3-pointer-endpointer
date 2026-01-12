## How each file looks like:
**docker-compose.yml**:
```
version: "3.9"

services:

  api:
    build: .
    container_name: taxi_api
    command: uvicorn app:app --host 0.0.0.0 --port 8000
    ports:
      - "8000:8000"
    env_file:
      - .env
    depends_on:
      - postgres
      - rabbitmq

  worker:
    build: .
    container_name: taxi_worker
    command: celery -A celery_config.celery_app worker --loglevel=info
    env_file:
      - .env
    depends_on:
      - rabbitmq
      - postgres

  rabbitmq:
    image: rabbitmq:3-management
    container_name: rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"

  postgres:
    image: postgres:latest
    container_name: CONTAINER
    environment:
      POSTGRES_DB: DB
      POSTGRES_USER: USER
      POSTGRES_PASSWORD: PASSWORD
    ports:
      - "5434:5432"
    volumes:
      - pgdata:/var/lib/postgresql

volumes:
  pgdata:
```
**dockerfile**:
```
FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Default command (overridden in docker-compose)
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```
**requirements.txt**
```
fastapi
uvicorn[standard]
pandas
sqlalchemy
psycopg2-binary
boto3
celery
python-multipart
requests
markdown
groq
pyarrow
fastparquet
```
**.env** (for credentials):
```
POSTGRES_USER=USER
POSTGRES_PASSWORD=PASSWORD
POSTGRES_DB=DB
DATABASE_URL=postgresql://postgres:your_password_here@postgres:PORT/DB

GROQ_API_KEY=your_groq_key_here

AWS_ACCESS_KEY_ID=your_aws_key_here
AWS_SECRET_ACCESS_KEY=your_aws_secret_here
AWS_DEFAULT_REGION=your_server_region
```
**tasks.py**
```
import os
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine
from celery_config import celery_app
import boto3
from botocore.exceptions import ClientError

POSTGRE_LINK = os.getenv("DATABASE_URL")
engine = create_engine(POSTGRE_LINK)

KEEP_COLUMNS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "PULocationID", "DOLocationID", "payment_type",
    "total_amount", "congestion_surcharge", "Airport_fee"
]

s3_client = boto3.client('s3')

@celery_app.task(name="process_taxi_from_s3")
def process_taxi_from_s3(bucket_name: str, s3_key: str, table_name: str = "yellow_taxi_trips", append: bool = True, original_filename: str = ""):
    try:
        # Download from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        contents = response['Body'].read()
        parquet_buffer = BytesIO(contents)
        df = pd.read_parquet(parquet_buffer)

        original_rows = len(df)
        original_cols = len(df.columns)

        # Clean (same as before)
        existing_keep_cols = [col for col in KEEP_COLUMNS if col in df.columns]
        df = df[existing_keep_cols]
        df.dropna(subset=[
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "PULocationID", "DOLocationID"
        ], inplace=True)

        fee_columns = ["congestion_surcharge", "Airport_fee"]
        for col in fee_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        df = df[df["trip_distance"] > 0]
        df = df[df["total_amount"] >= 0]
        df = df[(df["passenger_count"] >= 1) & (df["passenger_count"] <= 8)]

        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

        df["trip_duration_min"] = (
            df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
        ).dt.total_seconds() / 60
        df = df[df["trip_duration_min"] > 0]

        # Failsafe: Drop duplicates within this file (based on unique trip keys)
        df.drop_duplicates(subset=["tpep_pickup_datetime", "PULocationID", "DOLocationID"], inplace=True)

        cleaned_rows = len(df)
        cleaned_cols = len(df.columns)


        # Load to Postgres with failsafe (append, but skip dupes if DB has unique constraint)
        if_exists_mode = "append" if append else "replace"
        try:
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists_mode,
                index=False,
                method="multi",
                chunksize=10000
            )
        except Exception as db_e:
            # If DB error (e.g., dupes violate unique), fallback to chunked insert with ignore dupes
            # Assumes you ran the ALTER TABLE for unique constraint
            return {"status": "error", "error": f"DB load error (possible dupes): {str(db_e)}"}

        return {
            "status": "success",
            "message": "Pipeline complete: Downloaded from S3, cleaned, saved, loaded to Postgres!",
            "original_rows": original_rows,
            "cleaned_rows": cleaned_rows,
            "rows_removed": original_rows - cleaned_rows,
            "s3_source": f"s3://{bucket_name}/{s3_key}",
            "table_name": table_name,
            "append_mode": append
        }
    except ClientError as e:
        return {"status": "error", "error": f"S3 download error: {str(e)}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}
```
**app.py**:
```
from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import pandas as pd
import requests
from io import BytesIO
import os
from sqlalchemy import create_engine, text, cast
from botocore.exceptions import ClientError
import boto3
from tasks import process_taxi_from_s3
from celery.result import AsyncResult
import base64
from groq import Groq
import json

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

app = FastAPI(title="NYC Yellow Taxi Data Processor")

POSTGRE_LINK = os.getenv("DATABASE_URL")
engine = create_engine(POSTGRE_LINK)

s3_client = boto3.client('s3')

@app.post("/upload")
async def upload(
    file: UploadFile = File(...),
    table_name: str = "table_name",
    append: bool = True,
    bucket_name: str = "bucket_name",
    s3_prefix: str = "extra_folder_if_needed/"
):
    if not file.filename.endswith(".parquet"):
        raise HTTPException(status_code=400, detail="Only .parquet files are allowed")

    try:
        contents = await file.read()
        s3_key = s3_prefix + file.filename

        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=contents
        )

        # Queue Celery task to process from S3
        task = process_taxi_from_s3.delay(bucket_name, s3_key, table_name, append, file.filename)

        return JSONResponse({
            "message": "File uploaded to S3 and processing queued!",
            "s3_path": f"s3://{bucket_name}/{s3_key}",
            "task_id": task.id,
            "status": "queued",
            "check_status": f"/task-status/{task.id}"
        })

    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"AWS Error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    task_result = AsyncResult(task_id)
    if task_result.state == "PENDING":
        return {"task_id": task_id, "status": "pending"}
    elif task_result.state == "SUCCESS":
        return {"task_id": task_id, "status": "success", "result": task_result.result}
    elif task_result.state == "FAILURE":
        return {"task_id": task_id, "status": "failure", "error": str(task_result.result)}
    else:
        return {"task_id": task_id, "status": task_result.state}

@app.get("/load-taxi-zones-dim")
async def load_taxi_zones_dim(table_name: str = "dim_taxi_zones"):
    """
    Download and load the official NYC taxi zone lookup into PostgreSQL as a dim table.
    Run this once to create your dimension table for location mapping.
    """
    try:
        zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
        df_zones = pd.read_csv(zone_url)

        df_zones.columns = df_zones.columns.str.lower()
        df_zones.drop_duplicates(subset=["locationid"], inplace=True)

        df_zones.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000
        )

        return {
            "message": "Taxi zones dim table loaded successfully!",
            "table_name": table_name,
            "rows_loaded": len(df_zones),
            "columns": list(df_zones.columns)
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error loading dim table: {str(e)}")

@app.get("/")
async def root():
    return {"message": "Go to /docs for the automated pipeline endpoint!"}

from markdown import markdown
def markdown_to_html(text: str) -> str:
    """Convert markdown text to HTML"""
    return markdown(text, extensions=['fenced_code', 'tables', 'nl2br'])

@app.post("/analyze-dashboard")
async def analyze_dashboard(
    file: UploadFile = File(...),
    custom_prompt: str = "Analyze this PowerBI dashboard screenshot in detail. Extract all key metrics, describe each chart/visual, identify trends, anomalies, top performers, and provide actionable business insights. Structure your response with sections: Key Metrics, Chart Breakdown, Trends & Insights, Recommendations.",
    download_html: bool = True,
    save_to_db: bool = True
):
    if not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Only image files (PNG/JPG) are allowed")

    try:
        contents = await file.read()
        base64_image = base64.b64encode(contents).decode('utf-8')

        response = client.chat.completions.create(
            model="meta-llama/llama-4-scout-17b-16e-instruct",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": custom_prompt},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:{file.content_type};base64,{base64_image}"}
                        }
                    ]
                }
            ],
            temperature=0.5,
            max_tokens=2000
        )

        report_md = response.choices[0].message.content

        # Prepare structured report data for DB
        report_data = {
            "screenshot_filename": file.filename,
            "generated_at": __import__('datetime').datetime.utcnow().isoformat() + "Z",
            "prompt_used": custom_prompt,
            "model": "meta-llama/llama-4-scout-17b-16e-instruct",
            "raw_markdown_report": report_md,
            "structured_insights": {"full_report": report_md}
        }

        # Save to Postgres (independent of HTML return)
        if save_to_db:
            try:
                report_json_str  = json.dumps(report_data)
                
                with engine.connect() as conn:
                    
                    conn.execute(
                        text("""
                             INSERT INTO dashboard_analysis_reports (screenshot_filename, report_json) 
                             VALUES (:filename, CAST(:report_json AS jsonb))
                             """),
                        {
                            "filename": file.filename,
                            "report_json": report_json_str
                        }
                    )
                    conn.commit()
                
                print(f"Successfully saved AI report for '{file.filename}' to Postgres!")
            except Exception as db_err:
                print(f"Warning: Failed to save report to DB: {db_err}")

        # Generate beautiful HTML report (always done)
        report_html_body = markdown(report_md, extensions=['nl2br', 'tables'])

        full_html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Dashboard Analysis - {file.filename}</title>
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #f1f5f9; color: #1e293b; line-height: 1.8; padding: 40px; }}
                .container {{ max-width: 1000px; margin: 0 auto; background: white; padding: 50px; border-radius: 16px; box-shadow: 0 10px 40px rgba(0,0,0,0.08); }}
                h1 {{ color: #1e40af; text-align: center; border-bottom: 4px solid #3b82f6; padding-bottom: 15px; }}
                h2 {{ color: #1d4ed8; background: #eff6ff; padding: 15px; border-left: 6px solid #3b82f6; border-radius: 8px; margin-top: 40px; }}
                strong {{ color: #1e40af; }}
                ul, ol {{ padding-left: 30px; }}
                li {{ margin: 12px 0; }}
                .footer {{ text-align: center; margin-top: 70px; color: #64748b; font-size: 0.95em; padding-top: 20px; border-top: 1px solid #e2e8f0; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>AI Dashboard Analysis Report</h1>
                <p><strong>Source File:</strong> {file.filename}</p>
                <p><strong>Generated:</strong> {__import__('datetime').datetime.now().strftime('%B %d, %Y at %H:%M')}</p>
                <hr style="border: 1px solid #e2e8f0; margin: 30px 0;">
                
                {report_html_body}
                
                <div class="footer">
                    Generated by your FastAPI + Groq AI Vision Analyzer
                </div>
            </div>
        </body>
        </html>
        """

        # Return HTML download or JSON
        if download_html:
            html_bytes = BytesIO(full_html.encode('utf-8'))
            filename = file.filename.rsplit('.', 1)[0] + "_analysis_report.html"
            return StreamingResponse(
                html_bytes,
                media_type="text/html",
                headers={"Content-Disposition": f"attachment; filename=\"{filename}\""}
            )
        else:
            return {"report": report_md, "html": full_html, "saved_to_db": save_to_db}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis error: {str(e)}")
```
**celery_config.py**:
```
from celery import Celery
import os

celery_app = Celery(
    "taxi_processor",
    broker="amqp://guest:guest@rabbitmq:5672//",
    backend="rpc://",
    include=["tasks"]
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)
```
## Open terminal or PowerShell:
In dockerfile repository (use cd path/to/your/docker/file if needed) type:
```
docker-compose up --build
```
Then wait for docker to build your containers. It will start Swagger, RabbitMQ + Celery (for queuing), and log in to AWS + PostgreSQL server and Groq API.
## Using either Swagger or Postman, you can:
    1. Upload parquet files to AWS, load them back into docker to start cleaning and load to PostgreSQL server
    2. Download taxi-zones dim table to PostgreSQL
    3. Check either in Swagger or Postman task status
    4. Load your PowerBI dashboard for AI report, it will automatically load your report in JSON format to PostgreSQL, however you can also download it in readable HTML format
    
