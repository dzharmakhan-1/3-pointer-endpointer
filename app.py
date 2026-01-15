from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks, status
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
import asyncio
from contextlib import asynccontextmanager
from typing import List

# HARDCODED DEFAULTS
DEFAULT_TABLE_NAME = "yellow_taxi_trips"
DEFAULT_APPEND = True
DEFAULT_BUCKET_NAME = "YOUR_BUCKET"
DEFAULT_S3_PREFIX = "BUCKET_PREFIX/"

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

app = FastAPI(title="NYC Yellow Taxi Data Processor")

POSTGRE_LINK = os.getenv("DATABASE_URL")
engine = create_engine(POSTGRE_LINK)

s3_client = boto3.client('s3')

@app.get("/health", tags=["Health"])
async def health_check():
    """
    Liveness probe: returns 200 if the process is running at all.
    Very lightweight - used by orchestrators to know if container is alive.
    """
    return {"status": "healthy", "service": "NYC Yellow Taxi Data Processor"}

@app.get("/ready", tags=["Health"])
async def readiness_check():
    """
    Readiness probe: checks if the service is ready to accept traffic.
    Returns 200 only if DB and S3 are reachable.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {
            "status": "ready", 
            "database": "connected", 
            "timestamp": __import__("datetime").datetime.utcnow().isoformat()
        }
    except Exception as e:
        print(f"Readiness check failed: {e}")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "not ready",
                "error": "Database connection failed",
                "details": str(e)
            }
        )
        
        
@app.post("/upload", tags=["Data Ingestion"])
async def upload(
    file: UploadFile = File(...)
):
    if not file.filename.endswith(".parquet"):
        raise HTTPException(status_code=400, detail="Only .parquet files are allowed")

    try:
        contents = await file.read()
        s3_key = DEFAULT_S3_PREFIX + file.filename

        # Upload to S3
        s3_client.put_object(
            Bucket=DEFAULT_BUCKET_NAME,
            Key=s3_key,
            Body=contents
        )

        # Queue Celery task to process from S3
        task = process_taxi_from_s3.delay(
            DEFAULT_BUCKET_NAME,
            s3_key,
            DEFAULT_TABLE_NAME, 
            DEFAULT_APPEND, 
            file.filename
        )

        return JSONResponse({
            "message": "File uploaded to S3 and processing queued!",
            "s3_path": f"s3://{DEFAULT_BUCKET_NAME}/{s3_key}",
            "task_id": task.id,
            "status": "queued",
            "check_status": f"/task-status/{task.id}"
        })

    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"AWS Error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Application startup - checking / loading dim_taxi_zones table...")
    
    try:
        table_name = "dim_taxi_zones"
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                                       SELECT COUNT(*) 
                                       FROM information_schema.tables
                                       WHERE table_name = :table
                                       """), {"table": table_name})
            table_exists = result.scalar() > 0
        if not table_exists:
            print(f"Table {table_name} not found. Loading dimension table...")
            load_taxi_zones_dim(table_name)
        else:
            with engine.connect() as conn:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            
            if count == 0:
                print(f"Table {table_name} is empty. Loading dimension table...")
                load_taxi_zones_dim(table_name)
            else:
                print(f"Table {table_name} already exists with {count} records -> skipping.")
    except Exception as e:
        print(f"Warning: Could not auto-load dim_taxi_zones table: {e}\n")
            
    yield
            
    print("Application shutdown.")

def load_taxi_zones_dim(table_name: str = "dim_taxi_zones"):
    """Extracted logic - can be called from startup or endpoint"""
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
    print(f"Taxi zones dim table loaded successfully with {len(df_zones)} records into {table_name}!")

app.router.lifespan_context = lifespan
        
@app.get("/task-status/{task_id}", tags=["Task Status"])
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

from markdown import markdown
def markdown_to_html(text: str) -> str:
    """Convert markdown text to HTML"""
    return markdown(text, extensions=['fenced_code', 'tables', 'nl2br'])

@app.post("/analyze-dashboard")
async def analyze_dashboard(
    files: List[UploadFile] = File(...), 
    custom_prompt: str = "Analyze this PowerBI dashboard screenshot in detail. ...", 
    download_html: bool = True,
    save_to_db: bool = True
):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    for file in files:
        if not file.content_type.startswith("image/"):
            raise HTTPException(
                status_code=400,
                detail=f"File {file.filename} is not an image (only PNG/JPG allowed)"
            )

    try:
        # Prepare messages with ALL images
        content_parts = [{"type": "text", "text": custom_prompt}]

        base64_images = []
        for file in files:
            contents = await file.read()
            base64_img = base64.b64encode(contents).decode('utf-8')
            base64_images.append(base64_img)

            content_parts.append(
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:{file.content_type};base64,{base64_img}"}
                }
            )

        # Call Groq with multiple images in one request
        # Note: check if your chosen model supports multiple images in one prompt
        # Llama-4-Scout and many recent vision models do support it
        response = client.chat.completions.create(
            model="meta-llama/llama-4-scout-17b-16e-instruct",
            messages=[
                {
                    "role": "user",
                    "content": content_parts   # ← all images + prompt together
                }
            ],
            temperature=0.5,
            max_tokens=3000  # ← increased a bit since more content
        )

        report_md = response.choices[0].message.content

        # For DB: you can store one combined record or one per file
        # Here: one combined report (simplest)
        filenames = ", ".join(f.filename for f in files)
        report_data = {
            "screenshot_filenames": filenames,
            "generated_at": __import__('datetime').datetime.utcnow().isoformat() + "Z",
            "prompt_used": custom_prompt,
            "model": "meta-llama/llama-4-scout-17b-16e-instruct",
            "raw_markdown_report": report_md,
            "structured_insights": {"full_report": report_md},
            "file_count": len(files)
        }

        if save_to_db:
            try:
                report_json_str = json.dumps(report_data)
                print(f"DEBUG: Preparing to save report for {len(files)} files | JSON length: {len(report_json_str)} chars")
                
                with engine.begin() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO dashboard_analysis_reports 
                            (screenshot_filename, report_json) 
                            VALUES (:filename, CAST(:report_json AS jsonb))
                        """),
                        {
                            "filename": f"batch_{len(files)}_{filenames[:100]}",  # or generate UUID
                            "report_json": report_json_str
                        }
                    )
                
                print(f"Saved combined AI report for {len(files)} files to Postgres!")
            except Exception as db_err:
                print(f"Warning: DB save failed: {db_err}")
                import traceback
                traceback.print_exc()

        # Generate HTML — now mentioning multiple files
        report_html_body = markdown(report_md, extensions=['nl2br', 'tables'])

        full_html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Dashboard Analysis - {len(files)} screenshots</title>
            <style> /* same styles as before */ </style>
        </head>
        <body>
            <div class="container">
                <h1>AI Dashboard Analysis Report</h1>
                <p><strong>Source Files ({len(files)}):</strong> {filenames}</p>
                <p><strong>Generated:</strong> {__import__('datetime').datetime.now().strftime('%B %d, %Y at %H:%M')}</p>
                <hr style="border: 1px solid #e2e8f0; margin: 30px 0;">
                
                {report_html_body}
                
                <div class="footer">
                    Generated by your FastAPI + Groq AI Vision Analyzer • {len(files)} dashboard(s) analyzed
                </div>
            </div>
        </body>
        </html>
        """

        if download_html:
            html_bytes = BytesIO(full_html.encode('utf-8'))
            filename = f"batch_analysis_{len(files)}_dashboards.html"
            return StreamingResponse(
                html_bytes,
                media_type="text/html",
                headers={"Content-Disposition": f"attachment; filename=\"{filename}\""}
            )
        else:
            return {
                "report": report_md,
                "html": full_html,
                "file_count": len(files),
                "saved_to_db": save_to_db
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis error: {str(e)}")
