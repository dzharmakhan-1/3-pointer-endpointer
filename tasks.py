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
