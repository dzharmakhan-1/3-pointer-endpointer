# **Automated Data Warehouse Pipeline for NYC Yellow Taxi Data with AI-Powered Dashboard Analysis**

A complete, containerized **end-to-end data warehousing solution** that:

- Ingests raw NYC Yellow Taxi `.parquet` files from AWS S3
- Performs automated ETL using **Celery + RabbitMQ**
- Stores cleaned data in **PostgreSQL**
- Enables interactive dashboard creation in **Power BI**
- Uses **Groq API** to automatically analyze dashboard screenshots and generate natural-language insights

All orchestrated through a modern **FastAPI** web interface.

## ✨ Key Features

- **File Upload & Triggered ETL** — Upload `.parquet` → automatic cleaning & loading to PostgreSQL
- **Asynchronous Task Processing** — Celery workers + RabbitMQ broker
- **Task Status Monitoring** — Real-time status (queued / pending / success / failed) in **Postman or Swagger**
- **AI-Powered Insights** — Upload Power BI screenshot → Groq generates intelligent report
- **Fully Dockerized** — Easy one-command local & production deployment
- **Production-ready structure** — Environment variables, logging, error handling

## 🛠️ Tech Stack

| Component              | Technology                  | Purpose                              |
|------------------------|-----------------------------|--------------------------------------|
| API / Web Interface    | FastAPI + Swagger UI        | File upload, task status, AI endpoint|
| Message Broker         | RabbitMQ                    | Task queuing                         |
| Task Queue / Workers   | Celery                      | Asynchronous ETL jobs                |
| Storage (raw)          | AWS S3                      | Backup & source parquet files        |
| Data Warehouse         | PostgreSQL 16+              | Structured & analysis-ready data     |
| Dashboarding           | Power BI                    | Visual analytics                     |
| AI Report Generation   | Groq API                    | Natural language dashboard insights  |
| Containerization       | Docker + Docker Compose     | Reproducible environment             |

## 📁 Project Structure

```
3-pointer-endpointer/
├── app.py                  # FastAPI application (main entry point)
├── tasks.py                # Celery tasks — ETL logic
├── celery_config.py        # Celery configuration & broker settings
├── dockerfile              # Python service build instructions
├── docker-compose.yml      # Orchestrates all services
├── requirements.txt        # Python dependencies
├── .env.example            # Template for environment variables
├── HOWITWORKS.md           # Detailed internal architecture explanation
├── screenshots/            # UI + workflow screenshots
└── README.md
```
## 🚀 Quick Start (Docker Recommended)

### Prerequisites

- Docker & Docker Compose
- AWS S3 bucket with NYC taxi `.parquet` files
- Groq API key
- PostgreSQL client (optional, for manual checks)

### 1. Clone & Prepare

```bash
git clone https://github.com/dzharmakhan-1/3-pointer-endpointer.git
cd 3-pointer-endpointer
```
### 2. Configure Environment
```bash
cp .env.example .env
```
### In .env file add:
### AWS
```
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_S3_BUCKET=your-nyc-taxi-bucket
AWS_REGION=us-east-1
```
### PostgreSQL
```
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=nyc_taxi_dw
POSTGRES_HOST=db
```
### RabbitMQ (usually no need to change)
```
RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672//
```
### Groq
```
GROQ_API_KEY=gsk_xxxxxxxxxxxxxxxxxxxxxxxxxxxx
```
### 3. Create the reports table (one-time)
```SQL
CREATE TABLE dashboard_analysis_reports (
    id SERIAL PRIMARY KEY,
    screenshot_filename VARCHAR(255) NOT NULL,
    report_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    note TEXT
);

CREATE INDEX idx_dashboard_reports_filename ON dashboard_analysis_reports(screenshot_filename);
CREATE INDEX idx_dashboard_reports_created_at ON dashboard_analysis_reports(created_at);
```
### 4. Start the docker
```bash
docker-compose up --build
```
### Services will be available at:

- FastAPI UI → http://localhost:8000 (Swagger docs at http://localhost:8000/docs)
- RabbitMQ Management → http://localhost:15672 (guest/guest)
- PostgreSQL → localhost:5432

## 📊 How It Works (Workflow)

- Upload NYC Yellow Taxi .parquet file via FastAPI /upload
- File is stored in S3
- RabbitMQ triggers Celery ETL task (tasks.py)
- Data is cleaned & loaded into PostgreSQL dimensional model
- Build dashboard in Power BI using PostgreSQL connection
- Take screenshot → upload via /analyze-dashboard
- Groq generates natural-language summary/report
- Report is saved as JSONB in dashboard_analysis_reports

→ Check results in PostgreSQL or (optional) download HTML version

## 📸 Screenshots
### Here are some examples of the UI and workflow:
![Swagger](./screenshots/1.png)
![S3](./screenshots/2.png)
![Task Status](./screenshots/3.png)
![PostgreSQL Table](./screenshots/4.png)
![PowerBI](./screenshots/7.png)
![PostgreSQL JSON Report](./screenshots/6.png)


