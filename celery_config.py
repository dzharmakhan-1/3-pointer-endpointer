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