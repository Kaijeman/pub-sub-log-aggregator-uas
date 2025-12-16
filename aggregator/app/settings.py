import os

DATABASE_URL = os.environ["DATABASE_URL"]
REDIS_URL = os.environ["BROKER_URL"]
QUEUE_KEY = os.environ.get("QUEUE_KEY", "events_queue")
WORKERS = int(os.environ.get("WORKERS", "4"))
