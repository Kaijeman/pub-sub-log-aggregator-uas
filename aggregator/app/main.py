import json
import time
import asyncio
import logging
from typing import List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import redis.asyncio as redis

from .db import DB
from .models import Event, PublishBody
from .settings import DATABASE_URL, REDIS_URL, QUEUE_KEY, WORKERS
from .worker import worker_loop

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aggregator")

app = FastAPI(title="Distributed Pub-Sub Log Aggregator")

db = DB(DATABASE_URL)
redis_client: redis.Redis | None = None
start_time = time.time()
stop_event = asyncio.Event()
worker_tasks: List[asyncio.Task] = []

@app.on_event("startup")
async def startup():
    global redis_client
    await db.connect()
    await db.init_schema()
    redis_client = redis.from_url(REDIS_URL, decode_responses=False)

    for i in range(WORKERS):
        t = asyncio.create_task(worker_loop(f"w{i+1}", redis_client, db, stop_event))
        worker_tasks.append(t)

@app.on_event("shutdown")
async def shutdown():
    stop_event.set()
    for t in worker_tasks:
        t.cancel()
    await db.close()
    if redis_client:
        await redis_client.close()

@app.post("/publish")
async def publish(body: PublishBody):
    if redis_client is None:
        raise HTTPException(status_code=503, detail="broker not ready")

    events: List[Event]
    if isinstance(body, list):
        events = body
    else:
        events = [body]

    msgs = []
    for e in events:
        msgs.append(json.dumps({
            "topic": e.topic,
            "event_id": e.event_id,
            "timestamp": e.timestamp.isoformat(),
            "source": e.source,
            "payload": e.payload
        }))

    if len(msgs) == 1:
        await redis_client.rpush(QUEUE_KEY, msgs[0].encode("utf-8"))
    else:
        pipe = redis_client.pipeline()
        for m in msgs:
            pipe.rpush(QUEUE_KEY, m.encode("utf-8"))
        await pipe.execute()

    return JSONResponse({"accepted": len(msgs), "queue": QUEUE_KEY})

@app.get("/events")
async def get_events(topic: str = Query(min_length=1)):
    assert db.pool
    q = """
    SELECT topic, event_id, ts, source, payload, processed_at
    FROM processed_events
    WHERE topic = $1
    ORDER BY processed_at DESC
    LIMIT 1000;
    """
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(q, topic)
    return [dict(r) for r in rows]

@app.get("/stats")
async def stats():
    assert db.pool
    q = "SELECT received, unique_processed, duplicate_dropped FROM counters WHERE id = 1;"
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(q)
    uptime = int(time.time() - start_time)
    return {
        "received": int(row["received"]),
        "unique_processed": int(row["unique_processed"]),
        "duplicate_dropped": int(row["duplicate_dropped"]),
        "workers": WORKERS,
        "uptime_sec": uptime
    }
