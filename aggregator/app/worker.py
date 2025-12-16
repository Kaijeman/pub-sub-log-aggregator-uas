import json
import asyncio
import logging
from typing import Any, Dict
from datetime import datetime
import redis.asyncio as redis

from .db import DB
from .settings import QUEUE_KEY

logger = logging.getLogger("worker")

INSERT_PROCESSED = """
INSERT INTO processed_events (topic, event_id, ts, source, payload)
VALUES ($1, $2, $3, $4, $5::jsonb)
ON CONFLICT (topic, event_id) DO NOTHING
RETURNING id;
"""

AUDIT = """
INSERT INTO event_audit (topic, event_id, ts, source, status, note)
VALUES ($1, $2, $3, $4, $5, $6);
"""

INC_RECEIVED = "UPDATE counters SET received = received + $1 WHERE id = 1;"
INC_UNIQUE = "UPDATE counters SET unique_processed = unique_processed + $1 WHERE id = 1;"
INC_DUP = "UPDATE counters SET duplicate_dropped = duplicate_dropped + $1 WHERE id = 1;"

async def process_one(db: DB, payload: Dict[str, Any]) -> str:
    """
    Return: 'processed' | 'duplicate'
    """
    assert db.pool

    topic = payload["topic"]
    event_id = payload["event_id"]

    raw_ts = payload["timestamp"]
    ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))

    source = payload["source"]
    body = json.dumps(payload["payload"])  # <-- INI YANG HILANG

    async with db.pool.acquire() as conn:
        async with conn.transaction(isolation="read_committed"):
            await conn.execute(INC_RECEIVED, 1)
            await conn.execute(AUDIT, topic, event_id, ts, source, "received", None)

            row = await conn.fetchrow(INSERT_PROCESSED, topic, event_id, ts, source, body)
            if row is not None:
                await conn.execute(INC_UNIQUE, 1)
                await conn.execute(AUDIT, topic, event_id, ts, source, "processed", "inserted new")
                return "processed"

            await conn.execute(INC_DUP, 1)
            await conn.execute(AUDIT, topic, event_id, ts, source, "duplicate", "conflict on unique")
            return "duplicate"

async def worker_loop(name: str, r: redis.Redis, db: DB, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        item = await r.blpop(QUEUE_KEY, timeout=1)
        if not item:
            continue
        _, raw = item
        try:
            payload = json.loads(raw.decode("utf-8"))
            status = await process_one(db, payload)
            logger.info("worker=%s topic=%s event_id=%s status=%s", name, payload["topic"], payload["event_id"], status)
        except Exception:
            logger.exception("worker=%s failed to process message", name)
