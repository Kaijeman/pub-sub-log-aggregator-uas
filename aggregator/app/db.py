import asyncpg
from typing import Optional

DDL = """
CREATE TABLE IF NOT EXISTS processed_events (
  id BIGSERIAL PRIMARY KEY,
  topic TEXT NOT NULL,
  event_id TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  payload JSONB NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (topic, event_id)
);

CREATE TABLE IF NOT EXISTS event_audit (
  id BIGSERIAL PRIMARY KEY,
  topic TEXT NOT NULL,
  event_id TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  status TEXT NOT NULL, -- received | processed | duplicate
  note TEXT
);

CREATE TABLE IF NOT EXISTS counters (
  id SMALLINT PRIMARY KEY DEFAULT 1,
  received BIGINT NOT NULL DEFAULT 0,
  unique_processed BIGINT NOT NULL DEFAULT 0,
  duplicate_dropped BIGINT NOT NULL DEFAULT 0
);

INSERT INTO counters (id) VALUES (1)
ON CONFLICT (id) DO NOTHING;
"""

class DB:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=10)

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    async def init_schema(self) -> None:
        assert self.pool
        async with self.pool.acquire() as conn:
            await conn.execute(DDL)
