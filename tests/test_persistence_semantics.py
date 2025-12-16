import time
import httpx

def test_idempotent_after_delay(base_url):
    topic = "persist"
    eid = "persist-1"
    e = {"topic":topic,"event_id":eid,"timestamp":"2025-01-01T00:00:00Z","source":"p","payload":{"x":1}}
    httpx.post(f"{base_url}/publish", json=e).raise_for_status()
    time.sleep(1.0)
    httpx.post(f"{base_url}/publish", json=e).raise_for_status()
    time.sleep(1.0)

    rows = httpx.get(f"{base_url}/events", params={"topic": topic}).json()
    assert [r["event_id"] for r in rows].count(eid) == 1

def test_stats_uptime_exists(base_url):
    s = httpx.get(f"{base_url}/stats").json()
    assert "uptime_sec" in s
    assert isinstance(s["uptime_sec"], int)
