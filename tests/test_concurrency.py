import httpx
import concurrent.futures
import time

def send_same(base_url, topic, eid):
    e = {"topic":topic,"event_id":eid,"timestamp":"2025-01-01T00:00:00Z","source":"c","payload":{"x":1}}
    r = httpx.post(f"{base_url}/publish", json=e)
    r.raise_for_status()

def test_concurrent_publish_same_event_id(base_url):
    topic = "race"
    eid = "race-1"
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        futures = [ex.submit(send_same, base_url, topic, eid) for _ in range(50)]
        for f in futures:
            f.result()

    time.sleep(1.0)
    rows = httpx.get(f"{base_url}/events", params={"topic": topic}).json()
    ids = [r["event_id"] for r in rows]
    assert ids.count(eid) == 1

def test_concurrent_publish_many_unique(base_url):
    topic = "race2"
    def send(i):
        e = {"topic":topic,"event_id":f"u-{i}","timestamp":"2025-01-01T00:00:00Z","source":"c","payload":{"i":i}}
        httpx.post(f"{base_url}/publish", json=e).raise_for_status()

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
        for i in range(100):
            ex.submit(send, i)

    time.sleep(1.0)
    rows = httpx.get(f"{base_url}/events", params={"topic": topic}).json()
    assert len(set(r["event_id"] for r in rows)) >= 90

def test_stats_unique_not_exceed_received(base_url):
    s = httpx.get(f"{base_url}/stats").json()
    assert s["unique_processed"] <= s["received"]
