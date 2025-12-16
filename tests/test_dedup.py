import time
import httpx

def wait_processed(base_url, topic, min_len=1, timeout=5):
    t0 = time.time()
    while time.time() - t0 < timeout:
        rows = httpx.get(f"{base_url}/events", params={"topic": topic}).json()
        if len(rows) >= min_len:
            return rows
        time.sleep(0.2)
    return httpx.get(f"{base_url}/events", params={"topic": topic}).json()

def test_dedup_same_event_id_processed_once(base_url):
    topic = "dedup"
    eid = "same-1"
    e = {"topic":topic,"event_id":eid,"timestamp":"2025-01-01T00:00:00Z","source":"s","payload":{"x":1}}
    httpx.post(f"{base_url}/publish", json=e).raise_for_status()
    httpx.post(f"{base_url}/publish", json=e).raise_for_status()

    rows = wait_processed(base_url, topic, min_len=1)
    ids = [r["event_id"] for r in rows]
    assert ids.count(eid) == 1

def test_dedup_batch_with_duplicates(base_url):
    topic = "dedup2"
    eid = "same-2"
    batch = [
        {"topic":topic,"event_id":eid,"timestamp":"2025-01-01T00:00:00Z","source":"s","payload":{"x":1}},
        {"topic":topic,"event_id":eid,"timestamp":"2025-01-01T00:00:00Z","source":"s","payload":{"x":1}},
    ]
    httpx.post(f"{base_url}/publish", json=batch).raise_for_status()
    rows = wait_processed(base_url, topic, min_len=1)
    ids = [r["event_id"] for r in rows]
    assert ids.count(eid) == 1

def test_stats_increases_received(base_url):
    before = httpx.get(f"{base_url}/stats").json()
    e = {"topic":"st","event_id":"st-1","timestamp":"2025-01-01T00:00:00Z","source":"s","payload":{"x":1}}
    httpx.post(f"{base_url}/publish", json=e).raise_for_status()
    time.sleep(0.5)
    after = httpx.get(f"{base_url}/stats").json()
    assert after["received"] >= before["received"] + 1

def test_stats_duplicate_dropped_eventually_increases(base_url):
    before = httpx.get(f"{base_url}/stats").json()
    e = {"topic":"st2","event_id":"dup-1","timestamp":"2025-01-01T00:00:00Z","source":"s","payload":{"x":1}}
    httpx.post(f"{base_url}/publish", json=e).raise_for_status()
    httpx.post(f"{base_url}/publish", json=e).raise_for_status()
    time.sleep(0.8)
    after = httpx.get(f"{base_url}/stats").json()
    assert after["duplicate_dropped"] >= before["duplicate_dropped"] + 1
