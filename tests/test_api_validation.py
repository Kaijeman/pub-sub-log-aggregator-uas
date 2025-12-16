import httpx

def test_publish_reject_missing_field(base_url):
    bad = {"topic": "t", "event_id": "1", "timestamp": "2025-01-01T00:00:00Z", "payload": {}}
    r = httpx.post(f"{base_url}/publish", json=bad)
    assert r.status_code == 422

def test_publish_accept_single(base_url):
    e = {"topic":"t","event_id":"a","timestamp":"2025-01-01T00:00:00Z","source":"s","payload":{"x":1}}
    r = httpx.post(f"{base_url}/publish", json=e)
    assert r.status_code == 200
    assert r.json()["accepted"] == 1

def test_publish_accept_batch(base_url):
    batch = [
        {"topic":"t","event_id":"b","timestamp":"2025-01-01T00:00:00Z","source":"s","payload":{"x":1}},
        {"topic":"t","event_id":"c","timestamp":"2025-01-01T00:00:00Z","source":"s","payload":{"x":2}},
    ]
    r = httpx.post(f"{base_url}/publish", json=batch)
    assert r.status_code == 200
    assert r.json()["accepted"] == 2

def test_events_requires_topic(base_url):
    r = httpx.get(f"{base_url}/events")
    assert r.status_code in (422, 400)
