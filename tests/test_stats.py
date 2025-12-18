import time
import httpx


def wait_until(condition_fn, timeout=5.0, interval=0.2):
    start = time.time()
    while time.time() - start < timeout:
        if condition_fn():
            return True
        time.sleep(interval)
    return False


def test_stats_endpoint_exists(base_url):
    r = httpx.get(f"{base_url}/stats")
    assert r.status_code == 200

    data = r.json()
    assert "received" in data
    assert "unique_processed" in data
    assert "duplicate_dropped" in data
    assert "workers" in data
    assert "uptime_sec" in data

    assert isinstance(data["received"], int)
    assert isinstance(data["unique_processed"], int)
    assert isinstance(data["duplicate_dropped"], int)
    assert isinstance(data["workers"], int)
    assert isinstance(data["uptime_sec"], int)


def test_stats_received_increases_after_publish(base_url):
    before = httpx.get(f"{base_url}/stats").json()

    event = {
        "topic": "stats-test",
        "event_id": "stats-1",
        "timestamp": "2025-01-01T00:00:00Z",
        "source": "test",
        "payload": {"x": 1}
    }

    httpx.post(f"{base_url}/publish", json=event).raise_for_status()

    def received_increased():
        after = httpx.get(f"{base_url}/stats").json()
        return after["received"] >= before["received"] + 1

    assert wait_until(received_increased)


def test_stats_unique_processed_increases_for_new_event(base_url):
    before = httpx.get(f"{base_url}/stats").json()

    event = {
        "topic": "stats-test",
        "event_id": "unique-1",
        "timestamp": "2025-01-01T00:00:00Z",
        "source": "test",
        "payload": {"value": 123}
    }

    httpx.post(f"{base_url}/publish", json=event).raise_for_status()

    def unique_increased():
        after = httpx.get(f"{base_url}/stats").json()
        return after["unique_processed"] >= before["unique_processed"] + 1

    assert wait_until(unique_increased)


def test_stats_duplicate_dropped_increases_for_duplicate_event(base_url):
    before = httpx.get(f"{base_url}/stats").json()

    event = {
        "topic": "stats-test",
        "event_id": "dup-1",
        "timestamp": "2025-01-01T00:00:00Z",
        "source": "test",
        "payload": {"value": 999}
    }

    httpx.post(f"{base_url}/publish", json=event).raise_for_status()
    httpx.post(f"{base_url}/publish", json=event).raise_for_status()

    def duplicate_increased():
        after = httpx.get(f"{base_url}/stats").json()
        return after["duplicate_dropped"] >= before["duplicate_dropped"] + 1

    assert wait_until(duplicate_increased)


def test_stats_consistency_rules(base_url):
    stats = httpx.get(f"{base_url}/stats").json()

    assert stats["unique_processed"] <= stats["received"]
    assert stats["duplicate_dropped"] <= stats["received"]


def test_uptime_increases_over_time(base_url):
    s1 = httpx.get(f"{base_url}/stats").json()
    time.sleep(1.0)
    s2 = httpx.get(f"{base_url}/stats").json()

    assert s2["uptime_sec"] >= s1["uptime_sec"]
