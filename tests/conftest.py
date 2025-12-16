import os
import pytest

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8080")

@pytest.fixture(scope="session")
def base_url():
    return BASE_URL
