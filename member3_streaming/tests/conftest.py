import time
import pytest


@pytest.fixture(autouse=True)
def test_boundary_cooldown():
    yield
    time.sleep(3)
