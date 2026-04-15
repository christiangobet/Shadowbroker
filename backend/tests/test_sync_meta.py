"""Tests for sync metadata registry integrity."""

from importlib.util import find_spec

from services.sync_meta import get_all_status


def test_all_registered_sync_modules_exist():
    statuses = get_all_status()

    assert statuses, "expected sync registry entries"
    for status in statuses:
        assert find_spec(status["module"]) is not None, f'missing module for {status["id"]}'
