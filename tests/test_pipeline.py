"""Tests for the pipeline orchestration."""

from pathlib import Path
from unittest.mock import patch

import pytest

from dns_collector.config import DnsConfig
from dns_collector.db import get_connection
from dns_collector.pipeline import _normalize, run
from dns_collector.resolver import ResolveResult


@pytest.fixture()
def conn(tmp_path: Path):
    """Provide an in-memory DuckDB connection with schema applied."""
    db = get_connection(tmp_path / "test.db")
    yield db
    db.close()


@pytest.fixture()
def config():
    """Minimal pipeline config for testing."""
    return DnsConfig(
        domains=["example.com"], record_types=["A"], timeout=5.0, nameservers=[]
    )


# --- _normalize ---


def test_normalize_lowercases():
    """Domain names are lowercased."""
    assert _normalize("Example.COM") == "example.com"


def test_normalize_strips_trailing_dot():
    """Trailing dots (absolute DNS names) are stripped."""
    assert _normalize("example.com.") == "example.com"


def test_normalize_strips_whitespace():
    """Leading/trailing whitespace is stripped."""
    assert _normalize("  example.com  ") == "example.com"


# --- run ---


@patch(
    "dns_collector.pipeline.resolve",
    return_value=ResolveResult("ok", [("93.184.216.34", 3600)]),
)
def test_run_inserts_domain(mock_resolve, conn, config):
    """A resolved domain is inserted into the domains table."""
    run(config, conn)
    result = conn.execute("SELECT name FROM domains").fetchall()
    assert result == [("example.com",)]


@patch(
    "dns_collector.pipeline.resolve",
    return_value=ResolveResult("ok", [("93.184.216.34", 3600)]),
)
def test_run_inserts_dns_record(mock_resolve, conn, config):
    """Resolved records are inserted into the dns_records table."""
    run(config, conn)
    row = conn.execute("SELECT record_type, value, ttl FROM dns_records").fetchone()
    assert row == ("A", "93.184.216.34", 3600)


@patch("dns_collector.pipeline.resolve", return_value=ResolveResult("noanswer", []))
def test_run_no_records_inserts_nothing_to_dns(mock_resolve, conn, config):
    """Empty resolve results produce no rows in the dns_records table."""
    run(config, conn)
    assert conn.execute("SELECT count(*) FROM dns_records").fetchone()[0] == 0


@patch(
    "dns_collector.pipeline.resolve",
    return_value=ResolveResult("ok", [("93.184.216.34", 3600)]),
)
def test_run_deduplicates_domains(mock_resolve, conn):
    """Duplicate domains in config are only inserted once."""
    config = DnsConfig(
        domains=["example.com", "EXAMPLE.COM", "example.com."],
        record_types=["A"],
        timeout=5.0,
        nameservers=[],
    )
    run(config, conn)
    count = conn.execute("SELECT count(*) FROM domains").fetchone()[0]
    assert count == 1


@patch(
    "dns_collector.pipeline.resolve",
    return_value=ResolveResult("ok", [("93.184.216.34", 3600)]),
)
def test_run_deduplicates_record_types(mock_resolve, conn):
    """Duplicate record types in config are only resolved once per domain."""
    config = DnsConfig(
        domains=["example.com"], record_types=["A", "A"], timeout=5.0, nameservers=[]
    )
    run(config, conn)
    assert mock_resolve.call_count == 1


@patch(
    "dns_collector.pipeline.resolve",
    return_value=ResolveResult("ok", [("93.184.216.34", 3600)]),
)
def test_run_second_run_appends(mock_resolve, conn, config):
    """Running the pipeline twice appends new records."""
    run(config, conn)
    run(config, conn)
    count = conn.execute("SELECT count(*) FROM dns_records").fetchone()[0]
    assert count == 2


@patch(
    "dns_collector.pipeline.resolve",
    return_value=ResolveResult("ok", [("1.2.3.4", 60), ("5.6.7.8", 60)]),
)
def test_run_inserts_multiple_records(mock_resolve, conn, config):
    """Multiple answers for one domain/type are all inserted."""
    run(config, conn)
    count = conn.execute("SELECT count(*) FROM dns_records").fetchone()[0]
    assert count == 2


@patch("dns_collector.pipeline.resolve", return_value=ResolveResult("timeout", []))
def test_run_error_inserts_nothing(mock_resolve, conn, config):
    """A resolver error produces no rows in the dns_records table."""
    run(config, conn)
    assert conn.execute("SELECT count(*) FROM dns_records").fetchone()[0] == 0


# --- run_id / batch ---


@patch(
    "dns_collector.pipeline.resolve",
    return_value=ResolveResult("ok", [("1.2.3.4", 300)]),
)
def test_run_creates_run_entry(mock_resolve, conn, config):
    """Each pipeline execution creates a row in the runs table."""
    run(config, conn)
    rows = conn.execute("SELECT id, finished_at IS NOT NULL FROM runs").fetchall()
    assert len(rows) == 1
    assert rows[0][1] is True  # finished_at is set


@patch(
    "dns_collector.pipeline.resolve",
    return_value=ResolveResult("ok", [("1.2.3.4", 300)]),
)
def test_run_assigns_run_id_to_dns(mock_resolve, conn, config):
    """DNS records are linked to the run that produced them."""
    run(config, conn)
    run_id = conn.execute("SELECT run_id FROM dns_records").fetchone()[0]
    assert (
        conn.execute("SELECT id FROM runs WHERE id = ?", [run_id]).fetchone()
        is not None
    )


# --- resolution_log ---


@patch(
    "dns_collector.pipeline.resolve",
    return_value=ResolveResult("ok", [("1.2.3.4", 300)]),
)
def test_run_logs_resolution_attempt(mock_resolve, conn, config):
    """Every resolution attempt is logged in resolution_log."""
    run(config, conn)
    row = conn.execute("SELECT record_type, status FROM resolution_log").fetchone()
    assert row == ("A", "ok")


@patch("dns_collector.pipeline.resolve", return_value=ResolveResult("timeout", []))
def test_run_logs_failed_resolution(mock_resolve, conn, config):
    """Failed resolution attempts are logged with their status."""
    run(config, conn)
    row = conn.execute("SELECT status FROM resolution_log").fetchone()
    assert row == ("timeout",)
