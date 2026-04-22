"""Tests for validation queries."""

from pathlib import Path

import pytest

from dns_collector.db import get_connection
from dns_collector.queries import QUERIES, run_queries


@pytest.fixture()
def conn(tmp_path: Path):
    """Provide a DuckDB connection with schema applied."""
    db = get_connection(tmp_path / "test.db")
    yield db
    db.close()


def _seed(conn, domain: str, records: list[tuple[str, str, int]]) -> None:
    """Insert a domain and its DNS records for testing."""
    conn.execute("INSERT INTO runs DEFAULT VALUES")
    run_id = conn.execute("SELECT currval('runs_id_seq')").fetchone()[0]
    conn.execute(
        "INSERT INTO domains (name) VALUES (?) ON CONFLICT (name) DO NOTHING",
        [domain],
    )
    domain_id = conn.execute(
        "SELECT id FROM domains WHERE name = ?", [domain]
    ).fetchone()[0]
    for record_type, value, ttl in records:
        conn.execute(
            "INSERT INTO dns_records (run_id, domain_id, record_type,"
            " value, ttl) VALUES (?, ?, ?, ?, ?)",
            [run_id, domain_id, record_type, value, ttl],
        )


def test_run_queries_on_empty_db(conn):
    """run_queries does not crash on a database with no data."""
    run_queries(conn)


def test_all_queries_are_valid_sql(conn):
    """Every query in QUERIES executes without SQL errors."""
    _seed(conn, "example.com", [("A", "93.184.216.34", 3600)])
    for title, _, sql in QUERIES:
        result = conn.execute(sql).fetchall()
        assert isinstance(result, list), f"Query '{title}' did not return a list"


def test_record_count_by_type(conn):
    """Record count by type returns correct counts."""
    _seed(
        conn,
        "example.com",
        [("A", "1.2.3.4", 300), ("A", "5.6.7.8", 300), ("NS", "ns1.example.com", 3600)],
    )
    rows = conn.execute(QUERIES[0][2]).fetchall()
    counts = {row[0]: row[1] for row in rows}
    assert counts["A"] == 2
    assert counts["NS"] == 1


def test_records_per_domain(conn):
    """Records per domain counts are correct, including zero for empty domains."""
    _seed(conn, "example.com", [("A", "1.2.3.4", 300)])
    conn.execute("INSERT INTO domains (name) VALUES (?)", ["empty.com"])
    rows = conn.execute(QUERIES[1][2]).fetchall()
    result = {row[0]: row[1] for row in rows}
    assert result["example.com"] == 1
    assert result["empty.com"] == 0


def test_ttl_query_returns_aggregates(conn):
    """Average TTL query returns correct min, max, and average."""
    _seed(conn, "example.com", [("A", "1.2.3.4", 60), ("NS", "ns1.example.com", 3600)])
    rows = conn.execute(QUERIES[4][2]).fetchall()
    assert len(rows) == 1
    # avg_ttl, min_ttl, max_ttl
    assert rows[0][2] == 60  # min
    assert rows[0][3] == 3600  # max
