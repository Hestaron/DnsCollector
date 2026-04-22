"""Pipeline orchestration for DnsCollector."""

import logging

import duckdb

from dns_collector.config import DnsConfig
from dns_collector.resolver import resolve

logger = logging.getLogger(__name__)


def _normalize(domain: str) -> str:
    """Lowercase and strip trailing dot from a domain name."""
    return domain.strip().lower().rstrip(".")


def _upsert_domain(conn: duckdb.DuckDBPyConnection, name: str) -> int:
    """Insert a domain if it does not exist and return its id."""
    conn.execute(
        "INSERT INTO domains (name) VALUES (?) ON CONFLICT (name) DO NOTHING", [name]
    )
    row = conn.execute("SELECT id FROM domains WHERE name = ?", [name]).fetchone()
    assert row is not None, f"domain '{name}' not found after upsert"
    return row[0]


def run(config: DnsConfig, conn: duckdb.DuckDBPyConnection) -> None:
    """Resolve DNS records for all configured domains and store them.

    Args:
        config: Pipeline configuration with domains and record types.
        conn: Open DuckDB connection with schema applied.
    """
    domains = sorted({_normalize(d) for d in config.domains})
    # Remove duplicate record types while preserving their original order
    record_types = list(dict.fromkeys(config.record_types))

    # Start a new pipeline run
    conn.execute("INSERT INTO runs DEFAULT VALUES")
    row = conn.execute("SELECT currval('runs_id_seq')").fetchone()
    assert row is not None
    run_id: int = row[0]

    logger.info(
        f"Run {run_id}: {len(domains)} domains × {len(record_types)} record types"
    )

    total_records = 0
    total_errors = 0

    for domain in domains:
        domain_id = _upsert_domain(conn, domain)

        for record_type in record_types:
            result = resolve(
                domain,
                record_type,
                timeout=config.timeout,
                nameservers=config.nameservers,
            )

            # Log every resolution attempt
            conn.execute(
                "INSERT INTO resolution_log (run_id, domain_id, record_type, status) "
                "VALUES (?, ?, ?, ?)",
                [run_id, domain_id, record_type, result.status],
            )

            if result.records:
                conn.executemany(
                    "INSERT INTO dns_records"
                    " (run_id, domain_id, record_type, value, ttl)"
                    " VALUES (?, ?, ?, ?, ?)",
                    [
                        (run_id, domain_id, record_type, value, ttl)
                        for value, ttl in result.records
                    ],
                )
                total_records += len(result.records)
                logger.info(
                    f"{domain:<30} {record_type:<6} → {len(result.records)} record(s)"
                )

            if result.status not in ("ok", "noanswer"):
                total_errors += 1

    # Mark run as finished
    conn.execute("UPDATE runs SET finished_at = now() WHERE id = ?", [run_id])
    logger.info(
        f"Run {run_id} complete: {total_records} records"
        f" inserted ({total_errors} errors)"
    )
