"""Validation queries for the DNS data model.

Each query checks a specific property of the ingested data.  Running them
after a pipeline execution gives confidence that the data model is correct,
complete, and useful for downstream analytics.
"""

import logging

import duckdb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Query definitions
# Each entry: (title, description, SQL)
# ---------------------------------------------------------------------------

QUERIES: list[tuple[str, str, str]] = [
    (
        "Record count by type",
        "Validates that we collected all six expected record types and shows "
        "the distribution. A missing type may indicate a resolver issue.",
        """
        SELECT record_type, count(*) AS total
        FROM dns_records
        GROUP BY record_type
        ORDER BY total DESC
        """,
    ),
    (
        "Records per domain",
        "Validates that every target domain produced at least one record. "
        "A domain with zero records signals an NXDOMAIN or network failure.",
        """
        SELECT domains.name, count(dns_records.id) AS record_count
        FROM domains
        LEFT JOIN dns_records ON dns_records.domain_id = domains.id
        GROUP BY domains.name
        ORDER BY record_count DESC
        """,
    ),
    (
        "Domains sharing nameservers",
        "Finds domains that share the same NS records — a useful clustering "
        "signal for ML models evaluating domain relationships.",
        """
        SELECT r.value AS nameserver, count(DISTINCT domains.name) AS domain_count,
               list(domains.name) AS domains
        FROM dns_records r
        JOIN domains ON domains.id = r.domain_id
        WHERE r.record_type = 'NS'
        GROUP BY r.value
        HAVING count(DISTINCT domains.name) > 1
        ORDER BY domain_count DESC
        """,
    ),
    (
        "IPv6 readiness (AAAA records)",
        "Lists which domains have AAAA records. Useful for infrastructure "
        "profiling — absence may indicate legacy hosting.",
        """
        SELECT domains.name,
               count(dns_records.id) AS aaaa_count
        FROM domains
        LEFT JOIN dns_records
            ON dns_records.domain_id = domains.id
            AND dns_records.record_type = 'AAAA'
        GROUP BY domains.name
        ORDER BY aaaa_count DESC
        """,
    ),
    (
        "Average TTL by domain",
        "Low average TTLs can indicate fast-flux DNS, a technique used by "
        "malicious actors to rapidly rotate infrastructure. Domains with "
        "unusually low TTLs deserve closer inspection.",
        """
        SELECT domains.name,
               round(avg(dns_records.ttl)) AS avg_ttl,
               min(dns_records.ttl) AS min_ttl,
               max(dns_records.ttl) AS max_ttl
        FROM domains
        JOIN dns_records ON dns_records.domain_id = domains.id
        GROUP BY domains.name
        ORDER BY avg_ttl ASC
        """,
    ),
    (
        "Resolution success rate",
        "Shows the outcome of each resolution attempt per domain. "
        "Statuses other than 'ok' and 'noanswer' indicate "
        "failures worth investigating.",
        """
        SELECT d.name,
               rl.status,
               count(*) AS attempts
        FROM resolution_log rl
        JOIN domains d ON d.id = rl.domain_id
        GROUP BY d.name, rl.status
        ORDER BY d.name, rl.status
        """,
    ),
]


def run_queries(conn: duckdb.DuckDBPyConnection) -> None:
    """Execute all validation queries and log their results.

    Args:
        conn: Open DuckDB connection with ingested data.
    """
    for title, description, sql in QUERIES:
        logger.info(f"── {title} ──")
        logger.info(f"   {description}")
        try:
            result = conn.execute(sql).fetchdf()
            if result.empty:
                logger.info("   (no rows returned)")
            else:
                for line in result.to_string(index=False).splitlines():
                    logger.info(f"   {line}")
        except Exception:
            logger.error("   Query failed", exc_info=True)
        logger.info("")


if __name__ == "__main__":
    from dns_collector.db import get_connection

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    conn = get_connection()
    try:
        # Run all validation queries and log their output
        # Note: pandas and numpy are big display dependencies, but used for simplicity.
        run_queries(conn)
    finally:
        conn.close()
