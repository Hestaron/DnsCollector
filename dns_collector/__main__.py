"""Entry point for DnsCollector."""

import logging

from dns_collector.config import load_config
from dns_collector.db import get_connection
from dns_collector.pipeline import run

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def main() -> None:
    """Load config, open database, and run the ingestion pipeline."""
    config = load_config()
    conn = get_connection()
    try:
        run(config, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
