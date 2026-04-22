"""Configuration loading for DnsCollector."""

import tomllib
from pathlib import Path
from typing import NamedTuple

DEFAULT_CONFIG_PATH = Path("config.toml")


class DnsConfig(NamedTuple):
    """Holds validated DNS pipeline configuration."""

    domains: list[str]
    record_types: list[str]
    timeout: float
    nameservers: list[str]


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> DnsConfig:
    """Load and validate pipeline configuration from a TOML file.

    Args:
        config_path: Path to the TOML configuration file.

    Returns:
        A DnsConfig with domains and record types.

    Raises:
        FileNotFoundError: If the config file does not exist.
        KeyError: If required keys are missing from the config.
    """
    with config_path.open("rb") as f:
        raw = tomllib.load(f)

    return DnsConfig(
        domains=raw["dns"]["domains"]["targets"],
        record_types=raw["dns"]["record_types"],
        timeout=float(raw["dns"].get("timeout", 5.0)),
        nameservers=raw["dns"].get("nameservers", []),
    )
