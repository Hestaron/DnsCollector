"""Tests for configuration loading."""

import pytest

from dns_collector.config import load_config

VALID_TOML = b"""
[dns]
record_types = ["A", "AAAA"]

[dns.domains]
targets = ["example.com", "github.com"]
"""


@pytest.fixture()
def valid_config_file(tmp_path):
    """Write a minimal valid config.toml to a temp directory."""
    path = tmp_path / "config.toml"
    path.write_bytes(VALID_TOML)
    return path


def test_load_config_domains(valid_config_file):
    """Domains are loaded correctly from config."""
    cfg = load_config(valid_config_file)
    assert cfg.domains == ["example.com", "github.com"]


def test_load_config_record_types(valid_config_file):
    """Record types are loaded correctly from config."""
    cfg = load_config(valid_config_file)
    assert cfg.record_types == ["A", "AAAA"]


def test_load_config_default_timeout(valid_config_file):
    """Timeout defaults to 5.0 when not specified in config."""
    cfg = load_config(valid_config_file)
    assert cfg.timeout == 5.0


def test_load_config_default_nameservers(valid_config_file):
    """Nameservers default to an empty list when not specified."""
    cfg = load_config(valid_config_file)
    assert cfg.nameservers == []


def test_load_config_nameservers(tmp_path):
    """Nameservers are loaded correctly from config."""
    path = tmp_path / "config.toml"
    path.write_bytes(
        b'[dns]\nrecord_types = ["A"]\nnameservers = ["1.1.1.1"]\n'
        b'[dns.domains]\ntargets = ["example.com"]\n'
    )
    cfg = load_config(path)
    assert cfg.nameservers == ["1.1.1.1"]


def test_load_config_file_not_found(tmp_path):
    """FileNotFoundError is raised for a missing config file."""
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "nonexistent.toml")


def test_load_config_missing_key(tmp_path):
    """KeyError is raised when a required key is absent."""
    path = tmp_path / "config.toml"
    path.write_bytes(b"[dns]\nrecord_types = []\n")  # missing [dns.domains]
    with pytest.raises(KeyError):
        load_config(path)


def test_load_real_config():
    """The project config.toml loads without error and contains expected values."""
    cfg = load_config()
    assert len(cfg.domains) > 0
    assert "A" in cfg.record_types
    assert all(isinstance(d, str) for d in cfg.domains)
    assert all(isinstance(r, str) for r in cfg.record_types)
    assert cfg.timeout > 0
    assert isinstance(cfg.nameservers, list)
