"""Tests for DNS resolution."""

from unittest.mock import MagicMock, patch

import dns.exception
import dns.resolver

from dns_collector.resolver import ResolveResult, resolve


def make_answer(records: list[tuple[str, int]]) -> MagicMock:
    """Build a mock dns.resolver.Answer with the given (value, ttl) pairs."""
    answer = MagicMock()
    answer.rrset.ttl = records[0][1]
    mocks = []
    for value, _ in records:
        m = MagicMock()
        m.to_text.return_value = value
        mocks.append(m)
    answer.__iter__ = MagicMock(return_value=iter(mocks))
    return answer


@patch("dns_collector.resolver.dns.resolver.Resolver")
def test_resolve_returns_value_and_ttl(mock_resolver_cls):
    """Successful resolution returns an ok ResolveResult."""
    instance = mock_resolver_cls.return_value
    instance.resolve.return_value = make_answer([("93.184.216.34", 3600)])
    result = resolve("example.com", "A")
    assert result == ResolveResult("ok", [("93.184.216.34", 3600)])


@patch("dns_collector.resolver.dns.resolver.Resolver")
def test_resolve_returns_multiple_records(mock_resolver_cls):
    """Multiple answers are all returned."""
    instance = mock_resolver_cls.return_value
    instance.resolve.return_value = make_answer(
        [("140.82.121.3", 60), ("140.82.121.4", 60)]
    )
    result = resolve("github.com", "A")
    assert result.status == "ok"
    assert result.records == [("140.82.121.3", 60), ("140.82.121.4", 60)]


@patch("dns_collector.resolver.dns.resolver.Resolver")
def test_resolve_no_answer_returns_noanswer(mock_resolver_cls):
    """NoAnswer (record type absent) returns noanswer status."""
    instance = mock_resolver_cls.return_value
    instance.resolve.side_effect = dns.resolver.NoAnswer
    result = resolve("example.com", "AAAA")
    assert result == ResolveResult("noanswer", [])


@patch("dns_collector.resolver.dns.resolver.Resolver")
def test_resolve_nxdomain_returns_nxdomain(mock_resolver_cls):
    """NXDOMAIN (domain does not exist) returns nxdomain status."""
    instance = mock_resolver_cls.return_value
    instance.resolve.side_effect = dns.resolver.NXDOMAIN
    result = resolve("doesnotexist.invalid", "A")
    assert result == ResolveResult("nxdomain", [])


@patch("dns_collector.resolver.dns.resolver.Resolver")
def test_resolve_timeout_returns_timeout(mock_resolver_cls):
    """Timeout returns timeout status."""
    instance = mock_resolver_cls.return_value
    instance.resolve.side_effect = dns.exception.Timeout
    result = resolve("example.com", "A")
    assert result == ResolveResult("timeout", [])


@patch("dns_collector.resolver.dns.resolver.Resolver")
def test_resolve_dns_exception_returns_error(mock_resolver_cls):
    """Generic DNSException returns error status."""
    instance = mock_resolver_cls.return_value
    instance.resolve.side_effect = dns.exception.DNSException("unexpected error")
    result = resolve("example.com", "A")
    assert result == ResolveResult("error", [])


@patch("dns_collector.resolver.dns.resolver.Resolver")
def test_resolve_sets_lifetime(mock_resolver_cls):
    """The lifetime is set on the resolver instance."""
    instance = mock_resolver_cls.return_value
    instance.resolve.return_value = make_answer([("1.2.3.4", 300)])
    resolve("example.com", "A", timeout=10.0)
    assert instance.lifetime == 10.0


@patch("dns_collector.resolver.dns.resolver.Resolver")
def test_resolve_sets_nameservers(mock_resolver_cls):
    """Configured nameservers are set on the resolver instance."""
    instance = mock_resolver_cls.return_value
    instance.resolve.return_value = make_answer([("1.2.3.4", 300)])
    resolve("example.com", "A", nameservers=["1.1.1.1", "8.8.8.8"])
    assert instance.nameservers == ["1.1.1.1", "8.8.8.8"]


@patch("dns_collector.resolver.dns.resolver.Resolver")
def test_resolve_without_nameservers_uses_default(mock_resolver_cls):
    """When no nameservers are provided the resolver default is kept."""
    instance = mock_resolver_cls.return_value
    instance.resolve.return_value = make_answer([("1.2.3.4", 300)])
    resolve("example.com", "A")
    # nameservers was not explicitly assigned, so it stays a MagicMock auto-attr
    assert isinstance(instance.nameservers, MagicMock)
