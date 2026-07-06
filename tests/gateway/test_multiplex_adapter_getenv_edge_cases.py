"""Edge case tests for _resolve_adapter_for_source and _getenv exception handling.

These tests exercise the boundary conditions that the existing credential
isolation tests don't cover — missing attributes, empty dicts, fallback
paths, and the _getenv secondary-profile guard in gateway/config.py.
"""
import pytest
from unittest.mock import MagicMock
from pathlib import Path


# ---------------------------------------------------------------------------
# _resolve_adapter_for_source  —  gateway/run.py :: GatewayRunner
# ---------------------------------------------------------------------------

class FakeSource:
    """Minimal SessionSource stand-in for adapter resolution tests."""
    def __init__(self, platform="telegram", profile=None):
        self.platform = platform
        self.profile = profile


class TestResolveAdapterForSource:
    """Edge cases for GatewayRunner._resolve_adapter_for_source()."""

    @pytest.fixture
    def runner(self):
        """Build a minimal GatewayRunner-like object with adapters and _profile_adapters."""
        from gateway.run import GatewayRunner
        gw = MagicMock(spec=GatewayRunner)
        # Default adapter map
        gw.adapters = {"telegram": MagicMock(), "discord": MagicMock()}
        # Profile adapters — secondary profiles get their own
        gw._profile_adapters = {
            "shadow-reviewer": {"telegram": MagicMock()},
        }
        # Bind the real method
        import gateway.run as gwr
        gw._resolve_adapter_for_source = gwr.GatewayRunner._resolve_adapter_for_source.__get__(gw, GatewayRunner)
        return gw

    def test_no_profile_attribute(self, runner):
        """Source without a 'profile' attr falls back to self.adapters."""
        source = FakeSource(platform="telegram")
        del source.profile
        result = runner._resolve_adapter_for_source(source)
        assert result is runner.adapters["telegram"]

    def test_profile_none(self, runner):
        """Source.profile is None → fallback to self.adapters."""
        source = FakeSource(platform="discord", profile=None)
        result = runner._resolve_adapter_for_source(source)
        assert result is runner.adapters["discord"]

    def test_profile_not_in_adapters(self, runner):
        """Source.profile not in _profile_adapters → fallback to self.adapters."""
        source = FakeSource(platform="telegram", profile="nonexistent")
        result = runner._resolve_adapter_for_source(source)
        assert result is runner.adapters["telegram"]

    def test_profile_has_no_platform(self, runner):
        """Source.profile exists but has no entry for this platform → fallback."""
        source = FakeSource(platform="discord", profile="shadow-reviewer")
        result = runner._resolve_adapter_for_source(source)
        assert result is runner.adapters["discord"]

    def test_profile_has_platform(self, runner):
        """Source.profile matches and platform exists → profile adapter returned."""
        source = FakeSource(platform="telegram", profile="shadow-reviewer")
        result = runner._resolve_adapter_for_source(source)
        assert result is runner._profile_adapters["shadow-reviewer"]["telegram"]

    def test_empty_profile_adapters(self, runner):
        """_profile_adapters is empty dict → fallback to self.adapters."""
        runner._profile_adapters = {}
        source = FakeSource(platform="telegram", profile="shadow-reviewer")
        result = runner._resolve_adapter_for_source(source)
        assert result is runner.adapters["telegram"]

    def test_no_adapters_attribute(self, runner):
        """self.adapters missing (edge case) → AttributeError (code bug, not silent None)."""
        del runner.adapters
        source = FakeSource(platform="telegram")
        with pytest.raises(AttributeError):
            runner._resolve_adapter_for_source(source)

    def test_profile_adapter_is_none(self, runner):
        """Profile adapter entry is None → fallback to self.adapters."""
        runner._profile_adapters["shadow-reviewer"]["telegram"] = None
        source = FakeSource(platform="telegram", profile="shadow-reviewer")
        result = runner._resolve_adapter_for_source(source)
        assert result is runner.adapters["telegram"]


# ---------------------------------------------------------------------------
# _getenv  —  hermes_cli/runtime_provider.py
# ---------------------------------------------------------------------------

class TestGetenvEdgeCases:
    """Edge cases for _getenv in hermes_cli/runtime_provider.py.

    This _getenv routes through the secret scope (Workstream A). When
    multiplexing is active but no scope is set, it raises UnscopedSecretError
    to prevent credential leakage.
    """

    @pytest.fixture(autouse=True)
    def _reset_multiplex(self):
        from agent.secret_scope import set_multiplex_active, is_multiplex_active
        was_active = is_multiplex_active()
        set_multiplex_active(False)
        yield
        set_multiplex_active(was_active)

    def test_getenv_secret_found(self, monkeypatch):
        """Secret scope hit returns the scoped value."""
        from hermes_cli.runtime_provider import _getenv
        from agent.secret_scope import set_multiplex_active, set_secret_scope, reset_secret_scope

        set_multiplex_active(True)
        tok = set_secret_scope({"MY_SECRET": "scoped-value"})
        try:
            assert _getenv("MY_SECRET") == "scoped-value"
        finally:
            reset_secret_scope(tok)

    def test_getenv_unscoped_raises(self, monkeypatch):
        """Multiplex active, no scope → UnscopedSecretError (fail-closed)."""
        from hermes_cli.runtime_provider import _getenv
        from agent.secret_scope import set_multiplex_active, UnscopedSecretError

        monkeypatch.setenv("OPENAI_API_KEY", "sk-leak")
        set_multiplex_active(True)
        with pytest.raises(UnscopedSecretError):
            _getenv("OPENAI_API_KEY")

    def test_getenv_not_multiplex_reads_environ(self, monkeypatch):
        """Multiplex not active → reads environ normally."""
        from hermes_cli.runtime_provider import _getenv

        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-not-multiplex")
        result = _getenv("ANTHROPIC_API_KEY")
        assert result == "sk-not-multiplex"

    def test_getenv_secret_overrides_environ(self, monkeypatch):
        """Secret scope value takes priority over environ."""
        from hermes_cli.runtime_provider import _getenv
        from agent.secret_scope import set_multiplex_active, set_secret_scope, reset_secret_scope

        monkeypatch.setenv("OPENAI_API_KEY", "sk-env-leak")
        set_multiplex_active(True)

        tok = set_secret_scope({"OPENAI_API_KEY": "sk-scoped"})
        try:
            result = _getenv("OPENAI_API_KEY")
            assert result == "sk-scoped"
        finally:
            reset_secret_scope(tok)

    def test_getenv_missing_var_returns_default(self, monkeypatch):
        """Var not in environ and not in scope → returns default."""
        from hermes_cli.runtime_provider import _getenv

        result = _getenv("NONEXISTENT_VAR_12345", "fallback")
        assert result == "fallback"

    def test_getenv_missing_var_no_default(self, monkeypatch):
        """Var not in environ and not in scope, no default → returns ''."""
        from hermes_cli.runtime_provider import _getenv

        result = _getenv("NONEXISTENT_VAR_12345")
        assert result == ""

    def test_getenv_global_var_reads_environ(self, monkeypatch):
        """Global vars (HERMES_*) read from environ even under multiplex."""
        from hermes_cli.runtime_provider import _getenv
        from agent.secret_scope import set_multiplex_active

        monkeypatch.setenv("HERMES_MAX_ITERATIONS", "42")
        set_multiplex_active(True)
        result = _getenv("HERMES_MAX_ITERATIONS")
        assert result == "42"

    def test_getenv_global_var_no_scope_needed(self, monkeypatch):
        """Global vars (HERMES_MAX_ITERATIONS) don't need a secret scope."""
        from hermes_cli.runtime_provider import _getenv
        from agent.secret_scope import set_multiplex_active

        monkeypatch.setenv("HERMES_MAX_ITERATIONS", "99")
        set_multiplex_active(True)
        result = _getenv("HERMES_MAX_ITERATIONS")
        assert result == "99"
