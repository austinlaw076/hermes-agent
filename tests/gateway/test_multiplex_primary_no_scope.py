"""Regression test: primary/default config load with multiplex active but no secret scope.

When multiplex is active but the current config load is for the primary/default
profile (no secret scope installed), ``_apply_env_overrides()`` must NOT call
``get_secret()`` on the primary path.  The old code called ``get_secret()``
*before* checking ``is_secondary``, which raised ``UnscopedSecretError``.

This test reproduces the exact scenario:
  1. ``set_multiplex_active(True)`` — simulate multiplex being active
  2. No secret scope installed (primary/default profile path)
  3. Call ``_apply_env_overrides()`` — must complete without error
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from agent.secret_scope import (
    UnscopedSecretError,
    current_secret_scope,
    is_multiplex_active,
    set_multiplex_active,
    set_secret_scope,
)
from gateway.config import GatewayConfig, load_gateway_config


def _clear_secret_scope() -> None:
    """Ensure no secret scope is installed."""
    set_secret_scope(None)


def test_multiplex_active_no_secret_scope_primary_path() -> None:
    """Primary/default config load must not raise UnscopedSecretError
    when multiplex is active but no secret scope is installed."""
    # --- setup ---------------------------------------------------------------
    set_multiplex_active(True)
    _clear_secret_scope()  # ensure no scope is installed

    assert is_multiplex_active() is True
    assert current_secret_scope() is None

    config = GatewayConfig()

    # --- exercise -----------------------------------------------------------
    from gateway.config import _apply_env_overrides

    # This must NOT raise UnscopedSecretError
    _apply_env_overrides(config)

    # --- verify -------------------------------------------------------------
    # If we got here without exception, the fix works.
    assert isinstance(config, GatewayConfig)


def test_multiplex_active_no_secret_scope_load_gateway_config() -> None:
    """Full load_gateway_config path must not raise UnscopedSecretError
    when multiplex is active but no secret scope is installed."""
    # --- setup ---------------------------------------------------------------
    set_multiplex_active(True)
    _clear_secret_scope()

    assert is_multiplex_active() is True
    assert current_secret_scope() is None

    # Create a minimal config file so load_gateway_config can parse it.
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        config_file = tmp / "config.yaml"
        config_file.write_text(
            "bot_token: test:bot-token\n"
            "platforms: {}\n"
            "default_reset_policy:\n"
            "  at_hour: 4\n"
            "  idle_minutes: 1440\n"
        )

        # Point HERMES_HOME at the temp dir so the config loader finds our file.
        with patch.dict(os.environ, {"HERMES_HOME": str(tmp)}):
            import hermes_constants

            orig_home = hermes_constants.get_hermes_home
            try:
                hermes_constants.get_hermes_home = lambda: tmp  # type: ignore[method-assign]

                # This must NOT raise UnscopedSecretError
                result = load_gateway_config()

                assert result is not None
            finally:
                hermes_constants.get_hermes_home = orig_home


def test_multiplex_inactive_no_secret_scope_ok() -> None:
    """Sanity: when multiplex is NOT active, no scope is fine (legacy path)."""
    set_multiplex_active(False)
    _clear_secret_scope()

    config = GatewayConfig()
    from gateway.config import _apply_env_overrides

    _apply_env_overrides(config)
    assert isinstance(config, GatewayConfig)


def test_multiplex_active_with_secret_scope_secondary_path() -> None:
    """Sanity: when multiplex IS active AND a secret scope IS installed,
    the secondary path works (get_secret resolves from the scope)."""
    set_multiplex_active(True)
    _clear_secret_scope()

    # Install a minimal scope that returns a known value for a test key.
    class _TestScope(dict):
        def get(self, name: str) -> str | None:
            if name == "TEST_SECRET":
                return "from-scope"
            return None

    set_secret_scope(_TestScope())

    config = GatewayConfig()
    from gateway.config import _apply_env_overrides

    # This should work because the scope is installed.
    _apply_env_overrides(config)
    assert isinstance(config, GatewayConfig)

    _clear_secret_scope()
