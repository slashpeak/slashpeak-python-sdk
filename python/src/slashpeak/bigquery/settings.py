"""SDK settings, read once from the environment (or passed to ``install``)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping, Optional
from urllib.parse import urlsplit

MODES = ("off", "shadow", "active")
DEFAULT_ENDPOINT = "https://router.slashpeak.com"
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


@dataclass(frozen=True)
class Settings:
    api_key: Optional[str]
    endpoint: str = DEFAULT_ENDPOINT
    #: The SDK's own ceiling. The stricter of this and the server's mode wins.
    mode: str = "active"
    #: Send query parameter values (masked first). Off if the SDK or the server says off.
    send_param_values: bool = True

    @property
    def usable(self) -> bool:
        """A key, a mode other than off, and an endpoint the key may be sent to."""
        return bool(self.api_key) and self.mode != "off" and endpoint_is_safe(self.endpoint)


def endpoint_is_safe(endpoint: str) -> bool:
    """HTTPS only. Plain HTTP is allowed for a local test server, never for a remote host."""
    try:
        parts = urlsplit(endpoint)
    except ValueError:
        return False
    if parts.scheme == "https" and parts.hostname:
        return True
    return parts.scheme == "http" and (parts.hostname or "") in _LOCAL_HOSTS


def _flag(value: Optional[str], default: bool) -> bool:
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() not in ("0", "false", "off", "no")


def load_settings(env: Mapping[str, str] = os.environ) -> Settings:
    mode = (env.get("SLASHPEAK_MODE") or "active").strip().lower()
    return Settings(
        api_key=(env.get("SLASHPEAK_API_KEY") or "").strip() or None,
        endpoint=(env.get("SLASHPEAK_ENDPOINT") or DEFAULT_ENDPOINT).strip().rstrip("/"),
        # An unknown mode is a typo: do nothing rather than guess.
        mode=mode if mode in MODES else "off",
        send_param_values=_flag(env.get("SLASHPEAK_SEND_PARAM_VALUES"), True),
    )


def stricter_mode(a: Optional[str], b: Optional[str]) -> str:
    """off < shadow < active; an unknown value counts as off."""
    rank = {mode: i for i, mode in enumerate(MODES)}
    return MODES[min(rank.get(a or "off", 0), rank.get(b or "off", 0))]
