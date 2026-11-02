"""PII masking before any SQL leaves the process.

Same rules, same order and same split as SlashPeak's scan (which masks inside
BigQuery with RE2): built-in email, phone and IP patterns, then the account's
own patterns from ``/v1/config``. Backtick-quoted names are left alone.

Python's ``re`` runs the patterns with ``re.ASCII``, because RE2's ``\\d``,
``\\b`` and ``\\s`` are ASCII-only. One known gap: Python's ASCII ``\\s`` also
matches a vertical tab, RE2's does not. The server masks again with RE2, so a
gap can only mask more, never send more.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Sequence, Tuple

#: The router's built-in rules (``spec/mask_vectors.json`` → ``builtinRules``; a test keeps them equal).
BUILTIN_RULES: Tuple[Tuple[str, str], ...] = (
    (r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b", "<email>"),
    (
        r"\+\d(?:[-.\s()]{0,2}\d){6,14}\b|(?:\b1[-.\s])?(?:\(\d{3}\)[-.\s]?|\b\d{3}[-.\s])\d{3}[-.\s]\d{4}\b",
        "<phone>",
    ),
    (r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b", "<ip>"),
    (
        r"\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b|\b(?:[0-9a-fA-F]{1,4}:){1,6}(?::[0-9a-fA-F]{1,4}){1,6}\b"
        r"|\b(?:[0-9a-fA-F]{1,4}:){1,7}:|::(?:[0-9a-fA-F]{1,4}:){0,6}[0-9a-fA-F]{1,4}\b",
        "<ip>",
    ),
)
CUSTOM_TOKEN = "<redacted>"
BUILTIN_REVISION = "builtin"

#: Same split as the scan's ``REGEXP_EXTRACT_ALL(text, r'`[^`]*`|[^`]+|`')``.
_SEGMENTS = re.compile(r"`[^`]*`|[^`]+|`")


class MaskError(ValueError):
    """A rule does not compile. The caller then sends nothing."""


class Masker:
    def __init__(self, rules: Iterable[Tuple[str, str]], revision: str) -> None:
        compiled: List[Tuple["re.Pattern[str]", str]] = []
        for pattern, token in rules:
            try:
                compiled.append((re.compile(pattern, re.ASCII), token))
            except (re.error, TypeError, ValueError) as error:
                raise MaskError("mask rule does not compile") from error
        self._rules = tuple(compiled)
        self.revision = revision

    def _mask_segment(self, text: str) -> str:
        for regex, token in self._rules:
            # A function replacement: the token is used as written, never read for backrefs.
            text = regex.sub(lambda _match, _token=token: _token, text)
        return text

    def mask(self, text: str) -> str:
        out = []
        for match in _SEGMENTS.finditer(text):
            segment = match.group(0)
            out.append(segment if len(segment) > 1 and segment.startswith("`") else self._mask_segment(segment))
        return "".join(out)


def builtin_masker(owner_patterns: Sequence[str] = ()) -> Masker:
    return Masker(list(BUILTIN_RULES) + [(p, CUSTOM_TOKEN) for p in owner_patterns], BUILTIN_REVISION)


def masker_from_config(mask: object) -> Optional[Masker]:
    """The masker for ``/v1/config``'s ``mask`` block; None when it is missing or malformed."""
    if not isinstance(mask, dict):
        return None
    rules = mask.get("rules")
    revision = mask.get("revision")
    if not isinstance(rules, list) or not isinstance(revision, str):
        return None
    pairs = []
    for rule in rules:
        if not isinstance(rule, dict) or not isinstance(rule.get("pattern"), str) or not isinstance(rule.get("token"), str):
            return None
        pairs.append((rule["pattern"], rule["token"]))
    return Masker(pairs, revision[:64])
