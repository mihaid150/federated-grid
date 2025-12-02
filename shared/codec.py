from __future__ import annotations
import json
from typing import Any, Dict, Type, Callable

# json helpers
def dumps(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def loads(buffer: bytes | str) -> Dict[str, Any]:
    if isinstance(buffer, bytes):
        return json.loads(buffer.decode("utf-8"))
    return json.loads(buffer)

# Legacy shim: map {"cmd": "..."} or {"command": "..."} → {"command": {"cmd": "..."}}
def coerce_legacy_into_command_record(d: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(d.get("command"), dict) and "cmd" in d["command"]:
        return d  # already new shape
    # Accept legacy cmd field or command as str/int
    legacy = d.get("cmd")
    if legacy is None:
        legacy = d.get("command")
    if legacy is None:
        return d
    if isinstance(legacy, int):
        # Leave numeric—node-specific codec can translate code->cmd if desired
        d["command"] = {"cmd": str(legacy), "code": legacy}
    elif isinstance(legacy, str):
        d["command"] = {"cmd": legacy.upper()}
    # hoist known context fields if scattered
    for k in ("origin", "target", "round_id"):
        d.setdefault(k, d.get(k))
    return d

# Pluggable decode with node-specific Envelope parser
def decode_envelope(
    buf: bytes | str,
    parser: Callable[[Dict[str, Any]], Any],
    legacy_coercion: bool = True
):
    data = loads(buf)
    if legacy_coercion:
        data = coerce_legacy_into_command_record(data)
    return parser(data)