# fog/communication/fog_commands.py
from __future__ import annotations
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, Optional, Mapping
import time
from shared.commands_base import CommandRecordBase, EnvelopeBase, InvalidEnvelope

# Fog node (grid) accepts these from cloud
class FogNodeCommandCode(IntEnum):
    CREATE_LOCAL_MODEL    = 100       # legacy "0"
    START_FIRST_TRAINING  = 101       # legacy "1"
    CLOUD_MODEL_DISPATCH = 2001      # AMQP model downlink

FOG_NODE_CODEBOOK: Mapping[str, int] = {c.name: int(c.value) for c in FogNodeCommandCode}

@dataclass(frozen=True)
class FogNodeCommandRecord(CommandRecordBase):
    def validate(self) -> None:  # type: ignore[override]
        super().validate(FOG_NODE_CODEBOOK)

@dataclass(frozen=True)
class FogNodeEnvelope(EnvelopeBase):
    command: FogNodeCommandRecord = field(default_factory=lambda: FogNodeCommandRecord(cmd="CREATE_LOCAL_MODEL"))
    payload: Optional[Dict[str, Any]] = None

    def validate(self) -> None:
        self._require(self.type in ("command", "ack", "error"), f"bad type {self.type}")
        self._require(bool(self.origin), "origin required")
        self.command.validate()

    @classmethod
    def make(cls, cmd: str, *, origin: str, target: Optional[str] = None,
             round_id: Optional[int] = None, payload: Optional[Dict[str, Any]] = None,
             code: Optional[int] = None, qos: Optional[int] = 1) -> "FogNodeEnvelope":
        cmd_up = cmd.upper()
        rec = FogNodeCommandRecord(cmd=cmd_up, code=code or FOG_NODE_CODEBOOK.get(cmd_up))
        env = cls(origin=origin, target=target, round_id=round_id, command=rec, payload=payload, qos=qos)
        env.validate()
        return env

    @classmethod
    def parse_obj(cls, d: Dict[str, Any]) -> "FogNodeEnvelope":
        try:
            cmd_d = d["command"]
            rec = FogNodeCommandRecord(cmd=str(cmd_d["cmd"]).upper(), code=cmd_d.get("code"))
        except Exception as e:
            raise InvalidEnvelope(f"invalid command record {d.get('command')!r}: {e}")
        env = cls(
            v=int(d.get("v", 1)),
            type=str(d.get("type", "command")),
            id=str(d.get("id") or ""),
            ts=int(d.get("ts") or int(time.time())),
            origin=str(d.get("origin", "")),
            target=d.get("target"),
            round_id=d.get("round_id"),
            qos=d.get("qos", 1),
            command=rec,
            payload=d.get("payload"),
        )
        env.validate()
        return env
