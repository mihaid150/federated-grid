# cloud/communication/cloud_commands.py
from __future__ import annotations
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, Optional, Mapping
import time
from shared.commands_base import CommandRecordBase, EnvelopeBase, InvalidEnvelope

# Stable, cloud-only command codes
class CloudCommandCode(IntEnum):
    CREATE_LOCAL_MODEL     = 100    # previously "0"
    START_FIRST_TRAINING   = 101    # previously "1"
    SELECT_FOG             = 1001   # used by cloud-agent too
    GLOBAL_THROTTLE        = 1005
    CLOUD_MODEL_DISPATCH  = 2001   # for AMQP payloads to fogs (model downlink)

CLOUD_CODEBOOK: Mapping[str, int] = {c.name: int(c.value) for c in CloudCommandCode}

@dataclass(frozen=True)
class CloudCommandRecord(CommandRecordBase):
    def validate(self) -> None:  # type: ignore[override]
        super().validate(CLOUD_CODEBOOK)

@dataclass(frozen=True)
class CloudEnvelope(EnvelopeBase):
    command: CloudCommandRecord = field(default_factory=lambda: CloudCommandRecord(cmd="CREATE_LOCAL_MODEL"))
    payload: Optional[Dict[str, Any]] = None

    def validate(self) -> None:
        self._require(self.type in ("command", "ack", "error"), f"bad type {self.type}")
        self._require(bool(self.origin), "origin required")
        self.command.validate()

    # Factory
    @classmethod
    def make(
        cls,
        cmd: str,
        *,
        origin: str,
        target: Optional[str] = None,
        round_id: Optional[int] = None,
        date: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        code: Optional[int] = None,
        qos: Optional[int] = 1,
    ) -> "CloudEnvelope":
        cmd_up = cmd.upper()
        rec = CloudCommandRecord(cmd=cmd_up, code=code or CLOUD_CODEBOOK.get(cmd_up))
        env = cls(origin=origin, target=target, round_id=round_id, date=date, command=rec, payload=payload, qos=qos)
        env.validate()
        return env

    # Parser (if needed elsewhere)
    @classmethod
    def parse_obj(cls, d: Dict[str, Any]) -> "CloudEnvelope":
        try:
            cmd_d = d["command"]
            rec = CloudCommandRecord(cmd=str(cmd_d["cmd"]).upper(), code=cmd_d.get("code"))
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
            date=d.get("date"),
            qos=d.get("qos", 1),
            command=rec,
            payload=d.get("payload"),
        )
        env.validate()
        return env
