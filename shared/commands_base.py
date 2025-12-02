import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, Mapping


# ---------- Errors ----------
class CommandError(Exception): ...
class UnknownCommand(CommandError): ...
class InvalidEnvelope(CommandError): ...

# --------- Abstracts (node-agnostic) ----------
class Command(ABC):
    """Behavioral command"""
    @abstractmethod
    def execute(self, data: Dict[str, any]) -> Dict[str, Any]:
        raise NotImplementedError()

@dataclass(frozen=True)
class CommandRecordBase:
    """Node-specific command record must extend and guarantee cmd & code"""
    cmd: str  # UPPER_SNAKE_CASE
    code: Optional[int] = None

    def validate(self, codebook: Mapping[str, int]) -> None:
        if not self.cmd or self.cmd.upper() != self.cmd:
            raise InvalidEnvelope(f"cmd must be UPPER_SNAKE_CASE, got '{self.cmd!r}'")
        if self.cmd not in codebook:
            raise UnknownCommand(f"Unknown cmd '{self.cmd!r}'")
        if self.code is not None and self.code != codebook[self.cmd]:
            raise InvalidEnvelope(f"code {self.code} does not match cmd {self.cmd!r}")

@dataclass(frozen=True)
class EnvelopeBase:
    """Shared envelope shell. Each node implements a subclass and bind their CommandRecord"""
    v: int = 1
    type: str = "command"  #  "command" | "event" | "ack" | "error"
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    ts: int = field(default_factory=lambda: int(time.time()))
    origin: str = ""
    target: Optional[str] = None
    round_id: Optional[int] = None
    date: Optional[str] = None
    qos: Optional[int] = None
    # payload lives in subclasses for better typing

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def _require(cls, cond: bool, msg: str) -> None:
        if not cond:
            raise InvalidEnvelope(msg)
