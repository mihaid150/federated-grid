from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class Telemetry(BaseModel):
    ts: str
    value: float
    type: str = "measure"
    meta: Optional[Dict[str, Any]] = None

class NudgeCommand(BaseModel):
    command: str
    reason: str
    mae: float
    params: Dict[str, Any] = Field(default_factory=dict)

class ForecastRequest(BaseModel):
    horizon: int = 144
    exog: Optional[Dict[str, List[float]]] = None

class ForecastResponse(BaseModel):
    preds: List[float]
    std: Optional[List[float]] = None