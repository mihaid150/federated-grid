from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence

import requests

from shared.logging_config import logger


class DbRegistrationError(RuntimeError):
    """Raised when the DB manager call fails."""


@dataclass(frozen=True)
class FdbmClientConfig:
    base_url: str
    timeout: float = 4.0


class FdbmClient:
    """Thin HTTP client for the federated DB manager service."""

    def __init__(self, cfg: FdbmClientConfig):
        base_url = (cfg.base_url or "").strip()
        if not base_url:
            raise ValueError("FDBM base URL must be provided")
        object.__setattr__(self, "_base_url", base_url.rstrip("/"))
        object.__setattr__(self, "_timeout", float(cfg.timeout))

    @property
    def base_url(self) -> str:
        return getattr(self, "_base_url")

    @property
    def timeout(self) -> float:
        return getattr(self, "_timeout")

    def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            resp = requests.post(
                f"{self.base_url}{path}",
                json=payload,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:  # pragma: no cover - network failure handling
            logger.error("[Cloud][FDBM] request error posting simulation: %s", exc)
            raise DbRegistrationError(str(exc)) from exc

        if resp.status_code >= 400:
            msg = f"FDBM returned {resp.status_code}: {resp.text}"
            logger.error("[Cloud][FDBM] %s", msg)
            raise DbRegistrationError(msg)
        try:
            data = resp.json()
        except ValueError as exc:
            logger.error("[Cloud][FDBM] invalid JSON response: %s", exc)
            raise DbRegistrationError("invalid JSON response") from exc
        return data

    @staticmethod
    def _normalise_tags(tags: Optional[Iterable[str]]) -> Optional[list[str]]:
        if not tags:
            return None
        seen = set()
        cleaned: list[str] = []
        for tag in tags:
            value = str(tag).strip()
            if not value or value in seen:
                continue
            seen.add(value)
            cleaned.append(value)
        return cleaned or None

    @staticmethod
    def _maybe_int(value: Optional[Any]) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def create_simulation(
        self,
        name: str,
        requested_start: date,
        requested_end: date,
        tags: Optional[List[str]] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": name,
            "requested_start": requested_start.isoformat(),
            "requested_end": requested_end.isoformat(),
        }
        tag_list = self._normalise_tags(tags)
        if tag_list:
            payload["tags"] = tag_list
        if config:
            payload["config"] = config

        data = self._post("/simulations", payload)
        logger.info("[Cloud][FDBM] registered simulation id=%s", data.get("id"))
        return data

    def record_edge_training(
        self,
        edge_name: str,
        round_id: Optional[int] = None,
        fog_name: Optional[str] = None,
        simulation_id: Optional[str] = None,
        simulation_name: Optional[str] = None,
        simulation_date: Optional[date] = None,
        metrics: Optional[Dict[str, Any]] = None,
        tags: Optional[Sequence[str]] = None,
        extra: Optional[Dict[str, Any]] = None,
        round_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "edge_name": edge_name,
        }
        rid = self._maybe_int(round_id)
        if rid is not None:
            payload["round_id"] = rid
        if fog_name:
            payload["fog_name"] = fog_name
        if simulation_id:
            payload["simulation_id"] = simulation_id
        if simulation_name:
            payload["simulation_name"] = simulation_name
        if simulation_date:
            payload["simulation_date"] = simulation_date.isoformat()
        if metrics:
            payload["metrics"] = metrics
        if extra:
            payload["extra"] = extra
        if round_key:
            payload["round_key"] = round_key
        tag_list = self._normalise_tags(tags)
        if tag_list:
            payload["tags"] = tag_list
        data = self._post("/telemetry/edge-runs", payload)
        logger.info("[Cloud][FDBM] edge telemetry recorded id=%s", data.get("id"))
        return data

    def record_fog_aggregation(
        self,
        fog_name: str,
        round_id: Optional[int] = None,
        simulation_id: Optional[str] = None,
        simulation_name: Optional[str] = None,
        simulation_date: Optional[date] = None,
        metrics: Optional[Dict[str, Any]] = None,
        tags: Optional[Sequence[str]] = None,
        extra: Optional[Dict[str, Any]] = None,
        round_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "fog_name": fog_name,
        }
        rid = self._maybe_int(round_id)
        if rid is not None:
            payload["round_id"] = rid
        if simulation_id:
            payload["simulation_id"] = simulation_id
        if simulation_name:
            payload["simulation_name"] = simulation_name
        if simulation_date:
            payload["simulation_date"] = simulation_date.isoformat()
        if metrics:
            payload["metrics"] = metrics
        if extra:
            payload["extra"] = extra
        if round_key:
            payload["round_key"] = round_key
        tag_list = self._normalise_tags(tags)
        if tag_list:
            payload["tags"] = tag_list
        data = self._post("/telemetry/fog-aggregations", payload)
        logger.info("[Cloud][FDBM] fog telemetry recorded id=%s", data.get("id"))
        return data

    def record_cloud_aggregation(
        self,
        round_id: Optional[int] = None,
        mode: Optional[str] = None,
        selected_target: Optional[str] = None,
        targets: Optional[Sequence[str]] = None,
        reward: Optional[float] = None,
        simulation_id: Optional[str] = None,
        simulation_name: Optional[str] = None,
        simulation_date: Optional[date] = None,
        metrics: Optional[Dict[str, Any]] = None,
        tags: Optional[Sequence[str]] = None,
        extra: Optional[Dict[str, Any]] = None,
        round_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        rid = self._maybe_int(round_id)
        if rid is not None:
            payload["round_id"] = rid
        if mode:
            payload["mode"] = mode
        if selected_target:
            payload["selected_target"] = selected_target
        if targets:
            payload["targets"] = list(targets)
        if reward is not None:
            payload["reward"] = float(reward)
        if simulation_id:
            payload["simulation_id"] = simulation_id
        if simulation_name:
            payload["simulation_name"] = simulation_name
        if simulation_date:
            payload["simulation_date"] = simulation_date.isoformat()
        if metrics:
            payload["metrics"] = metrics
        if extra:
            payload["extra"] = extra
        if round_key:
            payload["round_key"] = round_key
        tag_list = self._normalise_tags(tags)
        if tag_list:
            payload["tags"] = tag_list
        data = self._post("/telemetry/cloud-aggregations", payload)
        logger.info("[Cloud][FDBM] cloud telemetry recorded id=%s", data.get("id"))
        return data
