import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import paho.mqtt.client as mqtt

from shared.logging_config import logger
from cloud.communication.config import CloudConfig
from cloud.communication.cloud_gateway import CloudGateway
from cloud.communication.db_manager_client import (
    DbRegistrationError,
    FdbmClient,
    FdbmClientConfig,
)


class CloudRoundScheduler:
    """Schedules daily rounds over a period and stops based on a metric threshold.

    Subscribes to cloud events to know when a round completes and to collect
    aggregated fog metrics (eval_before.r2) for threshold evaluation.
    """

    def __init__(self, cfg: CloudConfig, gateway: CloudGateway, state):
        self.cfg = cfg
        self.gateway = gateway
        self.state = state
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

        # MQTT client for event listening
        self._cli = mqtt.Client()
        self._cli.on_connect = self._on_connect
        self._cli.on_message = self._on_message
        self._connected = threading.Event()

        # Round tracking
        self._broadcast_rid: Optional[int] = None
        self._fog_eval_before_r2: Dict[int, List[float]] = {}  # rid -> list of fog-level r2
        self._last_registered_sim: Optional[Dict[str, Any]] = None

        self._db_client: Optional[FdbmClient] = None
        self._selection_objective: str = getattr(self.cfg, "cloud_selection_objective", "r2")
        self._subset_min: int = max(1, int(getattr(self.cfg, "cloud_subset_min", 1)))
        self._subset_max: int = max(self._subset_min, int(getattr(self.cfg, "cloud_subset_max", 2)))
        try:
            base_url = getattr(self.cfg, "fdbm_base_url", "")
            if base_url:
                cfg = FdbmClientConfig(
                    base_url=base_url,
                    timeout=getattr(self.cfg, "fdbm_timeout", 4.0),
                )
                self._db_client = FdbmClient(cfg)
                logger.info("[Cloud][Scheduler] FDBM client initialised (base_url=%s)", base_url)
        except Exception:
            logger.exception("[Cloud][Scheduler] failed to initialise FDBM client")
            self._db_client = None

    # ---- MQTT event bus ----
    def _on_connect(self, client, _userdata, _flags, rc):
        if rc == 0:
            try:
                client.subscribe("cloud/events/fog-model-received", qos=1)
                client.subscribe("cloud/events/cloud-model-broadcast", qos=1)
                logger.info("[Cloud][Scheduler] subscribed to fog-model-received + cloud-model-broadcast")
            except Exception as e:
                logger.warning("[Cloud][Scheduler] subscribe failed: %s", e)
            self._connected.set()
        else:
            logger.error("[Cloud][Scheduler] MQTT connect failed rc=%s", rc)

    def _on_message(self, _client, _userdata, msg):
        try:
            import json
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            return
        if msg.topic.endswith("fog-model-received"):
            rid = payload.get("round_id")
            metrics = payload.get("metrics") or {}
            if rid is None:
                return
            try:
                r2 = ((metrics.get("eval_before") or {}).get("r2")
                      or (metrics.get("before_training") or {}).get("r2"))
                if r2 is None:
                    return
                self._fog_eval_before_r2.setdefault(int(rid), []).append(float(r2))
            except Exception:
                return
        elif msg.topic.endswith("cloud-model-broadcast"):
            rid = payload.get("round_id")
            try:
                self._broadcast_rid = int(rid) if rid is not None else None
            except Exception:
                self._broadcast_rid = None

    def _ensure_bus(self):
        if not self._connected.is_set():
            try:
                self._cli.connect(self.cfg.cloud_mqtt_host, self.cfg.cloud_mqtt_port, keepalive=30)
                threading.Thread(target=self._cli.loop_forever, daemon=True).start()
            except Exception as e:
                logger.error("[Cloud][Scheduler] MQTT connect error: %s", e)

    # ---- public API ----
    def start(self, params: dict) -> Optional[Dict[str, Any]]:
        if self._thread and self._thread.is_alive():
            logger.info("[Cloud][Scheduler] already running; ignoring new request")
            return None
        self._stop.clear()
        self._last_registered_sim = None

        try:
            try:
                self._last_registered_sim = self._register_schedule(params)
            except Exception:
                logger.exception("[Cloud][Scheduler] unexpected error registering schedule with FDBM")
        except Exception:
            # defensive: never break scheduling due to registration errors
            pass
        # Reset round counter so a new schedule always starts from round_id=1
        try:
            self.state.persist(0, date=None)  # first scheduled day becomes round_id=1
            logger.info("[Cloud][Scheduler] reset round_id to 0 (new schedule will start from 1)")
        except Exception as e:
            logger.warning("[Cloud][Scheduler] failed to reset round counter: %s", e)
        self._thread = threading.Thread(target=self._run, args=(params,), daemon=True)
        self._thread.start()
        return self._last_registered_sim

    def stop(self) -> None:
        self._stop.set()

    # ---- runner ----
    def _run(self, params: dict) -> None:
        # Parse period and policy
        try:
            dt_start, dt_end = self._parse_period(params)
        except ValueError as e:
            logger.error("[Cloud][Scheduler] invalid period in params=%s: %s", params, e)
            return

        metric = str(params.get("metric", "r2")).lower()
        threshold = float(params.get("threshold", 0.0))
        hits_target = int(params.get("rounds-reached-threshold", 1))
        max_cycles = int(params.get("maximum-cycles", 1))
        cooldown = int(params.get("cooldown_seconds", 5))

        # Build list of dates inclusive
        days: List[str] = []
        cur = dt_start
        while cur <= dt_end:
            days.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)

        self._ensure_bus()
        if not self._connected.wait(timeout=10.0):
            logger.error("[Cloud][Scheduler] MQTT not connected; aborting schedule")
            return

        hits = 0
        cycle = 0
        round_id = self.state.round_id or 0
        # Keep a small history of round-level averages for visibility
        history: List[tuple[int, float]] = []
        logger.info("[Cloud][Scheduler] start period %s..%s days=%d metric=%s thr=%.4f hits=%d max_cycles=%d",
                    days[0], days[-1], len(days), metric, threshold, hits_target, max_cycles)

        while not self._stop.is_set() and cycle < max_cycles and hits < hits_target:
            cycle += 1
            logger.info("[Cloud][Scheduler] cycle %d/%d", cycle, max_cycles)
            for date_s in days:
                if self._stop.is_set():
                    break
                round_id = (round_id or 0) + 1
                # Start round: publish round-started via gateway
                payload = {
                    "date": date_s,
                    "round_id": round_id,
                    "objective": self._selection_objective,
                    "subset_min": self._subset_min,
                    "subset_max": self._subset_max,
                }
                logger.info("[Cloud][Scheduler] starting round_id=%s date=%s", round_id, date_s)
                try:
                    # Persist for cloud plane and emit event
                    self.gateway.notify_start_first_training(payload)
                except Exception as e:
                    logger.error("[Cloud][Scheduler] failed to start round %s: %s", round_id, e)
                    continue

                # Wait for cloud-model-broadcast for this round_id
                t0 = time.time()
                while not self._stop.is_set():
                    if self._broadcast_rid == round_id:
                        break
                    if time.time() - t0 > 900:  # 15 min safety
                        logger.warning("[Cloud][Scheduler] timeout waiting for cloud-model-broadcast rid=%s", round_id)
                        break
                    time.sleep(1.0)

                # Cooldown
                if cooldown > 0:
                    time.sleep(cooldown)

                # Evaluate threshold: average fog eval_before metric for this round
                vals = self._fog_eval_before_r2.pop(round_id, [])
                if vals:
                    avg = sum(vals) / len(vals)
                    logger.info("[Cloud][Scheduler] round_id=%s eval_before.%s=%.4f over %d fog(s)",
                                round_id, metric, avg, len(vals))
                    # Append to history and print the last few values to show evolution
                    try:
                        history.append((round_id, float(avg)))
                        tail = history[-5:]
                        tail_str = ", ".join(f"{rid}:{val:.4f}" for rid, val in tail)
                        logger.info("[Cloud][Scheduler] %s history (last %d): [%s]",
                                    metric, len(tail), tail_str)
                    except Exception:
                        pass
                    if metric == "r2" and avg >= threshold:
                        hits += 1
                        logger.info("[Cloud][Scheduler] threshold hit %d/%d (avg=%.4f >= %.4f)",
                                    hits, hits_target, avg, threshold)
                else:
                    logger.warning("[Cloud][Scheduler] no eval_before metrics for rid=%s", round_id)

                if hits >= hits_target:
                    break

        logger.info("[Cloud][Scheduler] completed: hits=%d/%d cycles=%d/%d", hits, hits_target, cycle, max_cycles)

    # ---- helpers ----
    def _parse_period(self, params: dict) -> Tuple[datetime, datetime]:
        start_raw = params.get("start-period") or params.get("start")
        end_raw = params.get("end-period") or params.get("end")
        if not start_raw or not end_raw:
            raise ValueError("missing start-period or end-period")
        try:
            dt_start = datetime.strptime(str(start_raw), "%Y-%m-%d")
            dt_end = datetime.strptime(str(end_raw), "%Y-%m-%d")
        except Exception as exc:  # pragma: no cover - defensive conversion
            raise ValueError(f"invalid date format: {exc}") from exc
        if dt_end < dt_start:
            raise ValueError("end date precedes start date")
        return dt_start, dt_end

    def _normalise_config(self, params: dict) -> Dict[str, Any]:
        def _coerce(value):
            if isinstance(value, (str, int, float, bool)) or value is None:
                return value
            if isinstance(value, dict):
                return {str(k): _coerce(v) for k, v in value.items()}
            if isinstance(value, (list, tuple, set)):
                return [_coerce(v) for v in value]
            return str(value)

        try:
            return _coerce(dict(params or {}))
        except Exception:
            return {}

    def _register_schedule(self, params: dict) -> Optional[Dict[str, Any]]:
        if not self._db_client:
            return None

        try:
            dt_start, dt_end = self._parse_period(params)
        except ValueError as exc:
            logger.warning("[Cloud][Scheduler] skip FDBM registration: %s", exc)
            return None

        name = (
            params.get("name")
            or params.get("simulation_name")
            or f"grid-schedule-{dt_start.date().isoformat()}-{dt_end.date().isoformat()}"
        )
        raw_tags = params.get("tags")
        tags: List[str] = []
        if isinstance(raw_tags, (list, tuple, set)):
            tags = [str(t).strip() for t in raw_tags if str(t).strip()]
        elif isinstance(raw_tags, str) and raw_tags.strip():
            tags = [raw_tags.strip()]
        if "grid-scheduler" not in tags:
            tags.append("grid-scheduler")
        tags = list(dict.fromkeys(tags))

        config = self._normalise_config(params)

        try:
            config.setdefault("objective", self._selection_objective)
            config.setdefault("subset_min", self._subset_min)
            config.setdefault("subset_max", self._subset_max)
            return self._db_client.create_simulation(
                str(name),
                dt_start.date(),
                dt_end.date(),
                tags=tags,
                config=config,
            )
        except DbRegistrationError:
            # already logged inside the client
            return None
