import threading, os
from shared.logging_config import logger
from fog.communication.config import FogConfig
from fog.communication.state import FogRoundState
from fog.communication.events import FogEventBus
from fog.communication.mqtt_bridge import MqttBridge
from fog.communication.amqp_bridge import CloudToEdgesBridge
from fog.communication.edge_ingest import EdgeModelIngestor
from fog.communication.uplink_worker import UplinkWorker
from fog.model.model_aggregation_service import aggregate_models_with_metrics
from fog.communication.fog_resources_paths import FogResourcesPaths
from shared.node_state import FederatedNodeState

class FogCoordinator:
    def __init__(self, cfg: FogConfig | None = None):
        self.cfg = cfg or FogConfig()
        self.state = FogRoundState()
        if self.cfg.resume_outbox_on_boot:
            self.state.enable_outbox(True)
        self.events = FogEventBus(self.cfg.fog_mqtt_host, self.cfg.fog_mqtt_port)

        self.mqtt_bridge = MqttBridge(self.cfg, self.state, self.events)
        self.cloud_bridge = CloudToEdgesBridge(self.cfg, self.state, self.events)
        self.ingestor = EdgeModelIngestor(self.cfg, self.state, self.events)
        self.uplink = UplinkWorker(self.cfg, self.state)

    # === lifecycle ===
    def start_background_consumers(self):
        threading.Thread(target=self.mqtt_bridge.start, daemon=True).start()
        threading.Thread(target=self.cloud_bridge.start, daemon=True).start()
        threading.Thread(target=self.ingestor.start, args=(self._maybe_aggregate,), daemon=True).start()
        threading.Thread(target=self.uplink.start, daemon=True).start()
        logger.info(f"[Fog]: background consumers started.")

    # === callbacks ===
    def _maybe_aggregate(self, edge_models_cache: dict[str, dict]):
        needed = len(getattr(FederatedNodeState.get_current_node(), "child_nodes", []) or [])
        if len(edge_models_cache) == needed:
            logger.info(f"[Fog]: all edge models received; aggregating...")
            try:
                fog_weight = 1.0
                try:
                    fog_weight = float(self.state.fog_weight())
                except Exception:
                    fog_weight = 1.0
                aggregated = aggregate_models_with_metrics(edge_models_cache, fog_weight=fog_weight)
            except Exception as e:
                logger.exception(f"[Fog]: aggregation failed: {e}"); return
            if aggregated is None:
                logger.error(f"[Fog]: aggregation produced no model; skip uplink."); return

            self.events.publish(self.cfg.topic_aggregation_complete,
                                {"round_id": self.state.round_id,
                                 "model_path": FogResourcesPaths.FOG_MODEL_FILE_PATH.value,
                                 "ts": int(__import__('time').time())})

            # enqueue & delete temp
            model_path = FogResourcesPaths.FOG_MODEL_FILE_PATH.value
            with open(model_path, "rb") as f: model_bytes = f.read()

            # Compute lightweight aggregated metrics for cloud reward shaping
            try:
                maes = []
                base_maes = []
                spike_maes = []
                before_r2s = []
                before_maes = []
                after_r2s = []
                after_maes = []
                for entry in edge_models_cache.values():
                    m = entry.get("metrics", {}) or {}
                    # After-training
                    after = m.get("after_training") or m.get("after") or {}
                    mae_a = after.get("mae") or after.get("MAE")
                    r2_a = after.get("r2")
                    if mae_a is not None:
                        maes.append(float(mae_a))
                        after_maes.append(float(mae_a))
                    if r2_a is not None:
                        after_r2s.append(float(r2_a))
                    # Conditional (baseline/spike)
                    cond = m.get("conditional") if isinstance(m, dict) else None
                    if isinstance(cond, dict):
                        try:
                            b = cond.get("baseline", {}).get("mae")
                            s = cond.get("spike", {}).get("mae")
                            if b is not None:
                                base_maes.append(float(b))
                            if s is not None:
                                spike_maes.append(float(s))
                        except Exception:
                            pass
                    # Before-training
                    before = m.get("before_training") or {}
                    r2_b = before.get("r2")
                    mae_b = before.get("mae") or before.get("MAE")
                    if r2_b is not None:
                        try: before_r2s.append(float(r2_b))
                        except Exception: pass
                    if mae_b is not None:
                        try: before_maes.append(float(mae_b))
                        except Exception: pass

                agg_metrics = {}
                if maes:
                    agg_metrics["aggregated"] = {"mae": sum(maes) / len(maes)}
                if base_maes or spike_maes:
                    agg_metrics.setdefault("conditional", {})
                    if base_maes:
                        agg_metrics["conditional"]["baseline"] = {"mae": sum(base_maes) / len(base_maes)}
                    if spike_maes:
                        agg_metrics["conditional"]["spike"] = {"mae": sum(spike_maes) / len(spike_maes)}
                if before_r2s or before_maes:
                    agg_metrics["eval_before"] = {}
                    if before_r2s:
                        agg_metrics["eval_before"]["r2"] = sum(before_r2s) / len(before_r2s)
                    if before_maes:
                        agg_metrics["eval_before"]["mae"] = sum(before_maes) / len(before_maes)
                if after_r2s or after_maes:
                    agg_metrics["eval_after"] = {}
                    if after_r2s:
                        agg_metrics["eval_after"]["r2"] = sum(after_r2s) / len(after_r2s)
                    if after_maes:
                        agg_metrics["eval_after"]["mae"] = sum(after_maes) / len(after_maes)

                # ---- visibility: summarize what we computed (avoid large dumps) ----
                def _avg(xs):
                    try:
                        return (sum(xs) / len(xs)) if xs else None
                    except Exception:
                        return None
                summary = {
                    "n_edges": len(edge_models_cache),
                    "before_r2": {"count": len(before_r2s), "avg": _avg(before_r2s)},
                    "after_r2":  {"count": len(after_r2s),  "avg": _avg(after_r2s)},
                    "before_mae": {"count": len(before_maes), "avg": _avg(before_maes)},
                    "after_mae":  {"count": len(after_maes),  "avg": _avg(after_maes)},
                    "cond_baseline_mae": {"count": len(base_maes),  "avg": _avg(base_maes)},
                    "cond_spike_mae":    {"count": len(spike_maes), "avg": _avg(spike_maes)},
                }
                try:
                    logger.info("[Fog]: metrics aggregation summary (round_id=%s): %s", self.state.round_id, summary)
                    # Log the compact agg_metrics view (only top-level keys and inner eval keys)
                    compact = {
                        "keys": list(agg_metrics.keys()),
                        "eval_before": agg_metrics.get("eval_before"),
                        "eval_after": agg_metrics.get("eval_after"),
                        "aggregated": agg_metrics.get("aggregated"),
                        "conditional": agg_metrics.get("conditional"),
                    }
                    logger.info("[Fog]: agg_metrics (round_id=%s): %s", self.state.round_id, compact)
                except Exception:
                    pass
            except Exception:
                agg_metrics = {}
            self.uplink.enqueue_snapshot(model_path, model_bytes, metrics=agg_metrics)

            try: os.remove(model_path)
            except Exception:
                pass
            edge_models_cache.clear()
            logger.info(f"[Fog]: aggregated model queued for cloud uplink.")
