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
        from shared.node_state import FederatedNodeState
        needed = len(getattr(FederatedNodeState.get_current_node(), "child_nodes", []) or [])
        if len(edge_models_cache) == needed:
            logger.info(f"[Fog]: all edge models received; aggregating...")
            try:
                aggregated = aggregate_models_with_metrics(edge_models_cache)
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
            self.uplink.enqueue_snapshot(model_path, model_bytes)
            try: os.remove(model_path)
            except Exception: pass
            edge_models_cache.clear()
            logger.info(f"[Fog]: aggregated model queued for cloud uplink.")