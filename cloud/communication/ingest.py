import base64, hashlib, json, os, time
from shared.logging_config import logger
from shared.node_state import FederatedNodeState
from cloud.communication.config import CloudConfig
from cloud.communication.state import RoundState
from cloud.communication.cloud_resources_paths import CloudResourcesPaths
from cloud.communication.amqp import declare_durable_queue, AmqpClient
from cloud.communication.mqtt import MqttPublisher
from cloud.model.model_aggregation_service import aggregate_received_models

class Ingestor:
    def __init__(self, cfg: CloudConfig, state: RoundState, pub: MqttPublisher):
        self.cfg, self.state, self.pub = cfg, state, pub
        self.fog_models_cache: dict[str, dict] = {}
        self._recent: dict[str, dict] = {}  # key -> {hash, ts}

    def _ready_to_aggregate(self) -> bool:
        node = FederatedNodeState.get_current_node()
<<<<<<< HEAD
        expected = getattr(self.state, "expected_fogs", None)
        logger.info(f"[Cloud]: expected fogs: {expected}")
        if expected:
            got = set()
            logger.info(f"[Cloud]: fog_models_cache: {self.fog_models_cache.values()}")
            for entry in self.fog_models_cache.values():
                name = entry.get("fog_name")
                if name:
                    got.add(name)
            return set(expected).issubset(got)
        # default wait for all child fogs
=======
>>>>>>> d713743c2c6a65a787e35b4fec23833e426ee6af
        return bool(node) and len(self.fog_models_cache) == len(getattr(node, "child_nodes", []) or [])

    def start(self):
        os.makedirs(CloudResourcesPaths.MODELS_FOLDER_PATH.value, exist_ok=True)
        conn = AmqpClient(self.cfg.cloud_amqp_host).open_blocking()
        ch = conn.channel()
        ch.basic_qos(prefetch_count=1)
        res = declare_durable_queue(ch, "fog_to_cloud_models")
        logger.info(f"[Cloud]: queue fog_to_cloud_models → message_count={res.method.message_count}, "
                    f"consumers={res.method.consumer_count}")

        if self.cfg.dev_purge_on_boot:
            ch.queue_purge("fog_to_cloud_models")
            logger.info("[Cloud]: (DEV): purged fog_to_cloud_models on boot.")

        recent_ids: dict[str, float] = {}
        MSG_TTL = 900

        def purge_old_ids(now):
            for mid, ts in list(recent_ids.items()):
                if now - ts > MSG_TTL:
                    recent_ids.pop(mid, None)

        def on_msg(_ch, method, props, body):
            now = time.time(); purge_old_ids(now)

            mid = getattr(props, "message_id", None)
            if mid:
                if mid in recent_ids:
<<<<<<< HEAD
                    _ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
=======
                    _ch.basic_ack(delivery_tag=method.delivery_tag); return
>>>>>>> d713743c2c6a65a787e35b4fec23833e426ee6af
                recent_ids[mid] = now

            try:
                payload = json.loads(body)
            except Exception:
                logger.warning("[Cloud]: non-JSON AMQP payload; acking.")
<<<<<<< HEAD
                _ch.basic_ack(delivery_tag=method.delivery_tag)
                return
=======
                _ch.basic_ack(delivery_tag=method.delivery_tag); return
>>>>>>> d713743c2c6a65a787e35b4fec23833e426ee6af

            rid = payload.get("round_id")
            if self.state.round_id is None or rid != self.state.round_id:
                logger.warning(f"[Cloud]: round mismatch (cloud={self.state.round_id}, got={rid}); acking.")
<<<<<<< HEAD
                _ch.basic_ack(delivery_tag=method.delivery_tag)
                return
=======
                _ch.basic_ack(delivery_tag=method.delivery_tag); return
>>>>>>> d713743c2c6a65a787e35b4fec23833e426ee6af

            fog_mac = payload.get("fog_device_mac")
            fog_name = payload.get("fog_name")
            model_b64 = payload.get("model")
            model_hash = payload.get("hash")

            if not (fog_mac and fog_name and model_b64):
                logger.warning("[Cloud]: malformed fog message; acking.")
<<<<<<< HEAD
                _ch.basic_ack(delivery_tag=method.delivery_tag)
                return
=======
                _ch.basic_ack(delivery_tag=method.delivery_tag); return
>>>>>>> d713743c2c6a65a787e35b4fec23833e426ee6af

            key = f"{fog_mac}:{fog_name}"
            if not model_hash:
                try:
                    model_hash = hashlib.sha256(base64.b64decode(model_b64)).hexdigest()
                except Exception:
                    logger.warning("[Cloud]: invalid base64 model; acking.")
<<<<<<< HEAD
                    _ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

            prev = self._recent.get(key)
            if prev and prev["hash"] == model_hash and (now - prev["ts"] < 900):
                _ch.basic_ack(delivery_tag=method.delivery_tag)
                return
=======
                    _ch.basic_ack(delivery_tag=method.delivery_tag); return

            prev = self._recent.get(key)
            if prev and prev["hash"] == model_hash and (now - prev["ts"] < 900):
                _ch.basic_ack(delivery_tag=method.delivery_tag); return
>>>>>>> d713743c2c6a65a787e35b4fec23833e426ee6af

            try:
                model_bytes = base64.b64decode(model_b64)
                model_path = os.path.join(CloudResourcesPaths.MODELS_FOLDER_PATH.value,
                                          f"{fog_name}_aggregated_model.keras")
                with open(model_path, "wb") as f:
                    f.write(model_bytes); f.flush(); os.fsync(f.fileno())
<<<<<<< HEAD

                entry = {"model_path": model_path, "fog_name": fog_name}
                metrics = payload.get("metrics")
                if metrics:
                    entry["metrics"] = metrics
                self.fog_models_cache[f"{fog_mac}_{fog_name}"] = entry

                self._recent[key] = {"hash": model_hash, "ts": now}
                fog_evt = {"round_id": rid, "fog_name": fog_name, "hash": model_hash, "ts": int(time.time())}
                if metrics:
                    fog_evt["metrics"] = metrics
                self.pub.publish("cloud/events/fog-model-received", fog_evt, qos=1, retain=False)
                # self.pub.publish("cloud/events/fog-model-received",
                #                  {"round_id": rid, "fog_name": fog_name, "hash": model_hash,
                #                   "ts": int(time.time())}, qos=1, retain=False)
=======
                self.fog_models_cache[f"{fog_mac}_{fog_name}"] = {"model_path": model_path}
                self._recent[key] = {"hash": model_hash, "ts": now}
                self.pub.publish("cloud/events/fog-model-received",
                                 {"round_id": rid, "fog_name": fog_name, "hash": model_hash,
                                  "ts": int(time.time())}, qos=1, retain=False)
>>>>>>> d713743c2c6a65a787e35b4fec23833e426ee6af
                logger.info(f"[Cloud]: cached aggregated model from fog {fog_name} at {model_path}.")
            except Exception as e:
                logger.error(f"Cloud: failed to save model from fog {fog_name}: {e}")
            finally:
                _ch.basic_ack(delivery_tag=method.delivery_tag)


            if self._ready_to_aggregate():
<<<<<<< HEAD
                logger.info("[Cloud]: all required fog models received; aggregating...")
=======
                logger.info("[Cloud]: all fog models received; aggregating...")
>>>>>>> d713743c2c6a65a787e35b4fec23833e426ee6af
                aggregate_received_models(self.fog_models_cache)
                self.fog_models_cache.clear()
                logger.info("[Cloud]: cloud model aggregation complete.")

<<<<<<< HEAD
                # clear expected fogs for the next round
                try:
                    self.state.set_expected_fogs(None)
                except Exception:
                    pass

                try:
                    self.pub.publish("cloud/events/cloud-model-broadcast",
                                     {"round_id": self.state.round_id, "ts": int(time.time())},
                                     qos=1, retain=False)
                except Exception as e:
                    logger.warning("Cloud: failed to publish cloud-model-broadcast: %s", e)

=======
>>>>>>> d713743c2c6a65a787e35b4fec23833e426ee6af
        ch.basic_consume(queue="fog_to_cloud_models", on_message_callback=on_msg, auto_ack=False)
        logger.info("[Cloud]: listening for aggregated models from fogs...")
        ch.start_consuming()
