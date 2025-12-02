import base64, hashlib, json, os, time, random, socket
import pika
from pika import exceptions as px
from shared.logging_config import logger
from shared.node_state import FederatedNodeState
from fog.communication.config import FogConfig
from fog.communication.state import FogRoundState
from fog.communication.fog_resources_paths import FogResourcesPaths

class UplinkWorker:
    def __init__(self, cfg: FogConfig, state: FogRoundState):
        self.cfg, self.state = cfg, state

    def enqueue_snapshot(self, model_path: str, model_bytes: bytes | None = None) -> None:
        outbox_dir = FogResourcesPaths.OUTBOX_FOLDER_PATH.value
        os.makedirs(outbox_dir, exist_ok=True)
        if model_bytes is None:
            with open(model_path, "rb") as f: model_bytes = f.read()
        model_hash = hashlib.sha256(model_bytes).hexdigest()
        meta_path = os.path.join(outbox_dir, f"{model_hash}.json")
        blob_path = os.path.join(outbox_dir, f"{model_hash}.keras")
        if os.path.exists(meta_path) and os.path.exists(blob_path):
            logger.info(f"[Fog]: model {model_hash} already queued; skip duplicate.")
            return
        with open(blob_path, "wb") as mf:
            mf.write(model_bytes); mf.flush(); os.fsync(mf.fileno())

        st = self.state
        if st.round_id is None:
            logger.info(f"[Fog]: no active round; refusing to enqueue model.")
            return

        node = FederatedNodeState.get_current_node()
        meta = {
            "fog_name": node.name,
            "fog_device_mac": node.device_mac,
            "model_file": blob_path,
            "hash": model_hash,
            "message_id": f"{node.device_mac}:{node.name}:{model_hash}",
            "round_id": st.round_id,
            "ts": int(time.time()),
        }
        tmp_meta = meta_path + ".tmp"
        with open(tmp_meta, "w") as jf:
            json.dump(meta, jf); jf.flush(); os.fsync(jf.fileno())
        os.replace(tmp_meta, meta_path)
        logger.info(f"[Fog]: queued model {model_hash} for cloud uplink.")

    def start(self):
        cfg, st = self.cfg, self.state
        delay, max_delay = 5, 60
        announced_down = False

        while True:
            if st.round_id is None or not st.outbox_enabled:
                time.sleep(2); continue

            outbox_dir = FogResourcesPaths.OUTBOX_FOLDER_PATH.value
            os.makedirs(outbox_dir, exist_ok=True)
            files = sorted(f for f in os.listdir(outbox_dir) if f.endswith(".json"))
            if not files:
                time.sleep(2); continue

            conn = None
            try:
                conn = pika.BlockingConnection(pika.ConnectionParameters(host=cfg.cloud_amqp_host, heartbeat=30, blocked_connection_timeout=60, connection_attempts=1, retry_delay=0))
                ch = conn.channel(); ch.confirm_delivery(); ch.queue_declare(queue="fog_to_cloud_models", durable=True)

                for fname in files:
                    meta_path = os.path.join(outbox_dir, fname)
                    try:
                        with open(meta_path) as f: meta = json.load(f)
                    except Exception as e:
                        logger.warning(f"[Fog]: bad outbox meta {meta_path}: {e}; removing.")
                        try: os.remove(meta_path)
                        except: pass
                        continue

                    if meta.get("round_id") != st.round_id:
                        logger.debug("[Fog]: skip %s (meta round %s != current %s)", meta_path, meta.get("round_id"), st.round_id)
                        continue

                    ts = meta.get("ts", 0)
                    blob_path = meta.get("model_file") or meta.get("model_path")
                    fog_name = meta.get("fog_name"); fog_mac = meta.get("fog_device_mac")
                    model_hash = meta.get("hash"); msg_id = meta.get("message_id")

                    # TTL prune
                    if cfg.outbox_ttl_secs > 0 and (time.time() - ts) > cfg.outbox_ttl_secs:
                        logger.info(f"[Fog]: pruning expired outbox item {meta_path}.")
                        for p in (meta_path, blob_path):
                            try: os.remove(p)
                            except: pass
                        continue

                    if not blob_path or not os.path.exists(blob_path):
                        logger.warning(f"[Fog]: missing blob for {meta_path}; dropping meta.")
                        try: os.remove(meta_path)
                        except: pass
                        continue

                    with open(blob_path, "rb") as mf: model_bytes = mf.read()
                    body = json.dumps({
                        "fog_name": fog_name,
                        "fog_device_mac": fog_mac,
                        "model": base64.b64encode(model_bytes).decode("utf-8"),
                        "hash": model_hash,
                        "round_id": st.round_id,
                    }).encode("utf-8")

                    props = pika.BasicProperties(delivery_mode=2, content_type="application/json", message_id=msg_id)

                    try:
                        ch.basic_publish(exchange="", routing_key="fog_to_cloud_models", body=body, properties=props, mandatory=True)
                        logger.info(f"[Fog]: publish confirmed by broker (msg_id={msg_id}, hash={model_hash}).")
                        for p in (meta_path, blob_path):
                            try: os.remove(p)
                            except: pass
                    except (px.UnroutableError, px.NackError, px.StreamLostError) as e:
                        logger.warning(f"[Fog]: publish failed ({e.__class__.__name__}). Will retry later.")

            except (px.NackError, px.UnroutableError, px.StreamLostError, socket.gaierror, pika.exceptions.AMQPError) as e:
                time.sleep(delay + random.uniform(0, 1.0)); delay = min(delay * 2, max_delay)
            except Exception:
                logger.exception(f"[Fog]: unexpected error in cloud uplink worker; retrying in {delay}...")
                time.sleep(delay); delay = min(delay * 2, max_delay)
            finally:
                try:
                    if conn and conn.is_open: conn.close()
                except Exception: pass