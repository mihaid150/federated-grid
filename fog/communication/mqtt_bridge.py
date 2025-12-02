import json, threading, time, socket, os
import paho.mqtt.client as mqtt
from shared.logging_config import logger
from shared.node_state import FederatedNodeState
from fog.communication.config import FogConfig
from fog.communication.state import FogRoundState

class MqttBridge:
    def __init__(self, cfg: FogConfig, state: FogRoundState, event_bus):
        self.cfg, self.state, self.event_bus = cfg, state, event_bus
        self._clear_timers = {}
        # de-dup window for safety (ms cmd_id -> ts)
        self._recent_cmd_ids: dict[int, float] = {}
        self._recent_ttl = 15 * 60  # seconds

    def _schedule_clear(self, fog_client, topic: str, delay_s: int = 300):
        t = self._clear_timers.pop(topic, None)
        if t and t.is_alive():
            t.cancel()

        def _clear():
            try:
                fog_client.publish(topic, payload=b"", qos=1, retain=True)
                logger.info(f"[Fog]: cleared retained command on {topic}.")
            except Exception as e:
                logger.warning(f"[Fog]: failed to clear retained on {topic}: {e}")
            finally:
                self._clear_timers.pop(topic, None)

        timer = threading.Timer(delay_s, _clear)
        self._clear_timers[topic] = timer
        timer.start()

    def _clear_all_edge_retained(self, fog_client):
        if not self.cfg.purge_mqtt_retained_on_boot:
            return
        edges = getattr(FederatedNodeState.get_current_node(), "child_nodes", []) or []
        for edge in edges:
            topic = f"fog/{edge.name}/command"
            try:
                fog_client.publish(topic, payload=b"", qos=1, retain=True)
                logger.info(f"[Fog]: cleared retained command on {topic}")
            except Exception as e:
                logger.warning(f"[Fog]: failed clearing retained on {topic}: {e}")

    def _relay_to_all_edges(self, fog_client, payload: dict, retain: bool = False):
        """Fan out a JSON payload to all edges on fog/<edge>/command."""
        edges = getattr(FederatedNodeState.get_current_node(), "child_nodes", []) or []
        for edge in edges:
            topic = f"fog/{edge.name}/command"
            try:
                fog_client.publish(topic, json.dumps(payload), qos=1, retain=retain)
                logger.info(
                    f"[Fog]: (relay) {payload.get('cmd') or payload.get('command')}, "
                    f"round_id={payload.get('round_id')} cmd_id={payload.get('cmd_id')} -> {topic}"
                )
            except Exception as e:
                logger.warning(f"[Fog]: (relay) failed to publish to {topic}: {e}")

    def start(self):
        cfg, st = self.cfg, self.state
        fog_name = FederatedNodeState.get_current_node().name

        cloud_client = mqtt.Client(client_id=f"fog-cloud-{fog_name}", clean_session=cfg.mqtt_clean_session)
        fog_client = mqtt.Client(client_id=f"fog-local-{fog_name}", clean_session=cfg.mqtt_clean_session)

        cloud_client.will_set(f"fogs/{fog_name}/status", payload="offline", qos=1, retain=True)
        fog_client.will_set(f"fog/{fog_name}/local_status", payload="offline", qos=1, retain=True)
        cloud_client.reconnect_delay_set(min_delay=1, max_delay=60)
        fog_client.reconnect_delay_set(min_delay=1, max_delay=60)

        def _mark_online(client, topic):
            try:
                client.publish(topic, payload="online", qos=1, retain=True)
            except Exception as e:
                logger.warning(f"[Fog]: MQTT failed to publish online to {topic}: {e}")

        # ------------- CLOUD callbacks (legacy round control only) -------------
        def on_cloud_connect(c, _u, _f, rc):
            if rc == 0:
                _mark_online(c, f"fogs/{fog_name}/status")
                try:
                    c.subscribe("cloud/fog/command", qos=1)
                    logger.info("[Fog]: subscribed to cloud/fog/command on CLOUD broker.")
                except Exception as e:
                    logger.warning(f"[Fog]: subscribe failed on cloud broker: {e}")
            else:
                logger.error(f"[Fog]: (cloud client): connect failed, rc={rc}")

        def on_cloud_disconnect(_c, _u, rc):
            if rc != 0:
                logger.warning(f"[Fog]: disconnected from CLOUD MQTT (rc={rc}). Auto-reconnect.")

        def _dedup_cmd(cmd_id: int | None) -> bool:
            """Return True to skip if we have seen cmd_id recently."""
            now = time.time()
            # purge old
            for k, ts in list(self._recent_cmd_ids.items()):
                if now - ts > self._recent_ttl:
                    self._recent_cmd_ids.pop(k, None)
            if not cmd_id:
                return False
            if cmd_id in self._recent_cmd_ids:
                return True
            self._recent_cmd_ids[cmd_id] = now
            return False

        def on_cloud_message(_c, _u, msg):
            # Ignore retained clears
            if getattr(msg, "retain", False) and (not msg.payload or len(msg.payload) == 0):
                logger.info(f"[Fog]: ignoring retained-empty clear on {msg.topic}")
                return
            if not msg.payload:
                logger.info(f"[Fog]: ignoring empty MQTT payload on {msg.topic}")
                return
            try:
                payload = json.loads(msg.payload.decode())
            except Exception as e:
                logger.warning(f"[Fog]: bad MQTT payload from cloud: {e}")
                return

            command = payload.get("command")
            rid = payload.get("round_id")
            cmd_id = payload.get("cmd_id")

            if _dedup_cmd(cmd_id):
                logger.info(f"[Fog]: ignoring duplicated command {cmd_id} on {msg.topic}")
                return

            # Advance round if provided
            if rid is not None and rid != st.round_id:
                logger.info(f"[Fog]: new round_id={rid} (was {st.round_id}) on {msg.topic} → persist & purge outbox.")
                st.persist(rid)

            # === KEEP ONLY LEGACY NUMERIC COMMANDS FROM CLOUD ===
            if command in ('1', '2'):
                if not st.outbox_enabled:
                    logger.info(f"[Fog]: enabling outbox worker (cloud command={command}).")
                    st.enable_outbox(True)

                # Relay to edges; retain for '1'/'2'
                edges = getattr(FederatedNodeState.get_current_node(), "child_nodes", []) or []
                for edge in edges:
                    topic = f"fog/{edge.name}/command"
                    try:
                        fog_client.publish(topic, json.dumps(payload), qos=1, retain=True)
                        logger.info(f"[Fog]: (cloud→edges) cmd {cmd_id} → {topic}")
                        self._schedule_clear(fog_client, topic)  # schedule retained clear
                    except Exception as e:
                        logger.warning(f"[Fog]: failed to publish to {topic}: {e}")
                return

            # Everything else (policy/actions) now flows via fog-agent locally
            logger.info(f"[Fog]: ignoring cloud policy command '{command}' (handled by fog-agent locally).")

        cloud_client.on_connect = on_cloud_connect
        cloud_client.on_disconnect = on_cloud_disconnect
        cloud_client.on_message = on_cloud_message

        # ------------- LOCAL FOG callbacks (fog-agent → fog-node) -------------
        def on_fog_connect(c, _u, _f, rc):
            if rc == 0:
                _mark_online(c, f"fog/{fog_name}/local_status")
                try:
                    c.subscribe(cfg.topic_agent_commands, qos=1)
                    logger.info(f"[Fog]: subscribed to {cfg.topic_agent_commands} on LOCAL broker.")
                except Exception as e:
                    logger.warning(f"[Fog]: subscribe failed on LOCAL broker: {e}")
                self._clear_all_edge_retained(c)
            else:
                logger.error(f"[Fog]: (local client): connect failed, rc={rc}")

        def on_fog_disconnect(_c, _u, rc):
            if rc != 0:
                logger.warning(f"[Fog]: disconnected from LOCAL MQTT (rc={rc}). Auto-reconnect.")

        def on_fog_local_message(_c, _u, msg):
            try:
                payload = json.loads(msg.payload.decode())
            except Exception as e:
                logger.warning(f"[Fog]: bad MQTT payload on {msg.topic}: {e}")
                return

            cmd = str(payload.get("cmd", "")).upper()
            edges = getattr(FederatedNodeState.get_current_node(), "child_nodes", []) or []

            # THROTTLE (a.k.a. GLOBAL_THROTTLE from cloud-agent → mapped by fog-agent)
            if cmd in ("THROTTLE", "GLOBAL_THROTTLE"):
                for edge in edges:
                    topic = f"fog/{edge.name}/command"
                    try:
                        fog_client.publish(topic, json.dumps(payload), qos=1, retain=False)
                        logger.info(f"[Fog]: (agent→edge) THROTTLE → {topic}")
                    except Exception as e:
                        logger.warning(f"[Fog]: THROTTLE publish failed to {topic}: {e}")
                return

            # RETRAIN_FOG → turn into REQUEST_RETRAIN for all edges
            if cmd == "RETRAIN_FOG":
                retrain = {
                    "command": "REQUEST_RETRAIN",
                    "reason": payload.get("reason", "policy"),
                    "params": payload.get("params", {"window_days": 2}),
                    "ts": int(time.time()),
                }
                for edge in edges:
                    topic = f"fog/{edge.name}/command"
                    try:
                        fog_client.publish(topic, json.dumps(retrain), qos=1, retain=False)
                        logger.info(f"[Fog]: (agent→edge) RETRAIN → {topic}")
                    except Exception as e:
                        logger.warning(f"[Fog]: RETRAIN publish failed to {topic}: {e}")
                return

            # SUGGEST_MODEL → targeted OR broadcast (if no 'target')
            if cmd == "SUGGEST_MODEL":
                target = payload.get("target")
                if target:
                    topic = f"fog/{target}/command"
                    try:
                        fog_client.publish(topic, json.dumps(payload), qos=1, retain=False)
                        logger.info(f"[Fog]: (agent→edge) SUGGEST_MODEL → {topic}")
                    except Exception as e:
                        logger.warning(f"[Fog]: SUGGEST_MODEL publish failed to {topic}: {e}")
                else:
                    for edge in edges:
                        topic = f"fog/{edge.name}/command"
                        try:
                            fog_client.publish(topic, json.dumps(payload), qos=1, retain=False)
                            logger.info(f"[Fog]: (agent→edges) SUGGEST_MODEL → {topic}")
                        except Exception as e:
                            logger.warning(f"[Fog]: SUGGEST_MODEL publish failed to {topic}: {e}")
                return

            # SELECT_FOG (fog-scope toggle/activation; no edge fanout by default)
            if cmd == "SELECT_FOG":
                target = payload.get("target")
                # Only act if this fog is the target (or no target provided).
                if target and target != fog_name:
                    logger.info(f"[Fog]: SELECT_FOG target={target} != this fog={fog_name}; ignoring.")
                    return
                try:
                    setattr(st, "selected", True)  # best-effort flag
                except Exception:
                    pass
                if not st.outbox_enabled:
                    st.enable_outbox(True)
                    logger.info("[Fog]: enabling outbox due to SELECT_FOG (local).")
                logger.info(f"[Fog]: this fog '{fog_name}' selected by fog-agent; no edge fanout.")
                return

            logger.warning(f"[Fog]: unrecognized LOCAL cmd='{cmd}' payload={payload}")

        fog_client.on_connect = on_fog_connect
        fog_client.on_disconnect = on_fog_disconnect
        fog_client.on_message = on_fog_local_message

        # Start both clients
        cloud_client.loop_start()
        fog_client.loop_start()
        self._clear_all_edge_retained(fog_client)

        try:
            cloud_client.connect_async(cfg.cloud_mqtt_host, cfg.cloud_mqtt_port, keepalive=60)
        except Exception as e:
            logger.warning(f"[Fog]: connect_async CLOUD failed: {e}")
        try:
            fog_client.connect_async(cfg.fog_mqtt_host, cfg.fog_mqtt_port, keepalive=60)
        except Exception as e:
            logger.warning(f"[Fog]: connect_async LOCAL failed: {e}")

        def _watchdog():
            while True:
                try:
                    if not cloud_client.is_connected():
                        try:
                            socket.getaddrinfo(cfg.cloud_mqtt_host, cfg.cloud_mqtt_port, 0, socket.SOCK_STREAM)
                        except Exception as dns_e:
                            logger.debug(f"[Fog]: CLOUD DNS not resolvable yet: {dns_e}")
                        try:
                            cloud_client.connect_async(cfg.cloud_mqtt_host, cfg.cloud_mqtt_port, keepalive=60)
                        except Exception:
                            pass
                    if not fog_client.is_connected():
                        try:
                            fog_client.connect_async(cfg.fog_mqtt_host, cfg.fog_mqtt_port, keepalive=60)
                        except Exception:
                            pass
                except Exception:
                    logger.exception("[Fog]: MQTT watchdog exception (ignored).")
                time.sleep(int(os.getenv("FOG_MQTT_WATCHDOG_SECS", "10")))

        threading.Thread(target=_watchdog, daemon=True).start()
