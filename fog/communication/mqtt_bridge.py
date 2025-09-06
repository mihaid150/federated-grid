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
            try: client.publish(topic, payload="online", qos=1, retain=True)
            except Exception as e:
                logger.warning(f"[Fog]: MQTT failed to publish online to {topic}: {e}")

        # ---- CLOUD callbacks ----
        def on_cloud_connect(c, _u, _f, rc):
            if rc == 0:
                _mark_online(c, f"fogs/{fog_name}/status")
                try:
                    c.subscribe("cloud/fog/command", qos=1)
                    logger.info(f"[Fog]: subscribed to cloud/fog/command on CLOUD broker.")
                except Exception as e:
                    logger.warning(f"[Fog]: subscribe failed on cloud broker: {e}")
            else:
                logger.error(f"[Fog]: (cloud client): connect failed, rc={rc}")

        def on_cloud_disconnect(_c, _u, rc):
            if rc != 0:
                logger.warning(f"[Fog]: disconnected from CLOUD MQTT (rc={rc}). Auto-reconnect.")

        def on_cloud_message(_c, _u, msg):
            if getattr(msg, "retain", False) and (not msg.payload or len(msg.payload) == 0):
                logger.info(f"[Fog]: ignoring retained-empty clear on {msg.topic}"); return
            if not msg.payload:
                logger.info(f"[Fog]: ignoring empty MQTT payload on {msg.topic}"); return
            try:
                payload = json.loads(msg.payload.decode())
            except Exception as e:
                logger.warning(f"[Fog]: bad MQTT payload from cloud: {e}"); return

            command = payload.get("command")
            rid = payload.get("round_id")
            cmd_id = payload.get("cmd_id")

            if rid is not None and rid != st.round_id:
                logger.info(f"[Fog]: new round_id={rid} (was {st.round_id}) → purge outbox.")
                st.persist(rid)
                # outbox content is pruned by worker while filtering on round_id

            if command in ('1', '2'):
                if not st.outbox_enabled:
                    logger.info(f"[Fog]: enabling outbox worker (command={command}).")
                st.enable_outbox(True)

            # Relay to edges with retain for '1' and '2'
            edges = getattr(FederatedNodeState.get_current_node(), "child_nodes", []) or []
            for edge in edges:
                topic = f"fog/{edge.name}/command"
                try:
                    fog_client.publish(topic, json.dumps(payload), qos=1, retain=command in ('1','2'))
                    logger.info(f"[Fog]: (relay) cmd {cmd_id} → {topic}")
                    if str(command) in ('1','2'):
                        self._schedule_clear(fog_client, topic)
                except Exception as e:
                    logger.warning(f"[Fog]: failed to publish to {topic}: {e}")

        cloud_client.on_connect = on_cloud_connect
        cloud_client.on_disconnect = on_cloud_disconnect
        cloud_client.on_message = on_cloud_message

        # ---- LOCAL FOG callbacks ----
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
                logger.error(f"[Fog] (local client): connect failed, rc={rc}")

        def on_fog_disconnect(_c, _u, rc):
            if rc != 0:
                logger.warning(f"[Fog]: disconnected from LOCAL MQTT (rc={rc}). Auto-reconnect.")

        def on_fog_local_message(_c, _u, msg):
            try:
                payload = json.loads(msg.payload.decode())
            except Exception as e:
                logger.warning(f"[Fog]: bad MQTT payload on {msg.topic}: {e}"); return
            cmd = payload.get("cmd")
            edges = getattr(FederatedNodeState.get_current_node(), "child_nodes", []) or []
            if cmd == "THROTTLE":
                for edge in edges:
                    topic = f"fog/{edge.name}/command"
                    fog_client.publish(topic, json.dumps(payload), qos=1, retain=False)
                    logger.info(f"[Fog]: (agent→edge) THROTTLE → {topic}")
            elif cmd == "RETRAIN_FOG":
                retrain = {
                    "command": "REQUEST_RETRAIN",
                    "reason": payload.get("reason", "policy"),
                    "params": payload.get("params", {"window_days": 2}),
                    "ts": int(time.time())
                }
                for edge in edges:
                    topic = f"fog/{edge.name}/command"
                    fog_client.publish(topic, json.dumps(retrain), qos=1, retain=False)
                    logger.info(f"[Fog]: (agent→edge) RETRAIN → {topic}")
            elif cmd == "SUGGEST_MODEL":
                target = payload.get("target")
                if target:
                    topic = f"fog/{target}/command"
                    fog_client.publish(topic, json.dumps(payload), qos=1, retain=False)
                    logger.info(f"[Fog]: (agent→edge) SUGGEST_MODEL → {topic}")

        fog_client.on_connect = on_fog_connect
        fog_client.on_disconnect = on_fog_disconnect
        fog_client.on_message = on_fog_local_message

        cloud_client.loop_start(); fog_client.loop_start()
        self._clear_all_edge_retained(fog_client)

        try: cloud_client.connect_async(cfg.cloud_mqtt_host, cfg.cloud_mqtt_port, keepalive=60)
        except Exception as e: logger.warning(f"[Fog]: connect_async CLOUD failed: {e}")
        try: fog_client.connect_async(cfg.fog_mqtt_host, cfg.fog_mqtt_port, keepalive=60)
        except Exception as e: logger.warning(f"[Fog]: connect_async LOCAL failed: {e}")

        def _watchdog():
            while True:
                try:
                    if not cloud_client.is_connected():
                        try: socket.getaddrinfo(cfg.cloud_mqtt_host, cfg.cloud_mqtt_port, 0, socket.SOCK_STREAM)
                        except Exception as dns_e:
                            logger.debug(f"[Fog]: CLOUD DNS not resolvable yet: {dns_e}")
                        try: cloud_client.connect_async(cfg.cloud_mqtt_host, cfg.cloud_mqtt_port, keepalive=60)
                        except Exception: pass
                    if not fog_client.is_connected():
                        try: fog_client.connect_async(cfg.fog_mqtt_host, cfg.fog_mqtt_port, keepalive=60)
                        except Exception: pass
                except Exception:
                    logger.exception(f"[Fog]: MQTT watchdog exception (ignored).")
                time.sleep(int(os.getenv("FOG_MQTT_WATCHDOG_SECS", "10")))

        threading.Thread(target=_watchdog, daemon=True).start()