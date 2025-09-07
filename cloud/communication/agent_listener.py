import json
import time
import paho.mqtt.client as mqtt
from shared.logging_config import logger

class AgentCommandListener:
    def __init__(self, cfg, state, pub):
        self.cfg, self.state, self.pub = cfg, state, pub

    def start(self):
        in_topic  = getattr(self.cfg, "topic_agent_commands", "cloud/agent/commands")
        out_topic = "cloud/fog/command"

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logger.info(
                    "[Cloud]: MQTT connected (agent-listener) to %s:%s (session_present=%s)",
                    self.cfg.cloud_mqtt_host, self.cfg.cloud_mqtt_port, getattr(flags, "session_present", False)
                )
                client.subscribe(in_topic, qos=1)
                logger.info("[Cloud]: agent-listener subscribed to %s (qos=1)", in_topic)
            else:
                logger.error("[Cloud]: MQTT connect failed (agent-listener) rc=%s", rc)

        def on_disconnect(_client, _userdata, rc):
            if rc != 0:
                logger.warning("[Cloud]: MQTT unexpected disconnect (agent-listener) rc=%s — will auto-reconnect", rc)
            else:
                logger.info("[Cloud]: MQTT clean disconnect (agent-listener)")

        def on_subscribe(_client, _userdata, mid, granted_qos):
            logger.info("[Cloud]: agent-listener on_subscribe mid=%s granted_qos=%s", mid, granted_qos)

        def on_message(_client, _userdata, msg):
            raw = msg.payload
            try:
                data = json.loads(raw.decode("utf-8"))
            except Exception as e:
                logger.warning("[Cloud]: agent-listener bad JSON on %s: %r (%s)", msg.topic, raw[:256], e)
                return

            cmd = str(data.get("cmd", "")).upper()
            logger.info("[Cloud]: agent-listener received cmd=%s payload=%s", cmd, data)

            if cmd == "GLOBAL_THROTTLE":
                try:
                    rate = float(data.get("rate", 0))
                except Exception:
                    logger.warning("[Cloud]: GLOBAL_THROTTLE missing/invalid 'rate' in %s", data)
                    return
                self.state.global_throttle = rate
                out = {"command": "GLOBAL_THROTTLE", "rate": rate, "cmd_id": int(time.time() * 1000)}
                self.pub.publish(out_topic, out, qos=1, retain=False)
                logger.info("[Cloud]: agent→fog publish %s -> %s", out_topic, out)

            elif cmd == "SELECT_FOG":
                target = data.get("target")
                if not target:
                    logger.warning("[Cloud]: SELECT_FOG missing 'target' in %s", data)
                    return
                self.state.selected_fog = target
                out = {"command": "SELECT_FOG", "target": target, "cmd_id": int(time.time() * 1000)}
                self.pub.publish(out_topic, out, qos=1, retain=False)
                logger.info("[Cloud]: agent→fog publish %s -> %s", out_topic, out)

            else:
                logger.warning("[Cloud]: agent-listener unknown cmd=%r payload=%s", cmd, data)

        client = mqtt.Client()  # keep default clean session; re-subscribes on reconnect below
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_subscribe = on_subscribe
        client.on_message = on_message
        client.reconnect_delay_set(min_delay=1, max_delay=60)

        logger.info("[Cloud]: agent-listener connecting to MQTT %s:%s ...",
                    self.cfg.cloud_mqtt_host, self.cfg.cloud_mqtt_port)
        client.connect(self.cfg.cloud_mqtt_host, self.cfg.cloud_mqtt_port)
        client.loop_forever()
