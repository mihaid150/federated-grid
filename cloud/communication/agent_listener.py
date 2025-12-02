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

        # Keep backwards compatibility but allow generic pass-through for any new commands
        AGENT_CMDS = {"PLAN_ROUND"}

        def _agent_topic(target: str | None) -> str:
            # targeted agent-plane topic; use 'all' for broadcast
            return f"cloud/agent/fog/{(target or 'all')}/commands"

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

            # update expected fogs and publish accordingly for PLAN_ROUND
            if cmd == "PLAN_ROUND":
                # control-only path: only set expectation, do not fan-out
                if bool(data.get("control_only")):
                    target = data.get("target")
                    targets = data.get("targets")
                    try:
                        if isinstance(targets, list) and targets:
                            self.state.set_expected_fogs([str(t) for t in targets])
                            self.state.set_downlink_fogs([str(t) for t in targets])
                        elif target and str(target).lower() != "all":
                            self.state.set_expected_fogs([str(target)])
                            self.state.set_downlink_fogs([str(target)])
                        else:
                            self.state.set_expected_fogs(None)
                            self.state.set_downlink_fogs(None)
                    except Exception:
                        pass
                    logger.info("[Cloud]: control-only PLAN_ROUND processed; expected_fogs=%s",
                                getattr(self.state, 'expected_fogs', None))
                    return
                target = data.get("target")
                targets = data.get("targets")
                # normalize
                if isinstance(targets, list) and targets:
                    try:
                        self.state.set_expected_fogs([str(t) for t in targets])
                        self.state.set_downlink_fogs([str(t) for t in targets])
                    except Exception:
                        pass
                    # fan-out to each target
                    if "ts" not in data:
                        data["ts"] = int(time.time())
                    for t in targets:
                        topic_out = _agent_topic(str(t))
                        self.pub.publish(topic_out, data, qos=1, retain=False)
                        logger.info(f"[Cloud]: agent->fog-agent publish {topic_out} -> {data}")
                    return
                elif target and str(target).lower() != "all":
                    try:
                        self.state.set_expected_fogs([str(target)])
                        self.state.set_downlink_fogs([str(target)])
                    except Exception:
                        pass
                    topic_out = _agent_topic(str(target))
                    if "ts" not in data:
                        data["ts"] = int(time.time())
                    self.pub.publish(topic_out, data, qos=1, retain=False)
                    logger.info(f"[Cloud]: agent->fog-agent publish {topic_out} -> {data}")
                    return
                else:
                    # explicit broadcast to all
                    try:
                        self.state.set_expected_fogs(None)
                        self.state.set_downlink_fogs(None)
                    except Exception:
                        pass
                    topic_out = _agent_topic("all")
                    if "ts" not in data:
                        data["ts"] = int(time.time())
                    self.pub.publish(topic_out, data, qos=1, retain=False)
                    logger.info(f"[Cloud]: agent->fog-agent publish {topic_out} -> {data}")
                    return

            # Generic pass-through for agent-plane commands to targeted/all
            target = data.get("target")
            topic_out = _agent_topic(target)
            # Stamp ts if missing
            if "ts" not in data:
                data["ts"] = int(time.time())
            self.pub.publish(topic_out, data, qos=1, retain=False)
            logger.info("[Cloud]: agent→fog-agent publish %s -> %s", topic_out, data)

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
