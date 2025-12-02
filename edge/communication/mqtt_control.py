import json, time
import paho.mqtt.client as mqtt
from shared.logging_config import logger
from shared.node_state import FederatedNodeState
from edge.communication.config import EdgeConfig
from edge.communication.state import EdgeRuntimeState


class MqttControl:
    def __init__(self, cfg: EdgeConfig, state: EdgeRuntimeState, edge_service):
        self.cfg, self.state, self.edge_service = cfg, state, edge_service


    def start(self):
        edge_name = FederatedNodeState.get_current_node().name
        fog_topic = f"fog/{edge_name}/command"
        agent_cmd_topic = f"agent/{edge_name}/commands"


        c = mqtt.Client(client_id=f"edge-{edge_name}", clean_session=self.cfg.mqtt_clean_session)
        c.reconnect_delay_set(min_delay=self.cfg.mqtt_min_delay, max_delay=self.cfg.mqtt_max_delay)


        def on_connect(client, userdata, flags, rc):
            sess = flags.get('session present', flags.get('session_present', 0))
            logger.info(f"[Edge] {edge_name}: MQTT connected rc={rc}, session_present={sess}")
            client.subscribe([(fog_topic, 1), (agent_cmd_topic, 1)])
            logger.info(f"[Edge] {edge_name}: (re)subscribed to {fog_topic} and {agent_cmd_topic}")


        def on_disconnect(client, userdata, rc):
            logger.warning(f"[Edge] {edge_name}: MQTT disconnected rc={rc}; auto-reconnect...")


        def _handle_fog_command(payload: dict):
            cmd_id = payload.get("cmd_id")
            if cmd_id is not None and cmd_id == self.state.last_cmd_id:
                logger.info(f"[Edge]: duplicate cmd_id {cmd_id} ignored.")
                return
            self.state.last_cmd_id = cmd_id


            cmd = str(payload.get('command'))
            logger.info(f"[Edge] {edge_name}: received FOG MQTT command: {cmd}")
            if cmd == '0':
                self.edge_service.create_local_edge_model()
            elif cmd == '1':
                self.edge_service.train_edge_local_model(payload)
            else:
                logger.debug(f"[Edge] {edge_name}: ignoring MQTT command={cmd}")


        def _handle_agent_command(payload: dict):
            try:
                self.edge_service.handle_agent_nudge(payload)
            except Exception:
                logger.exception(f"[Edge]: failed handling agent nudge")


        def on_message(_client, _userdata, msg):
            if not msg.payload:
                return
            try:
                payload = json.loads(msg.payload.decode())
            except Exception as e:
                logger.warning(f"[Edge] MQTT: bad JSON on {msg.topic}: {e}")
                return
            if msg.topic == fog_topic:
                _handle_fog_command(payload)
            elif msg.topic == agent_cmd_topic:
                _handle_agent_command(payload)


        c.on_connect = on_connect
        c.on_disconnect = on_disconnect
        c.on_message = on_message


        # async loop
        c.connect_async(self.cfg.fog_mqtt_host, self.cfg.fog_mqtt_port)
        c.loop_start()