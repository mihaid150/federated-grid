from cloud.communication.amqp import AmqpClient, declare_durable_queue
from shared.node_state import FederatedNodeState
from shared.logging_config import logger
from cloud.communication.config import CloudConfig
from cloud.communication.state import RoundState
from cloud.communication.mqtt import MqttPublisher
from cloud.communication.commands import Commands
from cloud.communication.broadcast import Broadcaster
from cloud.communication.ingest import Ingestor
from cloud.communication.agent_listener import AgentCommandListener
import threading

class CloudCoordinator:
    """
    A thin façade that your FastAPI startup can use.
    Keeps the same high-level API you had in CloudMessaging.
    """
    def __init__(self, cfg: CloudConfig | None = None):
        self.cfg = cfg or CloudConfig()
        self.state = RoundState()
        self.pub = MqttPublisher(self.cfg.cloud_mqtt_host, self.cfg.cloud_mqtt_port)
        self.commands = Commands(self.pub, self.state)
        self.broadcaster = Broadcaster(self.cfg, self.state, self.pub)
        self.ingestor = Ingestor(self.cfg, self.state, self.pub)
        self.agent_listener = AgentCommandListener(self.cfg, self.state, self.pub)
        self._run_boot_housekeeping()

    def _run_boot_housekeeping(self):
        # 1) Clear retained MQTT topics on cloud
        if self.cfg.purge_mqtt_retained_on_boot:
            topics = [t.strip() for t in (self.cfg.clear_retained_topics or "").split(",") if t.strip()]
            for t in topics:
                try:
                    self.pub.clear_retained(t)
                    logger.info("[Cloud]: cleared retained MQTT on topic '%s'.", t)
                except Exception as e:
                    logger.warning("[Cloud]: failed clearing retained on '%s': %s", t, e)

        # 2) Purge per-fog AMQP fanout queues on the cloud broker
        if self.cfg.purge_amqp_on_boot:
            try:
                conn = AmqpClient(self.cfg.cloud_amqp_host).open_blocking()
                ch = conn.channel()
                fogs = getattr(FederatedNodeState.get_current_node(), "child_nodes", []) or []
                for fog in fogs:
                    q = f"cloud_fanout_for_{fog.name}"
                    declare_durable_queue(ch, q)  # ensure exists, durable
                    ch.queue_purge(queue=q)
                    logger.info("[Cloud]: purged AMQP queue '%s' on boot.", q)
            except Exception as e:
                logger.warning("[Cloud]: AMQP purge-on-boot failed: %s", e)
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    # --- API from routes/handlers ---
    def notify_all_edges_to_create_local_model(self):
        self.commands.notify_create_local_model()

    def notify_all_edges_to_start_first_training(self, data: dict):
        self.commands.notify_start_first_training(data)

    def broadcast_cloud_model(self, data: dict):
        self.broadcaster.broadcast_model(data)

    # --- background listeners ---
    def start_background_consumers(self):
        threading.Thread(target=self.ingestor.start, daemon=True).start()
        threading.Thread(target=self.agent_listener.start, daemon=True).start()
        logger.info("[Cloud]: AMQP ingestor thread started.")
        logger.info("[Cloud]: agent-command listener thread started.")
