import base64, json
import pika
from shared.logging_config import logger
from shared.node_state import FederatedNodeState


class ModelUploader:
    def __init__(self, fog_amqp_host: str):
        self.host = fog_amqp_host


    def _create_connection(self) -> pika.BlockingConnection:
        return pika.BlockingConnection(pika.ConnectionParameters(host=self.host))


    def send_trained_model(self, model_path: str, metrics: dict) -> None:
        with open(model_path, "rb") as f:
            model_bytes = f.read()
        model_b64 = base64.b64encode(model_bytes).decode('utf-8')
        node = FederatedNodeState.get_current_node()
        payload = {
        "edge_mac": node.device_mac,
        "edge_name": node.name,
        "model": model_b64,
        "metrics": metrics,
        }
        conn = self._create_connection()
        ch = conn.channel()
        ch.queue_declare(queue='edge_to_fog_models', durable=True, auto_delete=False)
        ch.basic_publish(
        exchange='', routing_key='edge_to_fog_models', body=json.dumps(payload).encode('utf-8'),
        properties=pika.BasicProperties(delivery_mode=2)
        )
        conn.close()
        logger.info(f"[Edge] {node.name}: sent trained model and metrics to fog.")