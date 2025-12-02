import json, random, socket, time
import pika
from shared.logging_config import logger
from shared.node_state import FederatedNodeState
from edge.communication.config import EdgeConfig


class AmqpControl:
    def __init__(self, cfg: EdgeConfig, edge_service):
        self.cfg, self.edge_service = cfg, edge_service


    def start(self):
        edge_name = FederatedNodeState.get_current_node().name
        queue_name = f'edge_{edge_name}_messages_queue'
        delay, max_delay = 5, 60
        announced_down = False


        while True:
            conn = None
            try:
                params = pika.ConnectionParameters(
                host=self.cfg.fog_amqp_host, heartbeat=30, blocked_connection_timeout=60,
                client_properties={"connection_name": f"edge:{edge_name}:cmd-consumer"},
                )
                conn = pika.BlockingConnection(params)
                ch = conn.channel(); ch.basic_qos(prefetch_count=1)
                ch.queue_declare(queue=queue_name, durable=True, auto_delete=False)


                def on_amqp_command(ch_, method, _props, body):
                    try:
                        msg = json.loads(body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else body)
                        cmd = str(msg.get('command'))
                        if cmd == '2':
                            logger.info(f"[Edge] {edge_name}: received AMQP command 2 (retrain/broadcast).")
                        try:
                            self.edge_service.retrain_fog_model(msg)
                            logger.info(f"[Edge] {edge_name}: retrained local model.")
                        except Exception:
                            logger.exception(f"[Edge]: retrain_fog_model failed")
                        else:
                            logger.debug(f"[Edge]: AMQP message ignored (command={cmd}).", cmd)
                    except Exception:
                        logger.exception(f"[Edge]: failed to parse/handle AMQP message")
                    finally:
                        ch_.basic_ack(delivery_tag=method.delivery_tag)


                ch.basic_consume(queue=queue_name, on_message_callback=on_amqp_command, auto_ack=False)
                logger.info(f"[Edge] {edge_name}: consuming AMQP commands on {queue_name}")
                if announced_down:
                    logger.info(f"[Edge]: AMQP reconnected.")
                    announced_down = False
                delay = 5
                ch.start_consuming()


            except (socket.gaierror, pika.exceptions.AMQPError) as e:
                if not announced_down:
                    logger.warning(f"[Edge]: AMQP unavailable ({e.__class__.__name__}). Backing off up to {max_delay}...")
                    announced_down = True
                time.sleep(delay + random.uniform(0, 1.0)); delay = min(delay * 2, max_delay)
            except Exception:
                logger.exception(f"[Edge]: unexpected AMQP listener error; retrying in {delay}")
                time.sleep(delay); delay = min(delay * 2, max_delay)
            finally:
                try:
                    if conn and conn.is_open:
                        conn.close()
                except Exception:
                    pass