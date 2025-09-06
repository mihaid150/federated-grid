import os
from dataclasses import dataclass


@dataclass(frozen=True)
class EdgeConfig:
    # Fog brokers
    fog_amqp_host: str = os.getenv('FOG_RABBITMQ_HOST', 'rabbitmq-fog1')
    fog_mqtt_host: str = os.getenv('FOG_MQTT_HOST', 'mqtt-fog1')
    fog_mqtt_port: int = int(os.getenv('FOG_MQTT_PORT', 1883))


    # MQTT behavior
    mqtt_clean_session: bool = False
    mqtt_min_delay: int = 1
    mqtt_max_delay: int = 30