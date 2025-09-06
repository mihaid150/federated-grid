import os
from dataclasses import dataclass

@dataclass(frozen=True)
class CloudConfig:
    cloud_amqp_host: str = os.getenv("CLOUD_RABBITMQ_HOST", "rabbitmq-cloud")
    cloud_mqtt_host: str = os.getenv("CLOUD_MQTT_HOST", "mqtt-cloud")
    cloud_mqtt_port: int = int(os.getenv("CLOUD_MQTT_PORT", 1883))

    # existing
    dev_purge_on_boot: bool = os.getenv("DEV_PURGE_ON_BOOT", "false").lower() == "true"

    # new
    purge_amqp_on_boot: bool = os.getenv("CLOUD_PURGE_AMQP_ON_BOOT", "false").lower() == "true"
    purge_mqtt_retained_on_boot: bool = os.getenv("CLOUD_PURGE_MQTT_RETAINED_ON_BOOT", "false").lower() == "true"
    clear_retained_topics: str = os.getenv("CLOUD_CLEAR_RETAINED_TOPICS", "cloud/fog/command")
