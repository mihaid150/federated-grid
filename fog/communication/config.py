import os
from dataclasses import dataclass

@dataclass(frozen=True)
class FogConfig:
    # AMQP
    fog_amqp_host: str = os.getenv("FOG_RABBITMQ_HOST", "rabbitmq-fog1")
    cloud_amqp_host: str = os.getenv("CLOUD_RABBITMQ_HOST", "rabbitmq-cloud")

    # MQTT
    fog_mqtt_host: str = os.getenv('FOG_MQTT_HOST', 'mqtt-fog1')
    fog_mqtt_port: int = int(os.getenv('FOG_MQTT_PORT', 1883))
    cloud_mqtt_host: str = os.getenv('CLOUD_MQTT_HOST', 'mqtt-cloud')
    cloud_mqtt_port: int = int(os.getenv('CLOUD_MQTT_PORT', 1883))

    # Behavior flags
    purge_amqp_on_boot: bool = os.getenv("FOG_PURGE_AMQP_ON_BOOT", "false").lower() == "true"
    purge_edge_models_queue_on_boot: bool = os.getenv("FOG_PURGE_EDGE_MODELS_ON_BOOT", "false").lower() == "true"
    purge_mqtt_retained_on_boot: bool = os.getenv("FOG_PURGE_MQTT_RETAINED_ON_BOOT", "false").lower() == "true"
    mqtt_clean_session: bool = os.getenv("FOG_MQTT_CLEAN_SESSION", "false").lower() == "true"
    ignore_cloud_retained_secs: int = int(os.getenv("FOG_IGNORE_CLOUD_RETAINED_SECS", "15"))

    # Outbox
    resume_outbox_on_boot: bool = os.getenv('FOG_RESUME_OUTBOX_ON_BOOT', 'false').lower() == 'true'
    outbox_ttl_secs: int = int(os.getenv('FOG_OUTBOX_TTL_SECS', '86400'))
    use_quorum_queues: bool = os.getenv("FOG_USE_QUORUM_QUEUES", "false").lower() == "true"

    # Topics (to integrate with fog-agent)
    topic_edge_model_received: str = os.getenv("TOPIC_EDGE_MODEL_RECEIVED", "fog/events/edge-model-received")
    topic_aggregation_complete: str = os.getenv("TOPIC_AGGREGATION_COMPLETE", "fog/events/aggregation-complete")
    topic_cloud_model_downlink: str = os.getenv("TOPIC_CLOUD_MODEL_DOWNLINK", "fog/events/cloud-model-downlink")
    topic_agent_commands: str = os.getenv("TOPIC_AGENT_COMMANDS", "fog/agent/commands")