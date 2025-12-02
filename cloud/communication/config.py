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

    # objective-based scheduling knobs (shared with cloud agent)
    cloud_selection_objective: str = os.getenv("CLOUD_SELECTION_OBJECTIVE", "r2").lower()
    cloud_subset_min: int = int(os.getenv("CLOUD_SUBSET_MIN", "1"))
    cloud_subset_max: int = int(os.getenv("CLOUD_SUBSET_MAX", "2"))

    # federated DB manager integration
    fdbm_base_url: str = os.getenv(
        "FDBM_BASE_URL",
        os.getenv("FED_DB_MANAGER_URL", "http://fdbm-app.cloud.svc.cluster.local:8080"),
    )
    fdbm_timeout: float = float(os.getenv("FDBM_TIMEOUT", "4.0"))
