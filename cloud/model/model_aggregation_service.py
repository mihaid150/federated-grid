import os
import tensorflow as tf
import numpy as np
from cloud.communication.cloud_resources_paths import CloudResourcesPaths
from shared.logging_config import logger


def aggregate_received_models(fog_models_cache: dict):
    """
    Perform a simple weight-wise average of all aggregated fog models in
    self.fog_models_cache.  If a cloud model already exists at
    CloudResourcesPaths.CLOUD_MODEL_FILE_PATH, include it in the average.
    Saves the resulting model back to CloudResourcesPaths.CLOUD_MODEL_FILE_PATH.

    :return: the aggregated Keras model, or None if no models are available.
    """

    aggregated_weights = None
    model_count = 0
    cloud_model = None

    # Include existing cloud model if present
    if os.path.exists(CloudResourcesPaths.CLOUD_MODEL_FILE_PATH):
        cloud_model = tf.keras.models.load_model(CloudResourcesPaths.CLOUD_MODEL_FILE_PATH)
        aggregated_weights = [w.astype(np.float64) for w in cloud_model.get_weights()]
        model_count = 1

    # Include each fog model
    for fog_id, entry in fog_models_cache.items():
        model_path = entry["model_path"]
        model = tf.keras.models.load_model(model_path)
        weights = model.get_weights()
        if aggregated_weights is None:
            aggregated_weights = [w.astype(np.float64) for w in weights]
        else:
            for i in range(len(aggregated_weights)):
                aggregated_weights[i] += weights[i]
        model_count += 1

    if aggregated_weights is None or model_count == 0:
        logger.warning("Cloud: no models available to aggregate.")
        return None

    # Compute the simple average
    for i in range(len(aggregated_weights)):
        aggregated_weights[i] = (aggregated_weights[i] / model_count).astype(np.float32)

    # Use the existing cloud model as template, or fall back to the first fog model
    if cloud_model is None:
        # Grab an arbitrary fog model as a template for architecture
        first_fog_model_path = next(iter(fog_models_cache.values()))["model_path"]
        cloud_model = tf.keras.models.load_model(first_fog_model_path)

    cloud_model.set_weights(aggregated_weights)

    # Ensure the destination directory exists
    os.makedirs(os.path.dirname(CloudResourcesPaths.CLOUD_MODEL_FILE_PATH), exist_ok=True)
    cloud_model.save(CloudResourcesPaths.CLOUD_MODEL_FILE_PATH)
    logger.info("Cloud: aggregated model saved to %s", CloudResourcesPaths.CLOUD_MODEL_FILE_PATH)

    # Optionally clear fog model cache for next round
    fog_models_cache.clear()