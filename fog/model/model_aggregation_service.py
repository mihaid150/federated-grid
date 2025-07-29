import os
import tensorflow as tf
from fog.communication.fog_resources_paths import FogResourcesPaths


def aggregate_models_with_metrics(edge_models_cache: dict,
                                  fog_weight: float = 1.0):
    """
    Aggregate the current fog model (if present) with all edge models,
    weighting each model by the inverse of its MSE metric.  If the fog
    model file does not exist, only edge models are aggregated.

    :param edge_models_cache: dict mapping edge IDs to {"model_path": str, "metrics": dict}
    :param fog_weight: optional weight for the current fog model (default 1.0)
    :return: the aggregated Keras model, or None if there was nothing to aggregate
    """
    aggregated_weights = None
    total_weight = 0.0
    fog_model = None

    # If the fog model exists, include it in the aggregation
    if os.path.exists(FogResourcesPaths.FOG_MODEL_FILE_PATH):
        fog_model = tf.keras.models.load_model(FogResourcesPaths.FOG_MODEL_FILE_PATH)
        aggregated_weights = [w * fog_weight for w in fog_model.get_weights()]
        total_weight = fog_weight

    # Keep track of a model path to use as a template if fog_model is None
    template_model_path = None

    # Aggregate each edge model
    for edge_id, entry in edge_models_cache.items():
        model_path = entry["model_path"]
        template_model_path = model_path if template_model_path is None else template_model_path
        metrics = entry.get("metrics", {})
        mse = metrics.get("mse", 1.0)
        weight = 1.0 / (mse + 1e-8)

        edge_model = tf.keras.models.load_model(model_path)
        edge_weights = edge_model.get_weights()

        if aggregated_weights is None:
            # First model we encounter becomes the starting point
            aggregated_weights = [w * weight for w in edge_weights]
            total_weight = weight
        else:
            for idx in range(len(aggregated_weights)):
                aggregated_weights[idx] += weight * edge_weights[idx]
            total_weight += weight

    # Nothing to aggregate if no fog model and no edge models
    if aggregated_weights is None or total_weight == 0.0:
        return None

    # Compute the average
    for idx in range(len(aggregated_weights)):
        aggregated_weights[idx] = aggregated_weights[idx] / total_weight

    # If we didn't load an existing fog model, use one of the edge models as the base architecture
    if fog_model is None:
        if template_model_path is None:
            return None  # should not happen, checked above
        fog_model = tf.keras.models.load_model(template_model_path)

    # Set the aggregated weights and save the updated model
    fog_model.set_weights(aggregated_weights)
    fog_model.save(FogResourcesPaths.FOG_MODEL_FILE_PATH)
    return fog_model
