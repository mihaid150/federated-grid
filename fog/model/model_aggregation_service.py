import os
import time
import tensorflow as tf
from fog.communication.fog_resources_paths import FogResourcesPaths
from shared.utils import delete_files_containing

def _wait_for_file(path: str, timeout_s: float = 5.0, poll_s: float = 0.1) -> bool:
    """Wait until path exists and is non-empty."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if os.path.exists(path) and os.path.getsize(path) > 0:
                return True
        except Exception:
            pass
        time.sleep(poll_s)
    return False

def _safe_load_model(path: str, retries: int = 3, delay_s: float = 0.25):
    """Load a Keras model with simple retries."""
    last_err = None
    for _ in range(retries):
        try:
            return tf.keras.models.load_model(path)
        except Exception as e:
            last_err = e
            time.sleep(delay_s)
    raise last_err

def aggregate_models_with_metrics(edge_models_cache: dict, fog_weight: float = 1.0):
    """
    Aggregate the current fog model (if present) with all edge models,
    weighting each model by inverse MSE. Skips unreadable edge files.
    """
    aggregated_weights = None
    total_weight = 0.0
    fog_model = None
    template_model_path = None

    # include existing fog model if present
    if os.path.exists(FogResourcesPaths.FOG_MODEL_FILE_PATH.value):
        try:
            fog_model = _safe_load_model(FogResourcesPaths.FOG_MODEL_FILE_PATH.value)
            aggregated_weights = [w * fog_weight for w in fog_model.get_weights()]
            total_weight = fog_weight
        except Exception as e:
            # can't read fog model — continue with edges only
            print(f"[fog] WARN: failed to load fog model: {e}")

    # fold in each edge model
    for map_id, entry in edge_models_cache.items():
        model_path = entry["model_path"]
        metrics = entry.get("metrics", {})
        # prefer "after_training"->"mse", else top-level "mse", else 1.0
        mse = (metrics.get("after_training", {}).get("mse")
               or metrics.get("mse", 1.0))
        weight = 1.0 / (float(mse) + 1e-8)

        if not _wait_for_file(model_path, timeout_s=5.0):
            print(f"[fog] WARN: model file not ready: {model_path}")
            continue

        try:
            edge_model = _safe_load_model(model_path)
        except Exception as e:
            print(f"[fog] WARN: failed to load edge model {model_path}: {e}")
            continue

        edge_weights = edge_model.get_weights()
        template_model_path = template_model_path or model_path

        if aggregated_weights is None:
            aggregated_weights = [w * weight for w in edge_weights]
            total_weight = weight
        else:
            for i in range(len(aggregated_weights)):
                aggregated_weights[i] += weight * edge_weights[i]
            total_weight += weight

    if aggregated_weights is None or total_weight == 0.0:
        return None

    # average
    for i in range(len(aggregated_weights)):
        aggregated_weights[i] /= total_weight

    # choose a base model if we didn't have one already
    if fog_model is None:
        if not template_model_path:
            return None
        fog_model = _safe_load_model(template_model_path)

    fog_model.set_weights(aggregated_weights)
    fog_model.save(FogResourcesPaths.FOG_MODEL_FILE_PATH.value, include_optimizer=False)
    delete_files_containing(FogResourcesPaths.MODELS_FOLDER_PATH.value, "edge", [".keras"])

    return fog_model
