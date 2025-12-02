import tensorflow as tf
from shared.utils import required_columns
from shared.logging_config import logger

available_architectural_models = [
    'simple_lstm_two_gates'
]


def create_model(model_label: str):
    if model_label == 'simple_lstm_two_gates':
        # Build with variable time dimension so different sequence_length values are accepted at train time.
        return simple_lstm_model(sequence_length=None)


def simple_lstm_model(sequence_length: int | None = None, mask_value: int = -1):
    """Create a simple LSTM model.

    If sequence_length is None, the time dimension is variable (None), allowing
    training/inference with different sequence lengths (e.g., 96/144/192) as long
    as the feature dimension matches.
    """
    num_features = len(required_columns) - 1
    time_steps = sequence_length if sequence_length is not None else None
    inputs = tf.keras.layers.Input(shape=(time_steps, num_features), dtype=tf.float32)

    x = tf.keras.layers.Conv1D(filters=32, kernel_size=3, activation='relu', padding='same')(inputs)
    x = tf.keras.layers.BatchNormalization()(x)
    x = tf.keras.layers.Dropout(0.2)(x)

    x = tf.keras.layers.Masking(mask_value=mask_value)(x)

    x = tf.keras.layers.LSTM(64, activation='tanh', return_sequences=True)(x)
    x = tf.keras.layers.LSTM(128, activation='tanh')(x)

    x = tf.keras.layers.Dense(64, activation='relu')(x)
    x = tf.keras.layers.Dropout(0.2)(x)
    outputs = tf.keras.layers.Dense(1)(x)

    model = tf.keras.Model(inputs, outputs)
    optimizer = tf.keras.optimizers.Adam()
    model.compile(optimizer=optimizer, loss='mse', metrics=["mae", "mse"])

    logger.info(f"Created model with input shape ({time_steps}, {num_features})")
    return model
