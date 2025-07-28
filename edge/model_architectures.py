import tensorflow as tf
from shared.utils import required_columns
from shared.logging_config import logger

available_architectural_models = [
    'simple_lstm_two_gates'
]


def create_model(model_label: str):
    if model_label == 'simple_lstm_two_gates':
        return simple_lstm_model()


def simple_lstm_model(sequence_length: int = 144, mask_value: int = -1):
    num_features = len(required_columns) - 1
    inputs = tf.keras.layers.Input(shape=(sequence_length, num_features), dtype=tf.float32)

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

    logger.info(f"Created model with input shape ({sequence_length}, {num_features})")
    return model