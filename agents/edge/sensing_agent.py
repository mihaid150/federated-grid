import ray
from datetime import datetime
from river import drift, linear_model, optim, metrics

@ray.remote
class SensingAgent:
    def __init__(self, lr=0.01, mae_window=288):
        # Simple online regressor predicting value from time features
        self.model = linear_model.LinearRegression(optimizer=optim.SGD(lr))
        self.mae_metric = metrics.MAE()
        self.adwin = drift.ADWIN()
        self.errors = []
        self.mae_window = mae_window

    def _features(self, ts: str):
        dt = datetime.fromisoformat(ts)
        return {"hour": dt.hour, "weekday": dt.weekday()}

    def observe(self, ts: str, y_true: float):
        x = self._features(ts)
        y_pred = self.model.predict_one(x) or 0.0
        self.model.learn_one(x, y_true)
        err = abs(y_true - y_pred)
        self.mae_metric.update(y_true, y_pred)
        self.errors.append(err)
        if len(self.errors) > self.mae_window:
            self.errors.pop(0)
        self.adwin.update(err)
        return {
            "drift": float(self.adwin.drift_detected),
            "mae": sum(self.errors) / max(1, len(self.errors)),
            "n": len(self.errors),
        }

    def should_nudge(self, mae_thresh: float, drift_required: bool = True) -> bool:
        mae = sum(self.errors) / max(1, len(self.errors))
        cond = (mae > mae_thresh) and ((not drift_required) or self.adwin.drift_detected)
        return bool(cond)