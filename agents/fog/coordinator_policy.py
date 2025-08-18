import ray

@ray.remote
class CoordinatorPolicy:
    def decide(self, signals: dict):
        # signals: {"node": str, "drift": float, "mae": float, "backlog": int}
        drift = signals.get("drift", 0)
        mae = signals.get("mae", 0)
        backlog = signals.get("backlog", 0)
        if drift or mae > 0.15:
            return {"action": "RETRAIN", "where": "fog", "budget": "LOW"}
        if backlog > 10:
            return {"action": "THROTTLE", "rate": 0.5}
        return {"action": "NOOP"}