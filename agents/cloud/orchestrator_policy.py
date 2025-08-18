import ray

@ray.remote
class OrchestratorPolicy:
    def decide(self, fleet_ctx: dict):
        # fleet_ctx may aggregate fog signals, SLAs, quotas
        quota = fleet_ctx.get("quota", 0.2)
        pressure = fleet_ctx.get("pressure", 0.0)
        if pressure > 0.7:
            return {"action": "GLOBAL_THROTTLE", "rate": 0.5}
        return {"action": "NOOP", "quota": quota}