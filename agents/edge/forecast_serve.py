import ray
from ray import serve
from typing import Dict
from agents.common.contracts import ForecastRequest, ForecastResponse

@serve.deployment(route_prefix="/forecast")
class ForecasterAgent:
    def __init__(self):
        # Minimal naive forecaster placeholder; replace with ARIMA/darts later
        self.history = []

    async def __call__(self, request):
        body = await request.json()
        req = ForecastRequest(**body)
        last = self.history[-1] if self.history else 0.0
        preds = [last for _ in range(req.horizon)]
        return ForecastResponse(preds=preds).model_dump()

    def ingest(self, value: float):
        self.history.append(value)
        if len(self.history) > 10000:
            self.history = self.history[-10000:]

# Local bootstrap for single-node edge
if __name__ == "__main__":
    ray.init(ignore_reinit_error=True)
    serve.run(ForecasterAgent.bind())