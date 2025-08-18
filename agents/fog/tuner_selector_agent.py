import ray
from ray import tune
from mabwiser.mab import MAB, LearningPolicy

@ray.remote
class TunerSelectorAgent:
    def __init__(self):
        self.bandit = MAB(arms=["lstm","arima","xgb"], learning_policy=LearningPolicy.UCB1(alpha=1.25))
        self.bandit.fit(decisions=["arima"], rewards=[0.0])

    def suggest_model(self, context: dict) -> str:
        return self.bandit.predict()

    def reward(self, arm: str, metric: float):
        # Higher reward = better. If metric is MSE, use negative.
        self.bandit.partial_fit(decisions=[arm], rewards=[metric])

    def quick_tune(self, trainable, space: dict, num_samples=16):
        result = tune.run(trainable, config=space, num_samples=num_samples, metric="val_loss", mode="min")
        return {
            "best_config": result.best_config,
            "best_metric": result.best_result.get("val_loss"),
        }