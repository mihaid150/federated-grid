# Placeholder for more complex rule policies if you want to move logic outside actors.
class RulePolicy:
    def __init__(self, mae_threshold=0.12, max_retrain_per_day=1):
        self.mae_threshold = mae_threshold
        self.max_retrain_per_day = max_retrain_per_day
        self.tokens = {}

    def allow(self, node: str, day: str) -> bool:
        used = self.tokens.get((node, day), 0)
        if used < self.max_retrain_per_day:
            self.tokens[(node, day)] = used + 1
            return True
        return False