from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

# ---------------------------------------------------------------------------
# Z-score constants
# ---------------------------------------------------------------------------
# 3.0 = 3-sigma rule: ~99.7% of normal data won't trigger a false alert.
# Industry standard used by Datadog, Prometheus, and Grafana.
Z_SCORE_THRESHOLD = 3.0

# Guard against near-zero std_dev (perfectly flat data) causing division errors
MIN_STD_DEV = 0.01


@dataclass
class AnomalyResult:
    is_anomaly: bool
    z_score: float
    direction: str   # "high", "low", or "none"
    mean: float
    std_dev: float


class DetectionEngine:
    def __init__(self, thresholds: Optional[Dict[str, float]] = None):
        self.thresholds = thresholds or {
            "cpu": 80.0,
            "memory": 85.0,
            "disk": 90.0,
            "error_rate": 5.0,
        }
        self.alert_history: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Threshold check  (unchanged API — used by monitor.py and tests)
    # ------------------------------------------------------------------

    def check_metric(self, metric_name: str, value: float) -> Optional[Dict[str, Any]]:
        """Check if a metric value exceeds its configured threshold."""
        if metric_name not in self.thresholds:
            return None
        threshold = self.thresholds[metric_name]
        if value >= threshold:
            alert = {
                "timestamp": datetime.now().isoformat(),
                "metric": metric_name,
                "value": value,
                "threshold": threshold,
                "status": "alert",
                "level": "critical" if value >= threshold * 1.2 else "warning",
            }
            self.alert_history.append(alert)
            return alert
        return None

    # ------------------------------------------------------------------
    # Z-score anomaly detection  (replaces the old 2-std_dev window check)
    # ------------------------------------------------------------------

    def detect_anomaly(self, metrics: List[float], window: int = 10) -> bool:
        """
        Detect anomalies using the 3-sigma Z-score rule.

        Previous approach: abs(latest - mean) > 2 * std_dev
        Problem: 2-sigma flags ~5% of normal data as anomalies (too noisy).

        New approach: Z-score with 3-sigma threshold (~0.3% false positive rate).
        This is the industry standard used by Datadog, Prometheus, and Grafana.

        Args:
            metrics: Full list of historical values for a metric.
            window:  Number of recent samples to use as the reference window.

        Returns:
            True if the latest value is anomalous, False otherwise.
        """
        if len(metrics) < window:
            return False
        recent = metrics[-window:]
        result = self._zscore_check(recent[-1], recent)
        return result.is_anomaly

    def detect_anomaly_with_detail(
        self, value: float, batch_values: List[float]
    ) -> AnomalyResult:
        """
        Full Z-score check returning AnomalyResult with z_score and direction.
        Used in monitor.py for richer alert messages.

        Args:
            value:        Current metric value to evaluate.
            batch_values: Recent batch of values for this metric type.
        """
        if len(batch_values) < 2:
            return AnomalyResult(
                is_anomaly=False, z_score=0.0,
                direction="none", mean=value, std_dev=0.0
            )
        return self._zscore_check(value, batch_values)

    def _zscore_check(self, value: float, samples: List[float]) -> AnomalyResult:
        """Internal: compute Z-score for value against samples."""
        n = len(samples)
        mean = sum(samples) / n
        variance = sum((x - mean) ** 2 for x in samples) / max(n - 1, 1)
        std_dev = variance ** 0.5
        effective_std = max(std_dev, MIN_STD_DEV)
        z_score = (value - mean) / effective_std
        is_anomaly = abs(z_score) > Z_SCORE_THRESHOLD
        direction = "none"
        if is_anomaly:
            direction = "high" if z_score > 0 else "low"
        return AnomalyResult(
            is_anomaly=is_anomaly,
            z_score=round(z_score, 4),
            direction=direction,
            mean=round(mean, 4),
            std_dev=round(std_dev, 4),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def update_threshold(self, metric: str, threshold: float) -> None:
        """Update threshold for a metric at runtime."""
        self.thresholds[metric] = threshold

    def get_alert_history(self) -> List[Dict[str, Any]]:
        return self.alert_history

    def clear_history(self) -> None:
        self.alert_history = []
