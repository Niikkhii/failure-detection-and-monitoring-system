from detector.engine import DetectionEngine
import statistics


# ── Threshold tests ────────────────────────────────────────────────────────

def test_threshold_no_alert_below_threshold():
    engine = DetectionEngine()
    assert engine.check_metric("cpu", 50.0) is None


def test_threshold_warning_alert():
    engine = DetectionEngine()
    alert = engine.check_metric("cpu", 81.0)
    assert alert is not None
    assert alert["metric"] == "cpu"
    assert alert["level"] == "warning"


def test_threshold_critical_alert():
    engine = DetectionEngine()
    alert = engine.check_metric("cpu", 97.0)
    assert alert is not None
    assert alert["level"] == "critical"


def test_threshold_update():
    engine = DetectionEngine()
    engine.update_threshold("cpu", 90.0)
    assert engine.check_metric("cpu", 85.0) is None


# ── Z-score anomaly detection tests ───────────────────────────────────────

def test_no_anomaly_on_stable_data():
    engine = DetectionEngine()
    assert engine.detect_anomaly([50.0] * 10) is False


def test_no_anomaly_below_window():
    engine = DetectionEngine()
    assert engine.detect_anomaly([50.0] * 5, window=10) is False


def test_anomaly_detected_on_spike():
    engine = DetectionEngine()
    # Tightly clustered values then a massive spike — well beyond 3 sigma
    values = [50.0, 50.1, 49.9, 50.2, 49.8, 50.0, 50.1, 49.9, 50.0, 500.0]
    assert engine.detect_anomaly(values) is True


def test_anomaly_not_triggered_at_2sigma():
    """3-sigma should NOT flag a moderate 2.5-sigma deviation."""
    engine = DetectionEngine()
    controlled = [48.0, 49.0, 50.0, 51.0, 52.0, 50.0, 49.0, 51.0, 50.0]
    std_c = statistics.stdev(controlled)
    moderate_spike = 50.0 + 2.5 * std_c
    values = controlled + [moderate_spike]
    assert engine.detect_anomaly(values, window=10) is False


def test_detect_anomaly_with_detail_returns_direction():
    engine = DetectionEngine()
    # Tight cluster then massive upward spike
    batch = [50.0, 50.1, 49.9, 50.2, 49.8, 50.0, 50.1, 49.9, 50.0]
    result = engine.detect_anomaly_with_detail(value=500.0, batch_values=batch + [500.0])
    assert result.is_anomaly is True
    assert result.direction == "high"
    assert result.z_score > 3.0


def test_detect_anomaly_with_detail_low_direction():
    engine = DetectionEngine()
    # Tight cluster then massive downward spike
    batch = [90.0, 90.1, 89.9, 90.2, 89.8, 90.0, 90.1, 89.9, 90.0]
    result = engine.detect_anomaly_with_detail(value=1.0, batch_values=batch + [1.0])
    assert result.is_anomaly is True
    assert result.direction == "low"
    assert result.z_score < -3.0


def test_detect_anomaly_insufficient_data():
    engine = DetectionEngine()
    result = engine.detect_anomaly_with_detail(value=50.0, batch_values=[50.0])
    assert result.is_anomaly is False
    assert result.z_score == 0.0
