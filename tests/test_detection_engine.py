from detector.engine import DetectionEngine


# ── Threshold tests (unchanged behaviour) ──────────────────────────────────

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
    # 80 * 1.2 = 96 → critical
    alert = engine.check_metric("cpu", 97.0)
    assert alert is not None
    assert alert["level"] == "critical"


def test_threshold_update():
    engine = DetectionEngine()
    engine.update_threshold("cpu", 90.0)
    assert engine.check_metric("cpu", 85.0) is None


# ── Z-score anomaly detection tests ────────────────────────────────────────

def test_no_anomaly_on_stable_data():
    engine = DetectionEngine()
    stable = [50.0] * 10
    assert engine.detect_anomaly(stable) is False


def test_no_anomaly_below_window():
    engine = DetectionEngine()
    # Only 5 values but window=10 → should not flag
    assert engine.detect_anomaly([50.0] * 5, window=10) is False


def test_anomaly_detected_on_spike():
    engine = DetectionEngine()
    # 9 stable values then a huge spike — clearly > 3 sigma
    values = [50.0] * 9 + [200.0]
    assert engine.detect_anomaly(values) is True


def test_anomaly_not_triggered_at_2sigma():
    """
    Old code used 2-sigma, which has ~5% false-positive rate.
    New 3-sigma code should NOT flag a moderate 2.5-sigma deviation.
    """
    engine = DetectionEngine()
    import statistics
    mean = 50.0
    # Use a controlled dataset with measurable variance
    controlled = [48.0, 49.0, 50.0, 51.0, 52.0, 50.0, 49.0, 51.0, 50.0]
    std_c = statistics.stdev(controlled)
    # 2.5-sigma spike
    moderate_spike = mean + 2.5 * std_c
    values = controlled + [moderate_spike]
    assert engine.detect_anomaly(values, window=10) is False


def test_detect_anomaly_with_detail_returns_direction():
    engine = DetectionEngine()
    batch = [50.0] * 9
    # Large upward spike
    result = engine.detect_anomaly_with_detail(value=200.0, batch_values=batch + [200.0])
    assert result.is_anomaly is True
    assert result.direction == "high"
    assert result.z_score > 3.0


def test_detect_anomaly_with_detail_low_direction():
    engine = DetectionEngine()
    batch = [90.0] * 9
    # Large downward spike
    result = engine.detect_anomaly_with_detail(value=10.0, batch_values=batch + [10.0])
    assert result.is_anomaly is True
    assert result.direction == "low"
    assert result.z_score < -3.0


def test_detect_anomaly_insufficient_data():
    engine = DetectionEngine()
    result = engine.detect_anomaly_with_detail(value=50.0, batch_values=[50.0])
    assert result.is_anomaly is False
    assert result.z_score == 0.0
