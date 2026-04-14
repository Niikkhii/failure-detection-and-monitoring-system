import asyncio

from agent.monitor import MonitoringAgent
from alerting.alerts import AlertManager
from detector.engine import DetectionEngine
from storage.database import Database


def test_pipeline_batch_processing_integration(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "pipeline.db"))
    alerts = AlertManager(db=db)
    engine = DetectionEngine(thresholds={"cpu": 50.0, "memory": 90.0, "disk": 95.0, "error_rate": 5.0})
    agent = MonitoringAgent(db, alerts, engine, window_size=6)

    # Two points per metric gives us enough data for one batch.
    db.insert_raw_metric("cpu", 55.0)
    db.insert_raw_metric("memory", 40.0)
    db.insert_raw_metric("disk", 30.0)
    db.insert_raw_metric("cpu", 60.0)
    db.insert_raw_metric("memory", 45.0)
    db.insert_raw_metric("disk", 31.0)

    original_sleep = asyncio.sleep

    async def fast_sleep(_seconds: float):
        await original_sleep(0)

    monkeypatch.setattr("agent.monitor.asyncio.sleep", fast_sleep)

    async def run_once():
        task = asyncio.create_task(agent.start())
        await asyncio.sleep(0.05)
        await agent.stop()
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run_once())

    processed = db.get_latest_processed_metrics(limit=10)
    assert processed, "Expected processed metrics to be written"
    assert any(row["metric_type"] == "cpu" for row in processed)
    assert db.get_raw_metrics_count() == 0
    assert alerts.get_all_alerts(limit=10), "Expected at least one threshold alert"
