import asyncio
from typing import Dict, Any, List

class MonitoringAgent:
    def __init__(self, db, alert_manager, detection_engine, window_size: int = 10):
        self.db = db
        self.alert_manager = alert_manager
        self.detection_engine = detection_engine
        self.is_running = False
        self.batch_count = 0
        self.window_size = window_size

    async def start(self):
        """Start the monitoring processor loop."""
        self.is_running = True
        print("Monitoring agent started")
        await self.process_metrics()

    async def stop(self):
        """Stop the monitoring agent"""
        self.is_running = False
        print("Monitoring agent stopped")
    
    async def process_metrics(self):
        """Process raw metrics in batches when enough samples are available."""
        while self.is_running:
            try:
                raw_count = self.db.get_raw_metrics_count()
                if raw_count < self.window_size:
                    await asyncio.sleep(5)
                    continue

                grouped = self.db.get_last_n_raw_metrics(self.window_size)
                self.batch_count += 1

                for metric_type, values in grouped.items():
                    if not values:
                        continue

                    mean = sum(values) / len(values)
                    min_val = min(values)
                    max_val = max(values)
                    variance = sum((x - mean) ** 2 for x in values) / len(values)
                    std_dev = variance ** 0.5

                    latest = values[0]
                    # Z-score anomaly detection (3-sigma rule)
                    anomaly_result = self.detection_engine.detect_anomaly_with_detail(
                        value=latest, batch_values=values
                    )
                    anomaly = anomaly_result.is_anomaly
                    threshold_alert = self.detection_engine.check_metric(metric_type, max_val)
                    threshold_exceeded = threshold_alert is not None

                    if threshold_alert:
                        self.alert_manager.create_alert(
                            level=threshold_alert["level"],
                            message=(
                                f"{metric_type.upper()} threshold exceeded: "
                                f"{max_val:.2f} >= {threshold_alert['threshold']}"
                            ),
                            source="batch_processor",
                            metric_type=metric_type,
                            value=max_val,
                            threshold=threshold_alert["threshold"],
                        )
                    elif anomaly:
                        self.alert_manager.create_alert(
                            level="warning",
                            message=(
                                f"Anomaly detected in {metric_type}: "
                                f"latest={latest:.2f}, mean={anomaly_result.mean:.2f}, "
                                f"std={anomaly_result.std_dev:.2f}, "
                                f"z_score={anomaly_result.z_score:.2f} ({anomaly_result.direction})"
                            ),
                            source="batch_processor",
                            metric_type=metric_type,
                            value=latest,
                        )

                    self.db.insert_processed_metric(
                        metric_type=metric_type,
                        mean=mean,
                        min_val=min_val,
                        max_val=max_val,
                        std_dev=std_dev,
                        anomaly=anomaly,
                        threshold_exceeded=threshold_exceeded,
                    )

                self.db.insert_event(
                    "batch_processed",
                    f"batch={self.batch_count}, rows={raw_count}",
                )
                self.db.clear_raw_metrics()
                await asyncio.sleep(5)
            except Exception as e:
                print(f"Error in metric processing: {e}")
                await asyncio.sleep(5)

    def get_status(self) -> Dict[str, Any]:
        """Get agent status"""
        return {
            "is_running": self.is_running,
            "window_size": self.window_size,
            "batches_processed": self.batch_count,
            "raw_metrics_count": self.db.get_raw_metrics_count(),
            "active_alerts": len(self.alert_manager.get_active_alerts()),
        }
