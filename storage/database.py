import sqlite3
from typing import Optional, List, Dict, Any

class Database:
    def __init__(self, db_path: str = "monitoring.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Raw metrics table (collector writes here)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raw_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                metric_type TEXT NOT NULL,
                value REAL NOT NULL,
                server TEXT DEFAULT 'localhost',
                tags TEXT
            )
        ''')

        # Processed metrics table (agent writes here)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                metric_type TEXT NOT NULL,
                mean REAL NOT NULL,
                min_value REAL NOT NULL,
                max_value REAL NOT NULL,
                std_dev REAL,
                anomaly_detected BOOLEAN DEFAULT 0,
                threshold_exceeded BOOLEAN DEFAULT 0
            )
        ''')
        
        # Alerts table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                source TEXT DEFAULT 'system',
                metric_type TEXT,
                value REAL,
                threshold REAL,
                resolved BOOLEAN DEFAULT 0,
                resolved_at DATETIME
            )
        ''')
        
        # Events table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                event_type TEXT NOT NULL,
                data TEXT
            )
        ''')
        
        conn.commit()
        self._ensure_alert_columns(cursor)
        conn.commit()
        conn.close()

    def _ensure_alert_columns(self, cursor):
        cursor.execute("PRAGMA table_info(alerts)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        required_columns = {
            "source": "TEXT DEFAULT 'system'",
            "metric_type": "TEXT",
            "value": "REAL",
            "threshold": "REAL",
            "resolved_at": "DATETIME",
        }
        for col_name, definition in required_columns.items():
            if col_name not in existing_columns:
                cursor.execute(f"ALTER TABLE alerts ADD COLUMN {col_name} {definition}")

    def insert_raw_metric(
        self,
        metric_type: str,
        value: float,
        server: str = "localhost",
        tags: Optional[str] = None,
    ) -> int:
        """Insert a raw metric sample from collector"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO raw_metrics (metric_type, value, server, tags) VALUES (?, ?, ?, ?)',
            (metric_type, value, server, tags),
        )
        conn.commit()
        conn.close()
        return cursor.lastrowid

    # Backward-compatible alias used by existing /metrics POST
    def insert_metric(self, name: str, value: float, tags: Optional[str] = None) -> int:
        return self.insert_raw_metric(metric_type=name, value=value, tags=tags)

    def get_raw_metrics_count(self) -> int:
        """Get raw metrics row count"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM raw_metrics')
        count = cursor.fetchone()[0]
        conn.close()
        return count

    def get_latest_raw_metric_timestamp(self) -> Optional[str]:
        """Get the most recent raw metric timestamp."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT timestamp
            FROM raw_metrics
            ORDER BY id DESC
            LIMIT 1
            '''
        )
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None

    def get_last_n_raw_metrics(self, n: int = 10) -> Dict[str, List[float]]:
        """Get last N raw metrics grouped by metric type"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT metric_type, value
            FROM raw_metrics
            ORDER BY id DESC
            LIMIT ?
            ''',
            (n,),
        )
        rows = cursor.fetchall()
        conn.close()

        grouped: Dict[str, List[float]] = {}
        for metric_type, value in rows:
            grouped.setdefault(metric_type, []).append(value)
        return grouped

    def clear_raw_metrics(self):
        """Delete raw metric rows after batch processing"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM raw_metrics')
        conn.commit()
        conn.close()

    def insert_processed_metric(
        self,
        metric_type: str,
        mean: float,
        min_val: float,
        max_val: float,
        std_dev: float,
        anomaly: bool,
        threshold_exceeded: bool,
    ) -> int:
        """Insert processed metric summary for one batch"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO processed_metrics (
                metric_type, mean, min_value, max_value, std_dev,
                anomaly_detected, threshold_exceeded
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (metric_type, mean, min_val, max_val, std_dev, anomaly, threshold_exceeded),
        )
        conn.commit()
        conn.close()
        return cursor.lastrowid

    def get_latest_processed_metrics(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get latest processed metric rows"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            'SELECT * FROM processed_metrics ORDER BY id DESC LIMIT ?',
            (limit,),
        )
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]

    def insert_alert(
        self,
        level: str,
        message: str,
        source: str = "system",
        metric_type: Optional[str] = None,
        value: Optional[float] = None,
        threshold: Optional[float] = None,
    ) -> int:
        """Insert a new alert"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO alerts (level, message, source, metric_type, value, threshold)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (level, message, source, metric_type, value, threshold),
        )
        conn.commit()
        alert_id = cursor.lastrowid
        conn.close()
        return alert_id

    def resolve_alert(self, alert_id: int) -> bool:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE alerts
            SET resolved = 1, resolved_at = CURRENT_TIMESTAMP
            WHERE id = ? AND resolved = 0
            ''',
            (alert_id,),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated
    
    def insert_event(self, event_type: str, data: Optional[str] = None) -> int:
        """Insert a new event"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO events (event_type, data) VALUES (?, ?)',
            (event_type, data)
        )
        conn.commit()
        conn.close()
        return cursor.lastrowid
    
    def get_metrics(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent raw metrics (API compatibility shape)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT id, timestamp, metric_type AS name, value, tags
            FROM raw_metrics
            ORDER BY id DESC
            LIMIT ?
            ''',
            (limit,),
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    
    def get_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent alerts"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM alerts ORDER BY id DESC LIMIT ?', (limit,))
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]

    def get_active_alerts(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get unresolved alerts"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM alerts
            WHERE resolved = 0
            ORDER BY id DESC
            LIMIT ?
            ''',
            (limit,),
        )
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    
    def get_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent events"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM events ORDER BY timestamp DESC LIMIT ?', (limit,))
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
