"""SQLite 连接与建表。

数据库文件路径由环境变量 DB_PATH 决定，默认 ./lab.db。
应用启动时调用 init_db() 自动创建数据库与表结构。
"""

import os
import sqlite3
import threading
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "lab.db"))

# 摄入操作使用进程级锁串行化，保证"缓冲-排水-提交"在同一事务中原子完成。
ingest_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id      TEXT PRIMARY KEY,
    committed_upto INTEGER NOT NULL,          -- 已连续提交到的最大序号
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key           TEXT PRIMARY KEY,
    device_id     TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    payload_hash  TEXT NOT NULL,              -- 请求体规范化后的 sha256，用于检测同键不同载荷
    response_json TEXT NOT NULL,              -- 首次处理结果，重放时原样返回
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS buffer (
    device_id    TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    received_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (device_id, seq)
);

CREATE TABLE IF NOT EXISTS committed (
    device_id    TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    committed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (device_id, seq)
);

CREATE TABLE IF NOT EXISTS duplicates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id       TEXT NOT NULL,
    seq             INTEGER,
    idempotency_key TEXT,
    reason          TEXT NOT NULL,            -- idempotent_replay / already_committed / already_buffered
    received_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_duplicates_device ON duplicates (device_id, id);

-- 阈值告警规则：每台设备的每个指标至多一条规则
CREATE TABLE IF NOT EXISTS alert_rules (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id      TEXT NOT NULL,
    metric         TEXT NOT NULL,
    upper          REAL,                       -- 上限；value >= upper 视为越限（null 表示不启用）
    lower          REAL,                       -- 下限；value <= lower 视为越限（null 表示不启用）
    trigger_count  INTEGER NOT NULL,           -- 连续越限多少次后触发告警
    recover_count  INTEGER NOT NULL,           -- 告警中连续恢复多少次后解除
    enabled        INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (device_id, metric)
);

-- 规则状态机持久化状态（与规则一一对应，随规则级联删除）
CREATE TABLE IF NOT EXISTS alert_rule_states (
    rule_id        INTEGER PRIMARY KEY REFERENCES alert_rules(id) ON DELETE CASCADE,
    state          TEXT NOT NULL DEFAULT 'normal',  -- normal / alarm
    consecutive    INTEGER NOT NULL DEFAULT 0,      -- 当前状态下的连续计数
    evaluated_upto INTEGER NOT NULL DEFAULT 0,      -- 已评估到的最大提交序号（建规则前历史不回补）
    trigger_kind   TEXT,                            -- 本次告警的越限方向：upper / lower
    trigger_value  REAL,                            -- 触发时刻的测量值
    triggered_seq  INTEGER,                         -- 触发包序号
    triggered_at   TEXT,                            -- 触发时间（UTC ISO 8601）
    last_seq       INTEGER,                         -- 最近评估的包序号
    last_value     REAL                             -- 最近评估的测量值
);

-- 告警事件（触发 / 恢复），规则删除后保留审计轨迹，rule_id 置空
CREATE TABLE IF NOT EXISTS alert_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id     INTEGER REFERENCES alert_rules(id) ON DELETE SET NULL,
    device_id   TEXT NOT NULL,
    metric      TEXT NOT NULL,
    event_type  TEXT NOT NULL,                      -- triggered / recovered
    kind        TEXT,                               -- upper / lower（仅 triggered）
    seq         INTEGER NOT NULL,                   -- 产生事件的提交包序号
    value       REAL NOT NULL,                      -- 产生状态跃迁的测量值
    event_time  TEXT NOT NULL,                      -- 触发或恢复时间（UTC ISO 8601）
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_alert_events_device_metric
    ON alert_events (device_id, metric, id);
CREATE INDEX IF NOT EXISTS idx_alert_rules_device_metric
    ON alert_rules (device_id, metric);
CREATE INDEX IF NOT EXISTS idx_alert_states_state
    ON alert_rule_states (state);
"""


def init_db() -> None:
    """创建数据库文件与全部表（幂等）。"""
    directory = os.path.dirname(os.path.abspath(DB_PATH))
    os.makedirs(directory, exist_ok=True)
    with get_conn() as conn:
        conn.executescript(SCHEMA)


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
