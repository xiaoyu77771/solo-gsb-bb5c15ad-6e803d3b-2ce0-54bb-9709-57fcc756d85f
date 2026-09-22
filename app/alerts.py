"""持久化阈值告警：规则管理 + 带连续计数的迟滞状态机。

评估时机与顺序
--------------
测量值只在包**按序提交（committed）**时评估：摄入流程每提交一个包，
就按提交顺序（seq 升序）调用 evaluate_package 一次。因此：

- 重复包（idempotent replay / already committed / already buffered）不评估、不产事件；
- 乱序缓冲的包不评估；缺口补齐触发排水时，缓冲中的包按 seq 升序各评估一次，
  即"补齐乱序包时只按一次提交顺序处理"。

迟滞状态机（每条 device + metric 规则独立）
------------------------------------------
- normal（正常）：
  - 样本越限（value >= upper 或 value <= lower）-> violate_count += 1，
    累计达到 trigger_count 时跃迁为 alarm，写入 triggered 事件；
  - 样本在限内 -> violate_count 清零（连续计数，抖动样本会重置计数）。
- alarm（告警中）：
  - 样本在限内 -> recover_count += 1，达到规则的 recover_count 时恢复 normal，
    写入 recovered 事件；
  - 样本仍越限 -> recover_count 清零，告警保持。
- 规则在设备已有历史数据之后创建时，evaluated_upto 初始化为当前
  committed_upto，历史已提交包不回补评估。
"""

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .schemas import RuleUpsert

NORMAL = "normal"
ALARM = "alarm"


class RuleConflict(Exception):
    """同设备同指标已存在规则。"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


# --------------------------------------------------------------------------- #
# 规则 CRUD
# --------------------------------------------------------------------------- #
def _rule_to_dict(r: sqlite3.Row) -> Dict[str, Any]:
    d = dict(r)
    d["enabled"] = bool(d.get("enabled", 1))
    return d


def _get_rule_row(conn: sqlite3.Connection, rule_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(_RULE_SELECT + " WHERE r.id = ?", (rule_id,)).fetchone()


_RULE_SELECT = """
SELECT r.id, r.device_id, r.metric, r.upper, r.lower, r.trigger_count,
       r.recover_count, r.enabled, r.created_at, r.updated_at,
       s.state, s.consecutive, s.evaluated_upto, s.trigger_kind,
       s.trigger_value, s.triggered_seq, s.triggered_at,
       s.last_seq, s.last_value
FROM alert_rules r
JOIN alert_rule_states s ON s.rule_id = r.id
"""


def list_rules(
    conn: sqlite3.Connection,
    device_id: Optional[str] = None,
    metric: Optional[str] = None,
) -> List[Dict[str, Any]]:
    where, params = [], []
    if device_id is not None:
        where.append("r.device_id = ?")
        params.append(device_id)
    if metric is not None:
        where.append("r.metric = ?")
        params.append(metric)
    sql = _RULE_SELECT
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY r.device_id, r.metric"
    return [_rule_to_dict(r) for r in conn.execute(sql, params).fetchall()]


def get_rule(conn: sqlite3.Connection, rule_id: int) -> Optional[Dict[str, Any]]:
    row = _get_rule_row(conn, rule_id)
    return _rule_to_dict(row) if row is not None else None


def create_rule(
    conn: sqlite3.Connection, device_id: str, metric: str, body: RuleUpsert
) -> Dict[str, Any]:
    """创建规则及初始状态。建规则之前已提交的历史包不回补评估。"""
    exists = conn.execute(
        "SELECT 1 FROM alert_rules WHERE device_id = ? AND metric = ?",
        (device_id, metric),
    ).fetchone()
    if exists is not None:
        raise RuleConflict(f"设备 {device_id} 指标 {metric} 的告警规则已存在")

    # 建规则时刻设备已提交到的序号，之后更大的序号才参与评估
    dev = conn.execute(
        "SELECT committed_upto FROM devices WHERE device_id = ?", (device_id,)
    ).fetchone()
    evaluated_upto = dev["committed_upto"] if dev is not None else 0

    cur = conn.execute(
        "INSERT INTO alert_rules (device_id, metric, upper, lower,"
        " trigger_count, recover_count, enabled) VALUES (?,?,?,?,?,?,?)",
        (
            device_id,
            metric,
            body.upper,
            body.lower,
            body.trigger_count,
            body.recover_count,
            1 if body.enabled else 0,
        ),
    )
    rule_id = cur.lastrowid
    conn.execute(
        "INSERT INTO alert_rule_states (rule_id, state, consecutive, evaluated_upto)"
        " VALUES (?, 'normal', 0, ?)",
        (rule_id, evaluated_upto),
    )
    return get_rule(conn, rule_id)


def update_rule(
    conn: sqlite3.Connection, rule_id: int, body: RuleUpsert
) -> Optional[Dict[str, Any]]:
    """整体替换规则参数。

    状态（normal/alarm）保持不变，但连续计数清零：新参数下需重新累计
    trigger_count / recover_count 次才会发生下一次跃迁。
    """
    row = _get_rule_row(conn, rule_id)
    if row is None:
        return None
    conn.execute(
        "UPDATE alert_rules SET upper = ?, lower = ?, trigger_count = ?,"
        " recover_count = ?, enabled = ?, updated_at = ? WHERE id = ?",
        (
            body.upper,
            body.lower,
            body.trigger_count,
            body.recover_count,
            1 if body.enabled else 0,
            utc_now_iso(),
            rule_id,
        ),
    )
    conn.execute(
        "UPDATE alert_rule_states SET consecutive = 0 WHERE rule_id = ?",
        (rule_id,),
    )
    return get_rule(conn, rule_id)


def delete_rule(conn: sqlite3.Connection, rule_id: int) -> bool:
    """删除规则及其状态；已产生的告警事件保留（rule_id 置空）。"""
    cur = conn.execute("DELETE FROM alert_rules WHERE id = ?", (rule_id,))
    return cur.rowcount > 0


# --------------------------------------------------------------------------- #
# 状态机评估
# --------------------------------------------------------------------------- #
def _violation_kind(value: float, upper: Optional[float], lower: Optional[float]):
    if upper is not None and value >= upper:
        return "upper"
    if lower is not None and value <= lower:
        return "lower"
    return None


def _add_event(
    conn: sqlite3.Connection,
    rule_id: int,
    device_id: str,
    metric: str,
    event_type: str,
    kind: Optional[str],
    seq: int,
    value: float,
) -> str:
    event_time = utc_now_iso()
    conn.execute(
        "INSERT INTO alert_events (rule_id, device_id, metric, event_type,"
        " kind, seq, value, event_time) VALUES (?,?,?,?,?,?,?,?)",
        (rule_id, device_id, metric, event_type, kind, seq, value, event_time),
    )
    return event_time


def evaluate_package(
    conn: sqlite3.Connection,
    device_id: str,
    seq: int,
    payload_json: str,
) -> None:
    """按提交顺序评估一个已提交包中各指标的测量值。

    在摄入事务内调用：每个提交包恰好调用一次，重复包不会到达这里。
    一个包内同一指标出现多次时，按 measurements 的出现顺序逐个驱动状态机。
    """
    payload = json.loads(payload_json)
    measurements = payload.get("measurements", [])

    rules = conn.execute(
        "SELECT id, metric, upper, lower, trigger_count, recover_count"
        " FROM alert_rules WHERE device_id = ? AND enabled = 1",
        (device_id,),
    ).fetchall()
    if not rules:
        return

    # 指标 -> 该包内的测量值（按出现顺序）
    by_metric: Dict[str, List[float]] = {}
    for m in measurements:
        by_metric.setdefault(m["metric"], []).append(float(m["value"]))

    for rule in rules:
        values = by_metric.get(rule["metric"])
        if not values:
            continue
        state_row = conn.execute(
            "SELECT state, consecutive FROM alert_rule_states WHERE rule_id = ?",
            (rule["id"],),
        ).fetchone()
        state = state_row["state"]
        consecutive = state_row["consecutive"]

        # 本包内最近一次 normal->alarm 跃迁的信息（可能先恢复后再触发）
        last_trigger: Optional[tuple] = None
        last_value: float = values[-1]

        for value in values:
            kind = _violation_kind(value, rule["upper"], rule["lower"])
            if state == NORMAL:
                if kind is not None:
                    consecutive += 1
                    if consecutive >= rule["trigger_count"]:
                        event_time = _add_event(
                            conn, rule["id"], device_id, rule["metric"],
                            "triggered", kind, seq, value,
                        )
                        last_trigger = (kind, value, event_time)
                        state, consecutive = ALARM, 0
                else:
                    consecutive = 0
            else:  # ALARM
                if kind is None:
                    consecutive += 1
                    if consecutive >= rule["recover_count"]:
                        _add_event(
                            conn, rule["id"], device_id, rule["metric"],
                            "recovered", None, seq, value,
                        )
                        state, consecutive = NORMAL, 0
                else:
                    consecutive = 0

        if state == ALARM and last_trigger is not None:
            # 本包内（重新）进入告警：刷新触发信息
            kind, value, event_time = last_trigger
            conn.execute(
                "UPDATE alert_rule_states SET state = 'alarm', consecutive = ?,"
                " evaluated_upto = ?, trigger_kind = ?, trigger_value = ?,"
                " triggered_seq = ?, triggered_at = ?, last_seq = ?, last_value = ?"
                " WHERE rule_id = ?",
                (
                    consecutive, seq, kind, value, seq, event_time,
                    seq, last_value, rule["id"],
                ),
            )
        elif state == ALARM:
            # 进入本包前已是告警且仍未恢复：保留既有触发信息
            conn.execute(
                "UPDATE alert_rule_states SET state = 'alarm', consecutive = ?,"
                " evaluated_upto = ?, last_seq = ?, last_value = ? WHERE rule_id = ?",
                (consecutive, seq, seq, last_value, rule["id"]),
            )
        else:
            # normal：一直正常或本包内已恢复，触发信息清空（恢复时间见 recovered 事件）
            conn.execute(
                "UPDATE alert_rule_states SET state = 'normal', consecutive = ?,"
                " evaluated_upto = ?, trigger_kind = NULL, trigger_value = NULL,"
                " triggered_seq = NULL, triggered_at = NULL, last_seq = ?,"
                " last_value = ? WHERE rule_id = ?",
                (consecutive, seq, seq, last_value, rule["id"]),
            )


# --------------------------------------------------------------------------- #
# 查询
# --------------------------------------------------------------------------- #
def list_active_alerts(
    conn: sqlite3.Connection,
    device_id: Optional[str],
    metric: Optional[str],
    limit: int,
    offset: int,
) -> Tuple[int, List[Dict[str, Any]]]:
    where = ["s.state = 'alarm'"]
    params: List[Any] = []
    if device_id is not None:
        where.append("r.device_id = ?")
        params.append(device_id)
    if metric is not None:
        where.append("r.metric = ?")
        params.append(metric)
    clause = " WHERE " + " AND ".join(where)
    total = conn.execute(
        "SELECT COUNT(*) AS n FROM alert_rule_states s"
        " JOIN alert_rules r ON r.id = s.rule_id" + clause,
        params,
    ).fetchone()["n"]
    rows = conn.execute(
        "SELECT r.id AS rule_id, r.device_id, r.metric, r.upper, r.lower,"
        " 'alarm' AS state,"
        " s.trigger_kind, s.trigger_value, s.triggered_seq, s.triggered_at,"
        " s.consecutive, s.last_seq, s.last_value"
        " FROM alert_rule_states s JOIN alert_rules r ON r.id = s.rule_id"
        + clause + " ORDER BY s.triggered_at, r.device_id, r.metric"
        " LIMIT ? OFFSET ?",
        (*params, limit, offset),
    ).fetchall()
    return total, [dict(r) for r in rows]


def list_events(
    conn: sqlite3.Connection,
    device_id: str,
    metric: Optional[str],
    event_type: Optional[str],
    limit: int,
    offset: int,
) -> Tuple[int, List[sqlite3.Row]]:
    where = ["device_id = ?"]
    params: List[Any] = [device_id]
    if metric is not None:
        where.append("metric = ?")
        params.append(metric)
    if event_type is not None:
        where.append("event_type = ?")
        params.append(event_type)
    clause = " WHERE " + " AND ".join(where)
    total = conn.execute(
        "SELECT COUNT(*) AS n FROM alert_events" + clause, params
    ).fetchone()["n"]
    rows = conn.execute(
        "SELECT * FROM alert_events" + clause + " ORDER BY id LIMIT ? OFFSET ?",
        (*params, limit, offset),
    ).fetchall()
    return total, rows
