"""阈值告警模块测试：规则 CRUD、迟滞状态机、重复幂等、乱序排水、查询分页。"""

import pytest

from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


def pkg(device, seq, key, values, metric="temperature"):
    """values: float / (metric, value) 列表。"""
    if isinstance(values, (int, float)):
        values = [values]
    measurements = []
    for v in values:
        if isinstance(v, tuple):
            measurements.append({"metric": v[0], "value": v[1]})
        else:
            measurements.append({"metric": metric, "value": v})
    return {
        "device_id": device,
        "seq": seq,
        "idempotency_key": key,
        "measurements": measurements,
    }


def post(client, device, seq, value, metric="temperature", key=None):
    return client.post(
        "/api/v1/packages",
        json=pkg(device, seq, key or f"{device}-{seq}", value, metric=metric),
    )


def make_rule(client, device="dev-1", metric="temperature", **kw):
    body = {"upper": 10.0, "lower": None, "trigger_count": 1, "recover_count": 1}
    body.update(kw)
    return client.post(
        "/api/v1/alert-rules", params={"device_id": device, "metric": metric}, json=body
    )


def events(client, device="dev-1", **params):
    return client.get(f"/api/v1/devices/{device}/alert-events", params=params)


def active(client, **params):
    return client.get("/api/v1/alerts", params=params)


# --------------------------------------------------------------------------- #
# 规则管理
# --------------------------------------------------------------------------- #
def test_rule_crud(client):
    r = make_rule(client, "dev-crud", "temperature", upper=30.0, lower=0.0,
                  trigger_count=2, recover_count=3)
    assert r.status_code == 201
    rule = r.json()
    assert rule["id"] > 0
    assert rule["state"] == "normal"
    assert rule["consecutive"] == 0
    assert rule["evaluated_upto"] == 0  # 设备尚无数据
    rid = rule["id"]

    # 同设备同指标重复创建 -> 409
    assert make_rule(client, "dev-crud", "temperature").status_code == 409

    # 单条查询与列表过滤
    assert client.get(f"/api/v1/alert-rules/{rid}").json()["upper"] == 30.0
    listing = client.get("/api/v1/alert-rules",
                         params={"device_id": "dev-crud", "metric": "temperature"}).json()
    assert [x["id"] for x in listing] == [rid]
    assert client.get("/api/v1/alert-rules", params={"device_id": "other"}).json() == []

    # 更新（整体替换；连续计数清零）
    r = client.put(f"/api/v1/alert-rules/{rid}", json={
        "upper": 25.0, "trigger_count": 3, "recover_count": 2, "enabled": False,
    })
    assert r.status_code == 200
    body = r.json()
    assert body["upper"] == 25.0 and body["lower"] is None
    assert body["trigger_count"] == 3 and body["enabled"] is False

    # 更新不存在的规则 -> 404
    assert client.put("/api/v1/alert-rules/9999", json={"upper": 1.0}).status_code == 404

    # 删除后 404；事件保留（见 test_delete_rule_keeps_events）
    assert client.delete(f"/api/v1/alert-rules/{rid}").status_code == 204
    assert client.delete(f"/api/v1/alert-rules/{rid}").status_code == 404
    assert client.get(f"/api/v1/alert-rules/{rid}").status_code == 404


def test_rule_validation(client):
    # 上下限都缺
    r = make_rule(client, "dev-val", "temperature", upper=None, lower=None)
    assert r.status_code == 422
    # lower >= upper
    r = make_rule(client, "dev-val", "temperature", upper=10.0, lower=10.0)
    assert r.status_code == 422
    # 连续次数必须 >= 1
    r = make_rule(client, "dev-val", "temperature", trigger_count=0)
    assert r.status_code == 422


def test_rule_created_after_history_does_not_backfill(client):
    # 先有历史提交
    post(client, "dev-hist", 1, 100.0)
    post(client, "dev-hist", 2, 100.0)
    # 再建规则：evaluated_upto 自举为 2，历史不回补
    r = make_rule(client, "dev-hist", "temperature", upper=10.0)
    assert r.json()["evaluated_upto"] == 2
    assert active(client, device_id="dev-hist").json()["alerts"] == []
    assert events(client, "dev-hist").json()["count"] == 0
    # 之后的新提交才评估
    post(client, "dev-hist", 3, 100.0)
    assert active(client, device_id="dev-hist").json()["count"] == 1


# --------------------------------------------------------------------------- #
# 状态机
# --------------------------------------------------------------------------- #
def test_upper_trigger_and_recover(client):
    make_rule(client, "dev-1", "temperature", upper=10.0)
    post(client, "dev-1", 1, 5.0)
    assert active(client, device_id="dev-1").json()["alerts"] == []

    post(client, "dev-1", 2, 10.0)  # value == upper 即越限
    alerts_body = active(client, device_id="dev-1").json()
    assert alerts_body["count"] == 1
    a = alerts_body["alerts"][0]
    assert a["device_id"] == "dev-1" and a["metric"] == "temperature"
    assert a["trigger_kind"] == "upper"
    assert a["trigger_value"] == 10.0
    assert a["triggered_seq"] == 2 and a["triggered_at"]

    ev = events(client).json()["events"]
    assert [(e["event_type"], e["kind"], e["seq"]) for e in ev] == [
        ("triggered", "upper", 2)
    ]

    # 限内 -> 恢复
    post(client, "dev-1", 3, 9.99)
    assert active(client, device_id="dev-1").json()["alerts"] == []
    ev = events(client).json()["events"]
    assert [e["event_type"] for e in ev] == ["triggered", "recovered"]
    rec = ev[-1]
    assert rec["seq"] == 3 and rec["event_time"] and rec["kind"] is None


def test_lower_threshold(client):
    make_rule(client, "dev-lo", "ph", upper=None, lower=6.0)
    post(client, "dev-lo", 1, 6.0, metric="ph")  # value == lower 即越限
    a = active(client, device_id="dev-lo").json()["alerts"]
    assert len(a) == 1 and a[0]["trigger_kind"] == "lower"


def test_consecutive_trigger_count_debounce(client):
    make_rule(client, "dev-ct", "temperature", upper=10.0, trigger_count=3)
    post(client, "dev-ct", 1, 11.0)
    post(client, "dev-ct", 2, 11.0)
    assert active(client, device_id="dev-ct").json()["count"] == 0
    # 中间一个正常值 -> 连续计数清零
    post(client, "dev-ct", 3, 1.0)
    post(client, "dev-ct", 4, 11.0)
    post(client, "dev-ct", 5, 11.0)
    assert active(client, device_id="dev-ct").json()["count"] == 0
    # 第三次连续越限才触发
    post(client, "dev-ct", 6, 11.0)
    body = active(client, device_id="dev-ct").json()
    assert body["count"] == 1
    assert body["alerts"][0]["triggered_seq"] == 6
    assert events(client, "dev-ct").json()["count"] == 1


def test_recover_count_hysteresis(client):
    make_rule(client, "dev-rc", "temperature", upper=10.0,
              trigger_count=1, recover_count=2)
    post(client, "dev-rc", 1, 11.0)  # 告警
    post(client, "dev-rc", 2, 1.0)   # 恢复计数 1，仍告警
    assert active(client, device_id="dev-rc").json()["count"] == 1
    post(client, "dev-rc", 3, 11.0)  # 再次越限 -> 恢复计数清零，告警保持
    assert events(client, "dev-rc").json()["count"] == 1  # 无重复触发事件
    rule = [r for r in client.get("/api/v1/alert-rules",
            params={"device_id": "dev-rc"}).json()][0]
    assert rule["state"] == "alarm" and rule["consecutive"] == 0
    post(client, "dev-rc", 4, 1.0)
    assert active(client, device_id="dev-rc").json()["count"] == 1
    post(client, "dev-rc", 5, 1.0)  # 连续第二次恢复 -> 解除
    assert active(client, device_id="dev-rc").json()["count"] == 0
    assert [e["event_type"] for e in events(client, "dev-rc").json()["events"]] == [
        "triggered", "recovered"
    ]


def test_update_rule_resets_streak_keeps_state(client):
    make_rule(client, "dev-upd", "temperature", upper=10.0, trigger_count=3)
    post(client, "dev-upd", 1, 11.0)
    post(client, "dev-upd", 2, 11.0)
    rid = client.get("/api/v1/alert-rules",
                     params={"device_id": "dev-upd"}).json()[0]["id"]
    # 改为 trigger_count=1，连续计数应清零
    client.put(f"/api/v1/alert-rules/{rid}", json={"upper": 10.0, "trigger_count": 1})
    rule = client.get(f"/api/v1/alert-rules/{rid}").json()
    assert rule["consecutive"] == 0 and rule["state"] == "normal"
    post(client, "dev-upd", 3, 11.0)
    assert active(client, device_id="dev-upd").json()["count"] == 1


def test_disabled_rule_not_evaluated(client):
    r = make_rule(client, "dev-dis", "temperature", upper=10.0, enabled=False)
    rid = r.json()["id"]
    post(client, "dev-dis", 1, 99.0)
    post(client, "dev-dis", 2, 99.0)
    assert active(client, device_id="dev-dis").json()["count"] == 0
    assert events(client, "dev-dis").json()["count"] == 0
    # 重新启用后继续评估（evaluated_upto 跟踪提交序号，跳过的包不补评估）
    client.put(f"/api/v1/alert-rules/{rid}",
               json={"upper": 10.0, "enabled": True})
    post(client, "dev-dis", 3, 99.0)
    assert active(client, device_id="dev-dis").json()["count"] == 1


# --------------------------------------------------------------------------- #
# 幂等与乱序
# --------------------------------------------------------------------------- #
def test_duplicate_packages_do_not_duplicate_events(client):
    make_rule(client, "dev-dup", "temperature", upper=10.0,
              trigger_count=2, recover_count=1)
    p1 = pkg("dev-dup", 1, "dup-1", 11.0)
    p2 = pkg("dev-dup", 2, "dup-2", 11.0)
    client.post("/api/v1/packages", json=p1)
    client.post("/api/v1/packages", json=p2)  # 触发告警
    # 重放两个包（同键）-> duplicate，不产生新事件
    client.post("/api/v1/packages", json=p1)
    client.post("/api/v1/packages", json=p2)
    # 换键重发已提交序号 -> already_committed，同样不产生事件
    post(client, "dev-dup", 1, 11.0, key="dup-1-again")
    assert events(client, "dev-dup").json()["count"] == 1
    # 恢复后再重放越限包，不能重新触发
    post(client, "dev-dup", 3, 1.0)  # 恢复
    client.post("/api/v1/packages", json=p2)
    assert [e["event_type"] for e in events(client, "dev-dup").json()["events"]] == [
        "triggered", "recovered"
    ]
    assert active(client, device_id="dev-dup").json()["count"] == 0


def test_out_of_order_drain_evaluated_once_in_seq_order(client):
    make_rule(client, "dev-ooo", "temperature", upper=10.0, trigger_count=2)
    # 首包自举：seq 2 先到并立即提交（committed_upto=2），仅评估 seq 2
    post(client, "dev-ooo", 2, 11.0, key="ooo-2")
    assert events(client, "dev-ooo").json()["count"] == 0  # 计数 1/2
    # seq 4 缓冲（缺口 3，不评估）
    post(client, "dev-ooo", 4, 11.0, key="ooo-4")
    assert events(client, "dev-ooo").json()["count"] == 0
    rule = client.get("/api/v1/alert-rules",
                      params={"device_id": "dev-ooo"}).json()[0]
    assert rule["evaluated_upto"] == 2  # 缓冲包未评估
    # seq 3 到达 -> 排水 3、4，按 seq 升序各评估一次：连续第 2 次越限在 seq3 触发
    post(client, "dev-ooo", 3, 11.0, key="ooo-3")
    body = client.get("/api/v1/devices/dev-ooo/gaps").json()
    assert body["buffered_seqs"] == []
    a = active(client, device_id="dev-ooo").json()["alerts"]
    assert len(a) == 1 and a[0]["triggered_seq"] == 3
    evs = events(client, "dev-ooo").json()["events"]
    assert [(e["event_type"], e["seq"]) for e in evs] == [("triggered", 3)]
    # 重放 seq 3、4（换键，already_committed）不产生重复事件
    post(client, "dev-ooo", 3, 11.0, key="ooo-3-replay")
    post(client, "dev-ooo", 4, 11.0, key="ooo-4-replay")
    assert events(client, "dev-ooo").json()["count"] == 1


def test_out_of_order_gap_fill_single_submission_order(client):
    """缺口包晚到：缓冲包只在补齐排水时按序评估一次。"""
    make_rule(client, "dev-gap", "temperature", upper=10.0, trigger_count=2)
    post(client, "dev-gap", 1, 11.0, key="gap-1")
    post(client, "dev-gap", 3, 11.0, key="gap-3")  # 缓冲，不评估
    assert events(client, "dev-gap").json()["count"] == 0
    rule = client.get("/api/v1/alert-rules",
                      params={"device_id": "dev-gap"}).json()[0]
    assert rule["evaluated_upto"] == 1 and rule["consecutive"] == 1
    # 补齐 seq 2 -> 排水 2、3，按序评估：seq2 为连续第 2 次越限 -> 触发
    post(client, "dev-gap", 2, 11.0, key="gap-2")
    evs = events(client, "dev-gap").json()["events"]
    assert [(e["seq"], e["event_type"]) for e in evs] == [(2, "triggered")]


# --------------------------------------------------------------------------- #
# 查询
# --------------------------------------------------------------------------- #
def test_events_pagination_and_filters(client):
    make_rule(client, "dev-pg", "temperature", upper=10.0)
    # 触发 -> 恢复 -> 触发 -> 恢复：4 个事件
    for seq, v in [(1, 11.0), (2, 1.0), (3, 11.0), (4, 1.0)]:
        post(client, "dev-pg", seq, v, key=f"pg-{seq}")
    body = events(client, "dev-pg", limit=2, offset=0).json()
    assert body["count"] == 4 and len(body["events"]) == 2
    first_ids = [e["id"] for e in body["events"]]
    body2 = events(client, "dev-pg", limit=2, offset=2).json()
    second_ids = [e["id"] for e in body2["events"]]
    # 分页按自增 id 升序、不重不漏
    assert len(set(first_ids + second_ids)) == 4
    assert first_ids[1] < second_ids[0]
    # 类型过滤
    body = events(client, "dev-pg", type="triggered").json()
    assert body["count"] == 2 and all(e["event_type"] == "triggered" for e in body["events"])
    body = events(client, "dev-pg", type="recovered").json()
    assert body["count"] == 2 and all(e["event_type"] == "recovered" for e in body["events"])
    # 非法类型 -> 422
    assert events(client, "dev-pg", type="bogus").status_code == 422
    # 未知设备 -> 404
    assert events(client, "no-such-device").status_code == 404


def test_events_filter_by_metric_and_multiple_rules(client):
    make_rule(client, "dev-mm", "temperature", upper=10.0)
    make_rule(client, "dev-mm", "ph", upper=None, lower=6.0)
    client.post("/api/v1/packages", json=pkg(
        "dev-mm", 1, "mm-1", [("temperature", 11.0), ("ph", 5.0)]))
    body = events(client, "dev-mm").json()
    assert body["count"] == 2
    body = events(client, "dev-mm", metric="ph").json()
    assert body["count"] == 1
    assert body["events"][0]["metric"] == "ph"
    assert body["events"][0]["kind"] == "lower"
    # 当前告警可按设备 / 指标过滤
    assert active(client, device_id="dev-mm", metric="ph").json()["count"] == 1
    assert active(client, metric="temperature").json()["count"] >= 1


def test_delete_rule_keeps_events(client):
    make_rule(client, "dev-del", "temperature", upper=10.0)
    post(client, "dev-del", 1, 99.0)
    rid = client.get("/api/v1/alert-rules",
                     params={"device_id": "dev-del"}).json()[0]["id"]
    client.delete(f"/api/v1/alert-rules/{rid}")
    evs = events(client, "dev-del").json()["events"]
    assert len(evs) == 1 and evs[0]["rule_id"] is None
    assert active(client, device_id="dev-del").json()["alerts"] == []


def test_multiple_measurements_in_package_evaluated_in_order(client):
    # trigger_count=2：同一包内两次越限即触发
    make_rule(client, "dev-multi", "temperature", upper=10.0, trigger_count=2)
    client.post("/api/v1/packages", json=pkg(
        "dev-multi", 1, "multi-1", [11.0, 12.0]))
    a = active(client, device_id="dev-multi").json()["alerts"]
    assert len(a) == 1 and a[0]["triggered_seq"] == 1 and a[0]["trigger_value"] == 12.0
    # 包内顺序驱动：越限、正常、越限 -> 计数不连续，不触发
    make_rule(client, "dev-multi2", "temperature", upper=10.0, trigger_count=2)
    client.post("/api/v1/packages", json=pkg(
        "dev-multi2", 1, "multi2-1", [11.0, 1.0, 11.0]))
    assert active(client, device_id="dev-multi2").json()["count"] == 0
