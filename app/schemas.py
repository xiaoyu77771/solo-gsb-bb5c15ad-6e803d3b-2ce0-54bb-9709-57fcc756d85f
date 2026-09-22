"""请求 / 响应的数据模型。"""

import math
from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, model_validator


class Measurement(BaseModel):
    metric: str = Field(min_length=1, description="指标名，如 temperature")
    value: float
    unit: Optional[str] = None
    ts: Optional[datetime] = None


class PackageIn(BaseModel):
    """仪器上报的一个批量测量包。"""

    device_id: str = Field(min_length=1)
    seq: int = Field(ge=1, description="包序号，按设备单调递增、连续")
    idempotency_key: Optional[str] = Field(
        default=None, description="幂等键；也可用 Idempotency-Key 请求头传入"
    )
    sent_at: Optional[datetime] = None
    measurements: List[Measurement] = Field(min_length=1)


class IngestResponse(BaseModel):
    status: Literal["committed", "buffered", "duplicate"]
    device_id: str
    seq: int
    committed_upto: int
    committed_seqs: List[int] = Field(
        default_factory=list, description="本次请求触发提交的全部序号（含排水）"
    )
    duplicate_reason: Optional[str] = None


class DeviceState(BaseModel):
    device_id: str
    committed_upto: int
    next_expected: int
    buffered_count: int
    committed_count: int
    duplicate_count: int


class GapsResponse(BaseModel):
    device_id: str
    committed_upto: int
    next_expected: int
    buffered_seqs: List[int]
    gaps: List[int] = Field(description="缓冲区最大序号之前缺失的序号")


class CommittedResponse(BaseModel):
    device_id: str
    committed_upto: int
    count: int
    seqs: List[int]


class DuplicateRecord(BaseModel):
    id: int
    device_id: str
    seq: Optional[int]
    idempotency_key: Optional[str]
    reason: str
    received_at: str


class DuplicatesResponse(BaseModel):
    device_id: str
    count: int
    records: List[DuplicateRecord]


class RollupWindow(BaseModel):
    """单个半开窗口 [start, end) 的聚合结果；空窗口 count=0 且统计值为 null。"""

    start: datetime
    end: datetime
    count: int
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None


class RollupResponse(BaseModel):
    """按事件时间（sent_at）的窗口聚合结果。"""

    model_config = {"populate_by_name": True}

    device_id: str
    metric: str
    from_: datetime = Field(alias="from", description="聚合范围起点（含）")
    to: datetime = Field(description="聚合范围终点（不含）")
    window_seconds: float
    allowed_lateness: float
    watermark: Optional[datetime] = Field(
        default=None,
        description="设备已提交数据的最大 sent_at 减 allowed_lateness；无数据时为 null",
    )
    late_count: int = Field(
        description="范围内 sent_at 早于水位的迟到样本数（不改写已关闭窗口）"
    )
    windows: List[RollupWindow] = Field(description="按 start 升序的全部窗口")


class RuleUpsert(BaseModel):
    """创建 / 更新阈值告警规则的请求体（更新为整体替换语义）。"""

    upper: Optional[float] = Field(default=None, description="上限；测量值 >= upper 视为越限")
    lower: Optional[float] = Field(default=None, description="下限；测量值 <= lower 视为越限")
    trigger_count: int = Field(
        default=1, ge=1, description="连续越限达到该次数后触发告警"
    )
    recover_count: int = Field(
        default=1, ge=1, description="告警中连续恢复达到该次数后解除告警"
    )
    enabled: bool = Field(default=True, description="是否启用；停用后不再评估新提交的包")

    @model_validator(mode="after")
    def _check_thresholds(self):
        for name in ("upper", "lower"):
            v = getattr(self, name)
            if v is not None and not math.isfinite(v):
                raise ValueError(f"{name} 必须是有限数值")
        if self.upper is None and self.lower is None:
            raise ValueError("upper 与 lower 至少提供一个")
        if self.upper is not None and self.lower is not None and self.lower >= self.upper:
            raise ValueError("lower 必须严格小于 upper")
        return self


class RuleResponse(BaseModel):
    id: int
    device_id: str
    metric: str
    upper: Optional[float] = None
    lower: Optional[float] = None
    trigger_count: int
    recover_count: int
    enabled: bool
    created_at: str
    updated_at: str
    # 状态机当前状态
    state: Literal["normal", "alarm"]
    consecutive: int
    evaluated_upto: int = Field(description="已评估到的最大提交序号")
    trigger_kind: Optional[Literal["upper", "lower"]] = None
    trigger_value: Optional[float] = None
    triggered_seq: Optional[int] = None
    triggered_at: Optional[str] = None
    last_seq: Optional[int] = None
    last_value: Optional[float] = None


class AlertEvent(BaseModel):
    id: int
    rule_id: Optional[int] = None
    device_id: str
    metric: str
    event_type: Literal["triggered", "recovered"]
    kind: Optional[Literal["upper", "lower"]] = None
    seq: int = Field(description="产生该事件的提交包序号")
    value: float
    event_time: str = Field(description="触发或恢复时间（UTC ISO 8601）")
    created_at: str


class AlertEventsResponse(BaseModel):
    device_id: str
    metric: Optional[str] = None
    count: int
    limit: int
    offset: int
    events: List[AlertEvent]


class ActiveAlert(BaseModel):
    rule_id: int
    device_id: str
    metric: str
    state: Literal["alarm"]
    trigger_kind: Literal["upper", "lower"]
    trigger_value: float
    triggered_seq: int = Field(description="触发事件的提交包序号")
    triggered_at: str = Field(description="触发时间（UTC ISO 8601）")
    consecutive: int = Field(description="告警中最近一次连续计数")
    last_seq: Optional[int] = None
    last_value: Optional[float] = None
    upper: Optional[float] = None
    lower: Optional[float] = None


class ActiveAlertsResponse(BaseModel):
    count: int
    limit: int
    offset: int
    alerts: List[ActiveAlert]
