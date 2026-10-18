"""Модели запросов REST (pydantic). Все строки ограничены шаблоном: имена стратегий, монет и версий попадают в SQL только параметрами, но проверяются и здесь."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

Token = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_.@-]{1,60}$")]
VersionName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9 _.@+()-]{1,60}$", strip_whitespace=True)]
Mode = Literal["live", "shadow", "all"]


class NewsParams(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    before_min: float = Field(5, ge=0, le=240)  # минут ДО новости
    after_min: float = Field(5, ge=0, le=240)  # минут ПОСЛЕ новости
    mode: Literal["touch", "span"] = "touch"  # touch: вход или выход в окне; span: позиция была открыта в окне
    impacts: list[Literal["high", "medium", "low"]] = ["high"]


class CompareSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    baseline: VersionName
    candidate: VersionName
    align: Literal["overlap", "relative"] = "overlap"


class RunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: Mode = "shadow"
    strategies: list[Token] | None = Field(None, max_length=20)
    coins: list[Token] | None = Field(None, max_length=100)
    since: datetime | None = None
    until: datetime | None = None
    news: NewsParams = NewsParams()
    compare: list[CompareSpec] = Field([], max_length=10)


class VersionBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: VersionName
    strategy_id: Token
    ts_from: datetime | None = None
    ts_to: datetime | None = None
    plan_hash: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{8}$|^noplan$")] | None = None
    meta_equals: dict[Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_.-]{1,40}$")], str | int | float | bool] = {}
    note: Annotated[str, StringConstraints(max_length=300)] = ""
    overwrite: bool = False


class DryRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_id: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{12}$")]
    recommendation_ids: list[Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{10}$")]] | None = None  # None - все
