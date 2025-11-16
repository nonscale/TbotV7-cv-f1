from sqlalchemy import Column, Integer, String, Boolean, DateTime, JSON
from sqlalchemy.sql import func
from app.db.session import Base
from pydantic import BaseModel, ConfigDict
from typing import Optional, List, Dict, Any
import datetime

# ==================================
# SQLAlchemy Model
# ==================================

class Strategy(Base):
    __tablename__ = "strategies"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True, nullable=False)
    description = Column(String, nullable=True)

    # 2단계 스캔 로직을 JSON 형태로 저장
    # 예: {"1st_scan": [...], "2nd_scan": [...]}
    scan_logic = Column(JSON, nullable=False)

    # 스케줄링 설정
    is_active = Column(Boolean, default=False)
    cron_schedule = Column(String, nullable=True) # 예: "*/5 * * * *" (5분마다)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


# ==================================
# Pydantic Schemas (for Pydantic V2)
# ==================================

class StrategyBase(BaseModel):
    name: str
    description: Optional[str] = None
    scan_logic: Dict[str, Any] # 전략 빌더의 출력을 그대로 저장
    is_active: bool = False
    cron_schedule: Optional[str] = None

class StrategyCreate(StrategyBase):
    pass

class StrategyUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    scan_logic: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None
    cron_schedule: Optional[str] = None

class StrategyInDB(StrategyBase):
    id: int
    created_at: datetime.datetime
    updated_at: Optional[datetime.datetime] = None

    # Pydantic V2에서는 orm_mode 대신 from_attributes를 사용
    model_config = ConfigDict(from_attributes=True)
