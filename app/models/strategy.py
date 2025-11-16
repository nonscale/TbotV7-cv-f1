from sqlalchemy import Column, Integer, String, Boolean, DateTime, JSON
from sqlalchemy.sql import func
from app.db.session import Base
from pydantic import BaseModel, ConfigDict
from typing import Optional, Dict, Any
import datetime

# ==================================
# SQLAlchemy Model
# ==================================

class Strategy(Base):
    """
    데이터베이스에 저장되는 전략 정보를 나타내는 SQLAlchemy 모델.
    """
    __tablename__ = "strategies"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True, nullable=False, unique=True)

    # PRD 요구사항 반영: '명시적 컨텍스트' 필드 추가
    broker = Column(String, nullable=False)
    market = Column(String, nullable=False)

    description = Column(String, nullable=True)
    scan_logic = Column(JSON, nullable=False)
    is_active = Column(Boolean, default=False)
    cron_schedule = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

# ==================================
# Pydantic Schemas (API 데이터 검증용)
# ==================================

# Pydantic V2에서는 orm_mode 대신 ConfigDict(from_attributes=True)를 사용합니다.
# 이 설정을 통해 SQLAlchemy 모델 객체를 Pydantic 스키마로 자동으로 변환할 수 있습니다.

class StrategyBase(BaseModel):
    """
    전략 생성 및 수정을 위한 기본 필드를 정의하는 Pydantic 스키마.
    """
    name: str

    # PRD 요구사항 반영: '명시적 컨텍스트' 필드 추가
    broker: str
    market: str

    description: Optional[str] = None
    scan_logic: Dict[str, Any]
    is_active: bool = False
    cron_schedule: Optional[str] = None

class StrategyCreate(StrategyBase):
    """
    새로운 전략을 생성할 때 사용하는 스키마.
    """
    pass

class StrategyUpdate(BaseModel):
    """
    기존 전략을 업데이트할 때 사용하는 스키마. 모든 필드는 선택 사항입니다.
    """
    name: Optional[str] = None
    broker: Optional[str] = None
    market: Optional[str] = None
    description: Optional[str] = None
    scan_logic: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None
    cron_schedule: Optional[str] = None

class StrategySchema(StrategyBase):
    """
    API 응답으로 클라이언트에게 반환될 때 사용하는 스키마.
    데이터베이스의 모든 필드를 포함합니다.
    """
    id: int
    created_at: datetime.datetime
    updated_at: Optional[datetime.datetime] = None

    model_config = ConfigDict(
        from_attributes=True,
    )
