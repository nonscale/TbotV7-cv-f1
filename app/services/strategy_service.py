from sqlalchemy.orm import Session
from typing import List, Optional

from app.models.strategy import Strategy, StrategyCreate, StrategyUpdate

def get_strategy(db: Session, strategy_id: int) -> Optional[Strategy]:
    """ID로 특정 전략을 조회합니다."""
    return db.query(Strategy).filter(Strategy.id == strategy_id).first()

def get_strategies(db: Session, skip: int = 0, limit: int = 100) -> List[Strategy]:
    """모든 전략의 목록을 조회합니다."""
    return db.query(Strategy).offset(skip).limit(limit).all()

def create_strategy(db: Session, strategy: StrategyCreate) -> Strategy:
    """새로운 전략을 생성합니다."""
    db_strategy = Strategy(
        name=strategy.name,
        description=strategy.description,
        scan_logic=strategy.scan_logic,
        is_active=strategy.is_active,
        cron_schedule=strategy.cron_schedule
    )
    db.add(db_strategy)
    db.commit()
    db.refresh(db_strategy)
    return db_strategy

def update_strategy(db: Session, strategy_id: int, strategy_update: StrategyUpdate) -> Optional[Strategy]:
    """기존 전략을 수정합니다."""
    db_strategy = get_strategy(db, strategy_id)
    if db_strategy:
        update_data = strategy_update.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(db_strategy, key, value)
        db.commit()
        db.refresh(db_strategy)
    return db_strategy

def delete_strategy(db: Session, strategy_id: int) -> Optional[Strategy]:
    """전략을 삭제합니다."""
    db_strategy = get_strategy(db, strategy_id)
    if db_strategy:
        db.delete(db_strategy)
        db.commit()
    return db_strategy
