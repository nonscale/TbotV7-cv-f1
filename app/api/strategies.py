from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from app.db.session import get_db
from app.models.strategy import StrategyCreate, StrategyUpdate, StrategySchema
from app.services import strategy_service

router = APIRouter()

@router.post("/strategies", response_model=StrategySchema, status_code=201)
def create_strategy_endpoint(
    *,
    db: Session = Depends(get_db),
    strategy_in: StrategyCreate
):
    """
    새로운 전략을 생성합니다.
    """
    strategy = strategy_service.create_strategy(db=db, strategy=strategy_in)
    return strategy

@router.get("/strategies", response_model=List[StrategySchema])
def read_strategies_endpoint(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """
    전략 목록을 조회합니다.
    """
    strategies = strategy_service.get_strategies(db, skip=skip, limit=limit)
    return strategies

@router.get("/strategies/{strategy_id}", response_model=StrategySchema)
def read_strategy_endpoint(
    *,
    db: Session = Depends(get_db),
    strategy_id: int,
):
    """
    ID로 특정 전략을 조회합니다.
    """
    strategy = strategy_service.get_strategy(db, strategy_id=strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return strategy

@router.put("/strategies/{strategy_id}", response_model=StrategySchema)
def update_strategy_endpoint(
    *,
    db: Session = Depends(get_db),
    strategy_id: int,
    strategy_in: StrategyUpdate,
):
    """
    기존 전략을 수정합니다.
    """
    strategy = strategy_service.get_strategy(db, strategy_id=strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")
    strategy = strategy_service.update_strategy(db=db, strategy_id=strategy_id, strategy_update=strategy_in)
    return strategy

@router.delete("/strategies/{strategy_id}", response_model=StrategySchema)
def delete_strategy_endpoint(
    *,
    db: Session = Depends(get_db),
    strategy_id: int,
):
    """
    전략을 삭제합니다.
    """
    strategy = strategy_service.get_strategy(db, strategy_id=strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")
    strategy = strategy_service.delete_strategy(db=db, strategy_id=strategy_id)
    return strategy
