from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services import strategy_service
from app.core.engine import ScanEngine
from app.core.brokers.upbit import UpbitBroker
from app.services.websocket_manager import manager
import json
import polars as pl
import asyncio

router = APIRouter()

# --- Mock/Temporary implementations ---
# TODO: 플러그인 시스템을 통해 동적으로 로드해야 합니다.
def moving_average(period: int):
    return pl.col('close').rolling_mean(window_size=period)
mock_indicators = {"ma": moving_average}

# TODO: Redis와 같은 견고한 캐시/메시지 큐로 교체해야 합니다.
watchlist_storage = {}
# --- End of Mock/Temporary implementations ---


async def broadcast_scan_result(strategy_name: str, result_df: pl.DataFrame):
    """Helper function to broadcast scan results via WebSocket."""
    if not result_df.is_empty():
        result_json = result_df.write_json(row_oriented=True)
        message = {
            "event": "scan_result_found",
            "payload": {
                "strategy_name": strategy_name,
                "results": json.loads(result_json)
            }
        }
        await manager.broadcast(json.dumps(message))
        print(f"'{strategy_name}' 스캔 결과 ({len(result_df)}개) WebSocket으로 전송 완료.")
    else:
        print(f"'{strategy_name}' 스캔 결과 없음.")


async def broadcast_watchlist(strategy_name: str, watchlist: list[str]):
    """Helper function to broadcast the watchlist via WebSocket."""
    message = {
        "event": "watchlist_updated",
        "payload": {
            "strategy_name": strategy_name,
            "watchlist": watchlist,
            "count": len(watchlist)
        }
    }
    await manager.broadcast(json.dumps(message))
    print(f"'{strategy_name}' 관심종목 ({len(watchlist)}개) WebSocket으로 전송 완료.")


def run_1st_scan_background(strategy_id: int):
    """백그라운드에서 1차 스캔을 실행합니다."""
    from app.db.session import SessionLocal
    db = SessionLocal()
    try:
        strategy = strategy_service.get_strategy(db, strategy_id=strategy_id)
        if not strategy:
            print(f"백그라운드 작업 오류: 전략 ID {strategy_id}를 찾을 수 없습니다.")
            return

        print(f"1차 백그라운드 스캔 시작: {strategy.name}")
        broker = UpbitBroker()
        engine = ScanEngine(broker=broker, indicators=mock_indicators)

        watchlist = asyncio.run(engine.run_1st_scan(strategy.scan_logic))

        watchlist_storage[strategy.id] = watchlist
        print(f"'{strategy.name}'의 1차 스캔 완료. 관심종목 {len(watchlist)}개 저장.")

        asyncio.run(broadcast_watchlist(strategy.name, watchlist))

    finally:
        db.close()


def run_2nd_scan_background(strategy_id: int):
    """백그라운드에서 2차 스캔을 실행합니다."""
    from app.db.session import SessionLocal
    db = SessionLocal()
    try:
        strategy = strategy_service.get_strategy(db, strategy_id=strategy_id)
        if not strategy:
            print(f"백그라운드 작업 오류: 전략 ID {strategy_id}를 찾을 수 없습니다.")
            return

        watchlist = watchlist_storage.get(strategy.id)
        if watchlist is None:
            print(f"'{strategy.name}'에 대한 2차 스캔을 시작할 수 없습니다. 먼저 1차 스캔을 실행해야 합니다.")
            # TODO: 사용자에게 에러를 알리는 WebSocket 메시지 전송
            return

        print(f"2차 백그라운드 스캔 시작: {strategy.name} (대상: {len(watchlist)}개)")
        broker = UpbitBroker()
        engine = ScanEngine(broker=broker, indicators=mock_indicators)

        results = asyncio.run(engine.run_2nd_scan(strategy.scan_logic, tickers=watchlist))

        asyncio.run(broadcast_scan_result(strategy.name, results))

    finally:
        db.close()


@router.post("/scans/{strategy_id}/run-1st", status_code=202)
def run_1st_strategy_scan(
    *,
    strategy_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    [1단계] 특정 전략에 대한 1차 스캔을 백그라운드에서 실행하여 '관심종목'을 생성합니다.
    """
    if not strategy_service.get_strategy(db, strategy_id=strategy_id):
        raise HTTPException(status_code=404, detail="Strategy not found")

    background_tasks.add_task(run_1st_scan_background, strategy_id)
    return {"message": "1st phase scan has been started in the background."}


@router.post("/scans/{strategy_id}/run-2nd", status_code=202)
def run_2nd_strategy_scan(
    *,
    strategy_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    [2단계] 생성된 '관심종목'을 바탕으로 2차 스캔을 실행하여 최종 결과를 도출합니다.
    """
    if not strategy_service.get_strategy(db, strategy_id=strategy_id):
        raise HTTPException(status_code=404, detail="Strategy not found")

    if strategy_id not in watchlist_storage:
         raise HTTPException(status_code=404, detail="Watchlist not found. Please run the 1st phase scan first.")

    background_tasks.add_task(run_2nd_scan_background, strategy_id)
    return {"message": "2nd phase scan has been started in the background."}
