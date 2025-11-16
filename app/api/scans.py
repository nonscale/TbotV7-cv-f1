from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services import strategy_service
from app.core.engine import ScanEngine
from app.core.brokers.upbit import UpbitBroker # 실제 브로커 사용
from app.services.websocket_manager import manager
import json

router = APIRouter()

# 임시: 지표(indicator) 플러그인 로딩. 실제로는 플러그인 시스템을 통해 동적으로 로드해야 합니다.
# 여기서는 ScanEngine 테스트를 위해 간단한 ma 함수를 정의합니다.
import polars as pl
def moving_average(period: int):
    return pl.col('close').rolling_mean(window_size=period)

mock_indicators = {"ma": moving_average}


def run_scan_background(strategy_id: int):
    """백그라운드에서 스캔을 실행하는 실제 작업 함수"""
    # 백그라운드 작업에서는 새로운 DB 세션을 생성해야 합니다.
    from app.db.session import SessionLocal
    db = SessionLocal()

    try:
        strategy = strategy_service.get_strategy(db, strategy_id=strategy_id)
        if not strategy:
            print(f"백그라운드 작업: 전략 ID {strategy_id}를 찾을 수 없습니다.")
            return

        print(f"백그라운드 스캔 시작: {strategy.name}")

        # 1. 브로커 및 스캔 엔진 초기화
        broker = UpbitBroker()
        engine = ScanEngine(broker=broker, indicators=mock_indicators)

        # 2. 스캔 실행
        # ScanEngine의 run_scan을 비동기로 직접 호출하는 대신,
        # asyncio.run()을 사용하여 동기 함수 내에서 비동기 작업을 실행합니다.
        import asyncio

        # 스캔 결과를 실시간으로 처리하기 위한 콜백 함수
        async def result_callback(result_df: pl.DataFrame):
            if not result_df.is_empty():
                # Polars DataFrame을 JSON으로 변환하여 브로드캐스트
                result_json = result_df.write_json(row_oriented=True)
                message = {
                    "event": "scan_result_found",
                    "payload": {
                        "strategy_name": strategy.name,
                        "results": json.loads(result_json)
                    }
                }
                await manager.broadcast(json.dumps(message))

        # ScanEngine의 run_scan에 콜백을 전달하도록 수정이 필요함 (향후)
        # 현재는 run_scan이 끝난 후 결과를 한 번에 전송
        results = asyncio.run(engine.run_scan(strategy.scan_logic, tickers=["KRW-BTC", "KRW-ETH"])) # 테스트용 티커

        if not results.is_empty():
            print(f"스캔 결과 발견 ({len(results)}개), WebSocket으로 전송합니다.")
            asyncio.run(result_callback(results))
        else:
            print("스캔 결과 없음.")

    finally:
        db.close()


@router.post("/scans/{strategy_id}/run", status_code=202)
def run_strategy_scan(
    *,
    strategy_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    특정 전략에 대한 스캔을 백그라운드에서 실행합니다.
    """
    strategy = strategy_service.get_strategy(db, strategy_id=strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")

    # 백그라운드 작업 추가
    background_tasks.add_task(run_scan_background, strategy.id)

    return {"message": "Scan has been started in the background."}
