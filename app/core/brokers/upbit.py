from typing import List, Dict, Any
import polars as pl
import logging
import pyupbit
import asyncio
from functools import partial

from app.core.config import settings
from .base import BaseBroker

logger = logging.getLogger(__name__)

# pyupbit의 동기 함수를 비동기적으로 실행하기 위한 래퍼
async def run_sync(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))


class UpbitBroker(BaseBroker):
    """
    Upbit 거래소와의 연동을 담당하는 브로커 구현체.
    """
    def __init__(self, api_key: str = None, api_secret: str = None):
        access_key = api_key or settings.UPBIT_API_KEY
        secret_key = api_secret or settings.UPBIT_API_SECRET

        has_credentials = "default" not in access_key and "default" not in secret_key

        try:
            self.upbit = pyupbit.Upbit(access_key, secret_key)
            if has_credentials:
                balance = self.upbit.get_balance("KRW")
                logger.info(f"UpbitBroker가 인증된 사용자로 초기화되었습니다. (잔고: {balance} KRW)")
            else:
                logger.info("UpbitBroker가 인증되지 않은 사용자(시세 조회용)로 초기화되었습니다.")
        except Exception as e:
            logger.error(f"Upbit 클라이언트 초기화 실패: {e}", exc_info=True)
            raise ConnectionError("Upbit API 키가 유효하지 않거나 연결에 실패했습니다.")

    async def get_tickers(self, fiat="KRW") -> List[str]:
        logger.info(f"Upbit {fiat} 마켓 종목 목록을 가져옵니다.")
        try:
            tickers = await run_sync(pyupbit.get_tickers, fiat=fiat)
            return tickers
        except Exception as e:
            logger.error(f"Upbit 종목 목록 조회 실패: {e}", exc_info=True)
            return []

    async def get_market_data_for_1st_scan(self, tickers: List[str], timeframe: str = 'day') -> pl.DataFrame:
        """
        1차 스캔을 위해 여러 종목의 현재 시점 데이터를 한 번에 효율적으로 가져옵니다.
        """
        logger.info(f"1차 스캔을 위해 {len(tickers)}개 종목의 시장 데이터를 가져옵니다.")

        async def fetch_one(ticker):
            try:
                # get_ohlcv(count=2)를 사용하여 현재가(종가)와 전일 거래대금을 가져옴
                df = await self.get_ohlcv(ticker, timeframe=timeframe, limit=2)
                if df.height > 1:
                    # 최신 행 선택, ticker 컬럼 추가
                    latest = df.tail(1).with_columns(pl.lit(ticker).alias("ticker"))
                    return latest
                return None
            except Exception as e:
                logger.warning(f"1차 스캔 데이터 조회 중 {ticker} 오류: {e}")
                return None

        tasks = [fetch_one(ticker) for ticker in tickers]
        results = await asyncio.gather(*tasks)

        valid_results = [res for res in results if res is not None and not res.is_empty()]

        if not valid_results:
            return pl.DataFrame()

        return pl.concat(valid_results)

    async def get_ohlcv(
        self,
        ticker: str,
        timeframe: str = 'day',
        limit: int = 200
    ) -> pl.DataFrame:
        logger.debug(f"{ticker}의 {timeframe} OHLCV 데이터를 가져옵니다 (최근 {limit}개).")
        try:
            pandas_df = await run_sync(pyupbit.get_ohlcv, ticker=ticker, interval=timeframe, count=limit)

            if pandas_df is None or pandas_df.empty:
                return pl.DataFrame()

            df = pl.from_pandas(pandas_df.reset_index().rename(columns={'index': 'timestamp'}))
            df = df.rename({"value": "amount"})
            return df
        except Exception as e:
            logger.error(f"{ticker} OHLCV 데이터 조회 실패: {e}", exc_info=True)
            return pl.DataFrame()

    async def get_current_price(self, ticker: str) -> float:
        try:
            price = await run_sync(pyupbit.get_current_price, ticker)
            return price if price is not None else 0.0
        except Exception as e:
            logger.error(f"{ticker} 현재가 조회 실패: {e}", exc_info=True)
            return 0.0

    async def place_order(
        self,
        ticker: str,
        order_type: str,
        side: str,
        amount: float,
        price: float = None
    ) -> Dict[str, Any]:
        logger.info(f"주문 실행: {ticker}, {side}, {order_type}, 수량/금액:{amount}, 가격:{price}")
        try:
            if side.lower() == 'buy':
                if order_type == 'market':
                    return await run_sync(self.upbit.buy_market_order, ticker, amount)
                else:
                    return await run_sync(self.upbit.buy_limit_order, ticker, price, amount)
            elif side.lower() == 'sell':
                if order_type == 'market':
                    return await run_sync(self.upbit.sell_market_order, ticker, amount)
                else:
                    return await run_sync(self.upbit.sell_limit_order, ticker, price, amount)
            else:
                raise ValueError("side는 'buy' 또는 'sell'이어야 합니다.")
        except Exception as e:
            logger.error(f"{ticker} 주문 실패: {e}", exc_info=True)
            return {"error": str(e)}

    async def get_balance(self) -> Dict[str, Any]:
        logger.info("전체 잔고를 가져옵니다.")
        try:
            all_balances = await run_sync(self.upbit.get_balances)
            return {"all_balances": all_balances}
        except Exception as e:
            logger.error(f"잔고 조회 실패: {e}", exc_info=True)
            return {"error": str(e)}
