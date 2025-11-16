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

        # API 키가 제공되었는지 확인
        has_credentials = "default" not in access_key and "default" not in secret_key

        try:
            self.upbit = pyupbit.Upbit(access_key, secret_key)
            if has_credentials:
                # 인증된 클라이언트의 경우, 잔고 조회를 통해 키 유효성 검사
                balance = self.upbit.get_balance("KRW")
                logger.info(f"UpbitBroker가 인증된 사용자로 초기화되었습니다. (잔고: {balance} KRW)")
            else:
                logger.info("UpbitBroker가 인증되지 않은 사용자(시세 조회용)로 초기화되었습니다.")
        except Exception as e:
            logger.error(f"Upbit 클라이언트 초기화 실패: {e}", exc_info=True)
            raise ConnectionError("Upbit API 키가 유효하지 않거나 연결에 실패했습니다.")


    async def get_tickers(self, fiat="KRW") -> List[str]:
        """
        Upbit에서 거래 가능한 모든 KRW 마켓의 종목 목록을 반환합니다.
        """
        logger.info(f"Upbit {fiat} 마켓 종목 목록을 가져옵니다.")
        try:
            tickers = await run_sync(pyupbit.get_tickers, fiat=fiat)
            return tickers
        except Exception as e:
            logger.error(f"Upbit 종목 목록 조회 실패: {e}", exc_info=True)
            return []

    async def get_ohlcv(
        self,
        ticker: str,
        timeframe: str = 'day',
        limit: int = 200
    ) -> pl.DataFrame:
        """
        Upbit API를 통해 특정 종목의 OHLCV 데이터를 가져옵니다.
        prd.md의 '데이터 컬럼 표준'을 준수하여 컬럼명을 변환합니다.
        """
        logger.info(f"{ticker}의 {timeframe} OHLCV 데이터를 가져옵니다 (최근 {limit}개).")
        try:
            # pyupbit.get_ohlcv는 'count' 인자를 사용합니다.
            pandas_df = await run_sync(pyupbit.get_ohlcv, ticker=ticker, interval=timeframe, count=limit)

            if pandas_df is None or pandas_df.empty:
                logger.warning(f"{ticker}에 대한 OHLCV 데이터를 가져오지 못했습니다.")
                return pl.DataFrame()

            # Pandas DataFrame을 Polars DataFrame으로 변환
            df = pl.from_pandas(pandas_df.reset_index().rename(columns={'index': 'timestamp'}))

            # 'prd.md' 데이터 컬럼 표준에 맞게 컬럼명 변경 및 추가
            # pyupbit 컬럼: open, high, low, close, volume, value
            df = df.rename({
                "value": "amount"  # 'value'를 'amount'로 변경
            })

            return df

        except Exception as e:
            logger.error(f"{ticker} OHLCV 데이터 조회 실패: {e}", exc_info=True)
            return pl.DataFrame()

    async def get_current_price(self, ticker: str) -> float:
        """
        Upbit API를 통해 특정 종목의 현재가를 가져옵니다.
        """
        logger.info(f"{ticker}의 현재가를 가져옵니다.")
        try:
            price = await run_sync(pyupbit.get_current_price, ticker)
            return price if price is not None else 0.0
        except Exception as e:
            logger.error(f"{ticker} 현재가 조회 실패: {e}", exc_info=True)
            return 0.0

    async def place_order(
        self,
        ticker: str,
        side: str, # 'buy' or 'sell'
        price: float,
        amount: float,
        order_type: str = 'limit', # 'limit' (지정가), 'market' (시장가)
    ) -> Dict[str, Any]:
        """
        Upbit에 주문을 실행합니다.
        - side: 'buy' (매수), 'sell' (매도)
        - price: 주문 가격
        - amount: 주문 수량
        """
        logger.info(f"주문 실행: {ticker}, {side}, {order_type}, 수량:{amount}, 가격:{price}")
        try:
            if side.lower() == 'buy':
                # 시장가 매수의 경우 price는 총 주문액
                if order_type == 'market':
                    return await run_sync(self.upbit.buy_market_order, ticker, price * amount)
                else:
                    return await run_sync(self.upbit.buy_limit_order, ticker, price, amount)
            elif side.lower() == 'sell':
                 # 시장가 매도의 경우 amount는 주문 수량
                if order_type == 'market':
                    return await run_sync(self.upbit.sell_market_order, ticker, amount)
                else:
                    return await run_sync(self.upbit.sell_limit_order, ticker, price, amount)
            else:
                raise ValueError("side는 'buy' 또는 'sell'이어야 합니다.")
        except Exception as e:
            logger.error(f"{ticker} 주문 실패: {e}", exc_info=True)
            return {"error": str(e)}

    async def get_balance(self, ticker: str = "KRW") -> Dict[str, Any]:
        """
        Upbit 계좌 잔고 정보를 가져옵니다.
        """
        logger.info(f"{ticker} 잔고를 가져옵니다.")
        try:
            # get_balance는 단일 티커에 대한 잔고를 반환 (float)
            balance = await run_sync(self.upbit.get_balance, ticker)
            # get_balances는 전체 잔고 목록을 반환
            all_balances = await run_sync(self.upbit.get_balances)
            return {
                "ticker": ticker,
                "balance": balance,
                "all_balances": all_balances
            }
        except Exception as e:
            logger.error(f"잔고 조회 실패: {e}", exc_info=True)
            return {"error": str(e)}
