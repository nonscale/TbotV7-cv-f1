from abc import ABC, abstractmethod
from typing import List, Dict, Any
import polars as pl

class BaseBroker(ABC):
    """
    모든 브로커 구현체가 따라야 하는 추상 기반 클래스.
    """

    @abstractmethod
    async def get_tickers(self) -> List[str]:
        """
        거래 가능한 모든 종목의 목록을 반환합니다.
        """
        pass

    @abstractmethod
    async def get_market_data_for_1st_scan(self, tickers: List[str]) -> pl.DataFrame:
        """
        1차 스캔을 위해 여러 종목의 현재 시점 데이터를 한 번에 가져옵니다.
        'open', 'high', 'low', 'close', 'volume', 'amount' 컬럼을 포함해야 합니다.
        """
        pass

    @abstractmethod
    async def get_ohlcv(
        self,
        ticker: str,
        timeframe: str = '1d',
        limit: int = 200
    ) -> pl.DataFrame:
        """
        특정 종목의 OHLCV(시가, 고가, 저가, 종가, 거래량) 시계열 데이터를 가져옵니다.
        prd.md의 '데이터 컬럼 표준'을 준수해야 합니다.
        """
        pass

    @abstractmethod
    async def get_current_price(self, ticker: str) -> float:
        """
        특정 종목의 현재가를 반환합니다.
        """
        pass

    @abstractmethod
    async def place_order(
        self,
        ticker: str,
        order_type: str,
        side: str,
        amount: float,
        price: float = None
    ) -> Dict[str, Any]:
        """
        매수 또는 매도 주문을 실행합니다.
        """
        pass

    @abstractmethod
    async def get_balance(self) -> Dict[str, Any]:
        """
        계좌 잔고 정보를 반환합니다.
        """
        pass
