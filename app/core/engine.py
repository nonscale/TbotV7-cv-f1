import polars as pl
import operator
import logging
from typing import Dict, Any, List, Callable

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 지원하는 연산자 정의
OPERATORS = {
    '+': (operator.add, 1),
    '-': (operator.sub, 1),
    '*': (operator.mul, 2),
    '/': (operator.truediv, 2),
    '>': (operator.gt, 0),
    '>=': (operator.ge, 0),
    '<': (operator.lt, 0),
    '<=': (operator.le, 0),
    '==': (operator.eq, 0),
    '!=': (operator.ne, 0),
    'AND': (operator.and_, -1),
    'OR': (operator.or_, -1)
}

class LogicParser:
    """
    Shunting-yard 알고리즘을 사용하여 복잡한 논리 및 산술 표현식을 파싱하고 평가합니다.
    데이터프레임 컨텍스트 내에서 Polars Expression을 생성하고 실행합니다.
    """
    def __init__(self, indicators: Dict[str, Callable], data: pl.DataFrame):
        self.indicators = indicators
        self.data = data
        self.variables: Dict[str, Any] = {}

    def _parse_tokens(self, expression: str) -> List[str]:
        """
        입력 문자열을 토큰 리스트로 변환합니다.
        (예: "trix(12) > 0" -> ["trix(12)", ">", "0"])
        """
        # 간단한 공백 기반 토큰화 (향후 정교한 파서로 개선 가능)
        return expression.split()

    def _shunting_yard(self, tokens: List[str]) -> List[Any]:
        """
        토큰 리스트를 후위 표기법(RPN)으로 변환합니다.
        """
        output_queue: List[Any] = []
        operator_stack: List[str] = []

        for token in tokens:
            if token.replace('.', '', 1).isdigit():  # 숫자 처리
                output_queue.append(pl.lit(float(token)))
            elif token in self.data.columns: # 기본 데이터 컬럼
                output_queue.append(pl.col(token))
            elif token.endswith(')'): # 함수(지표) 또는 변수.shift() 처리
                if '.' in token and 'shift' in token: # shift 함수 처리
                     # ex: 'trix12.shift(1)'
                    var_name, func_call = token.split('.', 1)
                    shift_period = int(func_call.strip('shift()'))
                    if var_name in self.variables:
                        # 변수에 저장된 Polars Expression에 shift 적용
                        output_queue.append(self.variables[var_name].shift(shift_period))
                    else:
                        raise ValueError(f"Shift를 적용할 변수 '{var_name}'를 찾을 수 없습니다.")
                else: # 일반 지표 함수 처리
                    # ex: 'trix(12)'
                    func_name, args_str = token.split('(', 1)
                    args_str = args_str[:-1]
                    args = [arg.strip() for arg in args_str.split(',')]
                    if func_name in self.indicators:
                        # 지표 함수를 호출하여 Polars Expression 생성
                        # **주의: 실제 지표 함수는 숫자 파라미터를 기대합니다.**
                        # 여기서는 문자열 인자를 float으로 변환합니다.
                        try:
                            converted_args = [float(a) for a in args]
                            indicator_expr = self.indicators[func_name](*converted_args)
                            output_queue.append(indicator_expr)
                        except (ValueError, TypeError) as e:
                             raise ValueError(f"지표 '{func_name}'의 인자 변환 중 오류: {e}. 인자: {args}")
                    else:
                        raise ValueError(f"알 수 없는 지표 함수: {func_name}")
            elif token in OPERATORS: # 연산자 처리
                while (operator_stack and operator_stack[-1] != '(' and
                       OPERATORS[operator_stack[-1]][1] >= OPERATORS[token][1]):
                    output_queue.append(operator_stack.pop())
                operator_stack.append(token)
            elif token == '(':
                operator_stack.append(token)
            elif token == ')':
                while operator_stack and operator_stack[-1] != '(':
                    output_queue.append(operator_stack.pop())
                if operator_stack and operator_stack[-1] == '(':
                    operator_stack.pop()
                else:
                    raise ValueError("괄호 쌍이 맞지 않습니다.")
            else: # 정의되지 않은 변수 참조
                if token in self.variables:
                     output_queue.append(self.variables[token])
                else:
                    raise ValueError(f"알 수 없는 토큰 또는 변수: {token}")


        while operator_stack:
            if operator_stack[-1] == '(':
                raise ValueError("괄호 쌍이 맞지 않습니다.")
            output_queue.append(operator_stack.pop())

        return output_queue

    def _evaluate_rpn(self, rpn_queue: List[Any]) -> pl.Expr:
        """
        후위 표기법(RPN) 큐를 평가하여 최종 Polars Expression을 생성합니다.
        """
        stack: List[Any] = []
        for token in rpn_queue:
            if token in OPERATORS:
                op_func, _ = OPERATORS[token]
                # 스택에서 피연산자를 꺼낼 때 순서에 주의 (오른쪽 -> 왼쪽)
                right = stack.pop()
                left = stack.pop()
                # Polars Expression을 사용하여 연산 수행
                stack.append(op_func(left, right))
            else:
                # 숫자(Literal)나 컬럼(Column), 지표 결과(Expression)를 스택에 추가
                stack.append(token)

        if len(stack) != 1:
            raise ValueError("표현식 평가에 실패했습니다. 최종 스택에 하나 이상의 항목이 남았습니다.")
        return stack[0]

    def evaluate(self, expression: str) -> pl.Series:
        """
        전체 표현식을 평가하고, 데이터프레임에 적용하여 결과(Boolean Series)를 반환합니다.
        """
        logger.info(f"표현식 평가 시작: {expression}")
        tokens = self._parse_tokens(expression)
        logger.debug(f"토큰화 결과: {tokens}")
        rpn_queue = self._shunting_yard(tokens)
        logger.debug(f"RPN 변환 결과: {rpn_queue}")
        final_expr = self._evaluate_rpn(rpn_queue)
        logger.debug(f"최종 Polars Expression: {final_expr}")

        # 데이터프레임에 최종 표현식을 적용하여 결과 계산
        result_df = self.data.with_columns(result=final_expr)
        return result_df.get_column("result")

    def set_variable(self, var_name: str, expression: str):
        """
        표현식의 결과를 계산하여 변수로 저장합니다.
        (예: 'trix12', 'trix(12)')
        """
        logger.info(f"변수 설정: {var_name} = {expression}")
        tokens = self._parse_tokens(expression)
        rpn_queue = self._shunting_yard(tokens)
        expr_to_save = self._evaluate_rpn(rpn_queue)
        self.variables[var_name] = expr_to_save
        logger.info(f"변수 '{var_name}'에 Expression 저장 완료: {expr_to_save}")


class ScanEngine:
    """
    정의된 전략에 따라 스캔을 수행하는 엔진.
    """
    def __init__(self, broker, indicators: Dict[str, Callable]):
        self.broker = broker
        self.indicators = indicators
        self.parser = None

    async def _fetch_data(self, market: str, timeframe: str, limit: int) -> pl.DataFrame:
        """
        브로커를 통해 OHLCV 데이터를 가져옵니다.
        """
        logger.info(f"{market} 시장의 {timeframe} 데이터 로딩 중 (최근 {limit}개)")
        # 여기서는 모든 종목에 대해 동일한 데이터를 가져온다고 가정합니다.
        # 실제로는 종목별로 데이터를 가져와야 합니다.
        df = await self.broker.get_ohlcv("KRW-BTC", timeframe, limit)
        return df

    async def run_scan(self, strategy: Dict[str, Any]) -> pl.DataFrame:
        """
        주어진 전략에 따라 스캔을 실행합니다.
        """
        logger.info(f"'{strategy['name']}' 전략 스캔 시작")
        
        # 1. 데이터 가져오기 (전략에 명시된 타임프레임 사용)
        # 지금은 단일 타임프레임만 지원
        timeframe = strategy.get("timeframe", "1d") # 기본값 '1d'
        ohlcv_df = await self._fetch_data("crypto", timeframe, 200) # 데이터 갯수 하드코딩

        if ohlcv_df.is_empty():
            logger.warning("브로커로부터 데이터를 가져오지 못했습니다. 스캔을 중단합니다.")
            return pl.DataFrame()

        # 2. LogicParser 초기화
        self.parser = LogicParser(self.indicators, ohlcv_df)

        # 3. 변수 설정 (있는 경우)
        if 'variables' in strategy:
            for var in strategy['variables']:
                self.parser.set_variable(var['name'], var['expression'])

        # 4. 조건 평가
        final_condition = strategy['condition']
        
        try:
            # 최종 조건을 평가하여 boolean 마스크 생성
            mask = self.parser.evaluate(final_condition)

            # 마스크를 적용하여 조건에 맞는 행 필터링
            results_df = ohlcv_df.filter(mask)
            
            logger.info(f"스캔 완료. {len(results_df)}개의 결과 발견.")
            return results_df

        except Exception as e:
            logger.error(f"스캔 중 오류 발생: {e}", exc_info=True)
            return pl.DataFrame() # 오류 발생 시 빈 데이터프레임 반환

# 사용 예시 (테스트용)
if __name__ == '__main__':
    # 가짜 브로커 및 지표 정의
    class MockBroker:
        async def get_ohlcv(self, ticker, timeframe, limit):
            # 실제 데이터 대신 테스트용 데이터프레임 생성
            return pl.DataFrame({
                "open": [100, 101, 102, 103, 104, 105],
                "high": [105, 106, 107, 108, 109, 110],
                "low": [99, 100, 101, 102, 103, 104],
                "close": [101, 102, 103, 104, 105, 108],
                "volume": [10, 20, 15, 25, 30, 22]
            })

    # 간단한 이동평균 지표 함수 (Polars Expression 반환)
    def moving_average(period: int):
        return pl.col('close').rolling_mean(window_size=period)

    mock_indicators = {"ma": moving_average}
    
    # 실행할 전략 정의
    sample_strategy = {
        "name": "MA Cross Test",
        "timeframe": "1d",
        "variables": [
            {"name": "ma_short", "expression": "ma(2)"},
            {"name": "ma_long", "expression": "ma(4)"}
        ],
        "condition": "ma_short > ma_long AND close > 105"
        # 복잡한 조건 예시: "( ma_short > ma_long AND close > 105 ) OR volume > 20"
    }

    # 스캔 엔진 실행
    async def main():
        engine = ScanEngine(MockBroker(), mock_indicators)
        results = await engine.run_scan(sample_strategy)
        print("--- 스캔 결과 ---")
        print(results)

    import asyncio
    asyncio.run(main())