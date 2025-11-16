import polars as pl
import operator
import logging
from typing import Dict, Any, List, Callable

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ... (LogicParser 클래스는 변경 없음) ...
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
        return expression.split()

    def _shunting_yard(self, tokens: List[str]) -> List[Any]:
        output_queue: List[Any] = []
        operator_stack: List[str] = []
        OPERATORS = {
            '+': 1, '-': 1, '*': 2, '/': 2, '>': 0, '>=': 0, '<': 0, '<=': 0,
            '==': 0, '!=': 0, 'AND': -1, 'OR': -1
        }
        for token in tokens:
            if token.replace('.', '', 1).isdigit():
                output_queue.append(pl.lit(float(token)))
            elif token in self.data.columns:
                output_queue.append(pl.col(token))
            elif token.endswith(')'):
                if '.' in token and 'shift' in token:
                    var_name, func_call = token.split('.', 1)
                    shift_period = int(func_call.strip('shift()'))
                    if var_name in self.variables:
                        output_queue.append(self.variables[var_name].shift(shift_period))
                    else:
                        raise ValueError(f"Unknown variable for shift: {var_name}")
                else:
                    func_name, args_str = token.split('(', 1)
                    args = [a.strip() for a in args_str[:-1].split(',')]
                    if func_name in self.indicators:
                        try:
                            converted_args = [float(a) for a in args if a]
                            output_queue.append(self.indicators[func_name](*converted_args))
                        except (ValueError, TypeError) as e:
                            raise ValueError(f"Error converting args for {func_name}: {e}")
                    else:
                        raise ValueError(f"Unknown indicator function: {func_name}")
            elif token in OPERATORS:
                while (operator_stack and operator_stack[-1] != '(' and
                       OPERATORS.get(operator_stack[-1], 0) >= OPERATORS[token]):
                    output_queue.append(operator_stack.pop())
                operator_stack.append(token)
            elif token == '(':
                operator_stack.append(token)
            elif token == ')':
                while operator_stack and operator_stack[-1] != '(':
                    output_queue.append(operator_stack.pop())
                if operator_stack: operator_stack.pop()
                else: raise ValueError("Mismatched parentheses")
            elif token in self.variables:
                output_queue.append(self.variables[token])
            else:
                raise ValueError(f"Unknown token: {token}")

        while operator_stack:
            if operator_stack[-1] == '(': raise ValueError("Mismatched parentheses")
            output_queue.append(operator_stack.pop())
        return output_queue

    def _evaluate_rpn(self, rpn_queue: List[Any]) -> pl.Expr:
        stack: List[Any] = []
        OPERATOR_FUNCS = {
            '+': operator.add, '-': operator.sub, '*': operator.mul, '/': operator.truediv,
            '>': operator.gt, '>=': operator.ge, '<': operator.lt, '<=': operator.le,
            '==': operator.eq, '!=': operator.ne, 'AND': operator.and_, 'OR': operator.or_
        }
        for token in rpn_queue:
            if token in OPERATOR_FUNCS:
                right = stack.pop()
                left = stack.pop()
                stack.append(OPERATOR_FUNCS[token](left, right))
            else:
                stack.append(token)
        if len(stack) != 1: raise ValueError("Invalid expression")
        return stack[0]

    def evaluate(self, expression: str) -> pl.Series:
        tokens = self._parse_tokens(expression)
        rpn_queue = self._shunting_yard(tokens)
        final_expr = self._evaluate_rpn(rpn_queue)
        return self.data.with_columns(result=final_expr).get_column("result")

    def set_variable(self, var_name: str, expression: str):
        tokens = self._parse_tokens(expression)
        rpn_queue = self._shunting_yard(tokens)
        expr_to_save = self._evaluate_rpn(rpn_queue)
        self.variables[var_name] = expr_to_save


class ScanEngine:
    """
    정의된 전략에 따라 여러 티커에 대해 스캔을 수행하는 엔진.
    """
    def __init__(self, broker, indicators: Dict[str, Callable]):
        self.broker = broker
        self.indicators = indicators

    async def _fetch_data(self, ticker: str, timeframe: str, limit: int) -> pl.DataFrame:
        """
        브로커를 통해 단일 티커의 OHLCV 데이터를 가져옵니다.
        """
        logger.info(f"{ticker}의 {timeframe} 데이터 로딩 중 (최근 {limit}개)")
        df = await self.broker.get_ohlcv(ticker, timeframe, limit)
        return df

    async def run_scan(self, scan_logic: Dict[str, Any], tickers: List[str]) -> pl.DataFrame:
        """
        주어진 전략 로직에 따라 여러 티커에 대해 스캔을 실행합니다.
        조건을 만족하는 각 티커의 '최신' 데이터 행을 수집하여 반환합니다.
        """
        logger.info(f"'{scan_logic.get('name', 'Untitled')}' 전략으로 {len(tickers)}개 종목 스캔 시작")
        
        all_results = []
        timeframe = scan_logic.get("timeframe", "day")
        
        for ticker in tickers:
            try:
                ohlcv_df = await self._fetch_data(ticker, timeframe, 200)

                if ohlcv_df.is_empty():
                    logger.debug(f"{ticker}: 데이터를 가져오지 못해 건너뜁니다.")
                    continue

                parser = LogicParser(self.indicators, ohlcv_df)

                if 'variables' in scan_logic:
                    for var in scan_logic['variables']:
                        parser.set_variable(var['name'], var['expression'])

                final_condition = scan_logic['condition']
                mask = parser.evaluate(final_condition)

                # 마스크의 마지막 값이 True인지 확인 (가장 최신 데이터가 조건을 만족하는지)
                if mask.is_empty() or not mask[-1]:
                    continue

                # 조건을 만족한 경우, 해당 티커의 최신 데이터 행을 결과에 추가
                latest_data = ohlcv_df.tail(1).with_columns(pl.lit(ticker).alias("ticker"))
                all_results.append(latest_data)
                logger.info(f"조건 만족 종목 발견: {ticker}")

            except Exception as e:
                logger.error(f"{ticker} 스캔 중 오류 발생: {e}", exc_info=False)
                continue # 한 티커에서 오류 발생 시 다음 티커로 계속 진행

        if not all_results:
            return pl.DataFrame()

        # 모든 결과를 하나의 DataFrame으로 결합
        final_df = pl.concat(all_results)
        logger.info(f"스캔 완료. 총 {len(final_df)}개의 결과 발견.")
        return final_df
