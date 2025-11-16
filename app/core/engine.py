import polars as pl
import operator
import logging
from typing import Dict, Any, List, Callable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LogicParser:
    # ... (기존 LogicParser 코드는 변경 없음) ...
    def __init__(self, indicators: Dict[str, Callable], data: pl.DataFrame):
        self.indicators = indicators
        self.data = data
        self.variables: Dict[str, Any] = {}

    def _parse_tokens(self, expression: str) -> List[str]:
        # 간단한 공백 기반 토크나이저
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

    def evaluate_on_df(self, expression: str) -> pl.Series:
        tokens = self._parse_tokens(expression)
        rpn_queue = self._shunting_yard(tokens)
        final_expr = self._evaluate_rpn(rpn_queue)
        return self.data.select(final_expr).to_series()

    def set_variable(self, var_name: str, expression: str):
        tokens = self._parse_tokens(expression)
        rpn_queue = self._shunting_yard(tokens)
        expr_to_save = self._evaluate_rpn(rpn_queue)
        self.variables[var_name] = expr_to_save


class ScanEngine:
    """
    PRD v7.3의 '2단계 스캔' 아키텍처를 구현한 스캔 엔진.
    """
    def __init__(self, broker, indicators: Dict[str, Callable]):
        self.broker = broker
        self.indicators = indicators

    async def run_1st_scan(self, scan_logic: Dict[str, Any], tickers: List[str]) -> List[str]:
        """
        1차 스캔: 현재 시점 데이터만으로 빠르게 종목을 필터링합니다.
        """
        first_scan_conditions = scan_logic.get("1st_scan")
        if not first_scan_conditions:
            logger.info("1차 스캔 조건이 없습니다. 모든 종목을 2차 스캔 대상으로 합니다.")
            return tickers

        logger.info(f"1차 스캔 시작: {len(tickers)}개 종목 대상")
        market_data = await self.broker.get_market_data_for_1st_scan(tickers)

        if market_data.is_empty():
            logger.warning("1차 스캔을 위한 시장 데이터를 가져오지 못했습니다.")
            return []

        # 1차 스캔은 보조지표를 사용하지 않으므로, 빈 indicator 딕셔너리로 파서 초기화
        parser = LogicParser({}, market_data)

        # 'condition' 키에 전체 조건이 문자열로 들어옴
        condition_str = first_scan_conditions['condition']

        filtered_df = market_data.filter(parser.evaluate_on_df(condition_str))

        if filtered_df.is_empty():
            logger.info("1차 스캔 결과, 조건을 만족하는 종목이 없습니다.")
            return []

        passed_tickers = filtered_df["ticker"].to_list()
        logger.info(f"1차 스캔 통과: {len(passed_tickers)}개 종목")
        return passed_tickers

    async def run_2nd_scan(self, scan_logic: Dict[str, Any], tickers: List[str]) -> pl.DataFrame:
        """
        2차 스캔: 시계열 데이터를 사용하여 정밀하게 종목을 분석합니다.
        """
        second_scan_conditions = scan_logic.get("2nd_scan")
        if not second_scan_conditions:
            logger.warning("2차 스캔 조건이 없어 스캔을 종료합니다.")
            return pl.DataFrame()

        logger.info(f"2차 스캔 시작: {len(tickers)}개 종목 대상")
        all_results = []
        timeframe = second_scan_conditions.get("timeframe", "day")
        
        for ticker in tickers:
            try:
                # 2차 스캔은 과거 데이터가 필요
                ohlcv_df = await self.broker.get_ohlcv(ticker, timeframe, limit=200)

                if ohlcv_df.is_empty():
                    logger.debug(f"{ticker}: 2차 스캔 데이터를 가져오지 못해 건너뜁니다.")
                    continue

                parser = LogicParser(self.indicators, ohlcv_df)

                if 'variables' in second_scan_conditions:
                    for var in second_scan_conditions['variables']:
                        parser.set_variable(var['name'], var['expression'])

                final_condition = second_scan_conditions['condition']
                mask = parser.evaluate_on_df(final_condition)

                if mask.is_empty() or not mask.tail(1)[0]:
                    continue

                latest_data = ohlcv_df.tail(1).with_columns(pl.lit(ticker).alias("ticker"))
                all_results.append(latest_data)
                logger.info(f"2차 스캔 조건 만족: {ticker}")

            except Exception as e:
                logger.error(f"{ticker} 2차 스캔 중 오류: {e}", exc_info=False)
                continue

        if not all_results:
            return pl.DataFrame()

        final_df = pl.concat(all_results)
        logger.info(f"2차 스캔 완료. 최종 {len(final_df)}개 결과 발견.")
        return final_df
