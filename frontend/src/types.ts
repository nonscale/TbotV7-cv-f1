export interface ExpressionItem {
  id: string;
  type: 'data' | 'operator' | 'number' | 'indicator' | 'variable' | 'function';
  label: string;
}

export interface ScanResult {
  ticker: string;
  code: string;
  price: string;
  amount: string;
  ohlc: 'bar_up' | 'bar_down';
}