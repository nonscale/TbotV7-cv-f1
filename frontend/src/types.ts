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

export interface Strategy {
  id: number;
  name: string;
  description?: string;
  scan_logic: any;
  is_active: boolean;
  cron_schedule?: string;
  created_at: string;
  updated_at?: string;
}
