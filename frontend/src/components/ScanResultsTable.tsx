import React from 'react';
import { ScanResult } from '../types';
import './ScanResultsTable.css';

interface ScanResultsTableProps {
  results: ScanResult[];
  isLoading: boolean;
}

const ScanResultsTable: React.FC<ScanResultsTableProps> = ({ results, isLoading }) => {
  if (isLoading) return <div className="loading-spinner">Loading...</div>;
  if (results.length === 0) return <div className="no-results">스캔 결과가 여기에 표시됩니다.</div>;
  return (
    <div className="scan-results-container">
      <h3>스캔 결과</h3>
      <table className="results-table">
        <thead>
          <tr>
            <th>종목명</th><th>코드명</th><th>현재가</th><th>거래대금(억)</th><th>OHLC 미니바</th>
          </tr>
        </thead>
        <tbody>
          {results.map((result, index) => (
            <tr key={index}>
              <td>{result.ticker}</td><td>{result.code}</td>
              <td>{result.price}</td><td>{result.amount}</td>
              <td><div className={`ohlc-bar ${result.ohlc}`}></div></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default ScanResultsTable;