import React, { useState, useCallback } from 'react';
import Palette from '../components/Palette';
import Canvas from '../components/Canvas';
import ScanResultsTable from '../components/ScanResultsTable';
import { ExpressionItem, ScanResult } from '../types';
import './StrategyBuilderPage.css';

const StrategyBuilderPage: React.FC = () => {
  const [firstScanItems, setFirstScanItems] = useState<ExpressionItem[]>([]);
  const [secondScanItems, setSecondScanItems] = useState<ExpressionItem[]>([]);
  const [scanResults, setScanResults] = useState<ScanResult[]>([]);
  const [isScanning, setIsScanning] = useState(false);
  const [activeCanvas, setActiveCanvas] = useState<'first' | 'second'>('first');

  const handleItemAdd = useCallback((item: ExpressionItem) => {
    if (activeCanvas === 'first') {
      setFirstScanItems(prev => [...prev, item]);
    } else {
      setSecondScanItems(prev => [...prev, item]);
    }
  }, [activeCanvas]);

  const handleRunScan = async () => {
    setIsScanning(true);
    setScanResults([]); // Clear previous results

    const firstScanExpression = firstScanItems.map(item => item.label).join(' ');
    const secondScanExpression = secondScanItems.map(item => item.label).join(' ');

    console.log("Running 1st Scan with expression:", firstScanExpression);
    console.log("Running 2nd Scan with expression:", secondScanExpression);
    
    // --- Mock Scan Simulation ---
    await new Promise(resolve => setTimeout(resolve, 2000));

    const mockData: ScanResult[] = [
      { ticker: 'KRW-BTC', code: 'C001', price: '71,200,000', amount: '1,500억', ohlc: 'bar_up' },
      { ticker: 'KRW-ETH', code: 'C002', price: '4,530,000', amount: '800억', ohlc: 'bar_down' },
      { ticker: 'KRW-DOGE', code: 'C003', price: '210', amount: '350억', ohlc: 'bar_up' },
    ];
    
    const finalResults = secondScanExpression.includes('BTC') 
      ? mockData.filter(r => r.ticker.includes('BTC'))
      : mockData;

    setScanResults(finalResults);
    // --- End of Mock Scan ---

    setIsScanning(false);
  };

  return (
    <div className="strategy-builder-page">
      <h2>Strategy Builder</h2>
      <Palette onItemAdd={handleItemAdd} />
      <div className="canvas-selector">
        <button onClick={() => setActiveCanvas('first')} className={activeCanvas === 'first' ? 'active' : ''}>
          1차 스캔
        </button>
        <button onClick={() => setActiveCanvas('second')} className={activeCanvas === 'second' ? 'active' : ''}>
          2차 스캔
        </button>
      </div>
      <div className="canvases-container">
        <div className="canvas-wrapper">
          <h3>1차 스캔 캔버스</h3>
          <Canvas items={firstScanItems} setItems={setFirstScanItems} />
        </div>
        <div className="canvas-wrapper">
          <h3>2차 스캔 캔버스</h3>
          <Canvas items={secondScanItems} setItems={setSecondScanItems} />
        </div>
      </div>
      <div className="scan-controls">
        <button onClick={handleRunScan} disabled={isScanning}>
          {isScanning ? 'Scanning...' : 'Run Scan'}
        </button>
      </div>
      <ScanResultsTable results={scanResults} isLoading={isScanning} />
    </div>
  );
};

export default StrategyBuilderPage;