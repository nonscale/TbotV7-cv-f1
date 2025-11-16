import React, { useState, useEffect } from 'react';

// 스캔 결과 데이터 타입 정의
interface ScanResult {
  ticker: string;
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  amount: number;
}

interface ScanEventPayload {
  strategy_name: string;
  results: ScanResult[];
}

const DashboardPage: React.FC = () => {
  const [scanResults, setScanResults] = useState<ScanEventPayload[]>([]);
  const [wsStatus, setWsStatus] = useState<'Connecting' | 'Connected' | 'Disconnected'>('Connecting');

  useEffect(() => {
    // JWT 토큰이나 사용자 ID 등을 사용하여 고유한 클라이언트 ID 생성
    const clientId = `client_${Date.now()}`;
    const wsUrl = `ws://localhost:8000/ws/v1/updates?token=${clientId}`;

    const socket = new WebSocket(wsUrl);

    socket.onopen = () => {
      console.log('WebSocket connection established');
      setWsStatus('Connected');
    };

    socket.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data);

        // 서버에서 보낸 스캔 결과 이벤트 처리
        if (message.event === 'scan_result_found') {
          console.log('Scan result received:', message.payload);
          setScanResults(prevResults => [message.payload, ...prevResults]);
        } else {
          console.log('Received notification:', message.payload);
        }
      } catch (error) {
        console.error('Error parsing WebSocket message:', error);
      }
    };

    socket.onclose = () => {
      console.log('WebSocket connection closed');
      setWsStatus('Disconnected');
    };

    socket.onerror = (error) => {
      console.error('WebSocket error:', error);
      setWsStatus('Disconnected');
    };

    // 컴포넌트 언마운트 시 WebSocket 연결 정리
    return () => {
      socket.close();
    };
  }, []); // 빈 배열을 전달하여 컴포넌트 마운트 시 한 번만 실행되도록 설정

  return (
    <div style={{ padding: '2rem' }}>
      <h2>Dashboard</h2>
      <p>WebSocket Status: <strong>{wsStatus}</strong></p>
      <p>This dashboard displays real-time scan results from active strategies.</p>

      <div style={{ marginTop: '2rem' }}>
        <h3>Real-time Scan Results</h3>
        {scanResults.length === 0 ? (
          <p>No scan results yet. Run a scan from the Strategy Management page.</p>
        ) : (
          scanResults.map((payload, index) => (
            <div key={index} style={{ marginBottom: '1.5rem', border: '1px solid #ddd', padding: '1rem' }}>
              <h4>Strategy: {payload.strategy_name}</h4>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ borderBottom: '1px solid #ccc' }}>
                    <th style={{ padding: '8px', textAlign: 'left' }}>Ticker</th>
                    <th style={{ padding: '8px', textAlign: 'left' }}>Close Price</th>
                    <th style={{ padding: '8px', textAlign: 'left' }}>Volume</th>
                    <th style={{ padding: '8px', textAlign: 'left' }}>Amount (KRW)</th>
                  </tr>
                </thead>
                <tbody>
                  {payload.results.map((result) => (
                    <tr key={result.ticker} style={{ borderBottom: '1px solid #eee' }}>
                      <td style={{ padding: '8px' }}>{result.ticker}</td>
                      <td style={{ padding: '8px' }}>{result.close.toLocaleString()}</td>
                      <td style={{ padding: '8px' }}>{result.volume.toFixed(2)}</td>
                      <td style={{ padding: '8px' }}>{(result.amount / 100000000).toFixed(2)}억</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ))
        )}
      </div>
    </div>
  );
};

export default DashboardPage;
