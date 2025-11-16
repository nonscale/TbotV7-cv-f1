import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Strategy } from '../types';

// API 클라이언트 설정
const apiClient = axios.create({
  baseURL: 'http://localhost:8000/api/v1',
  headers: {
    'Content-Type': 'application/json',
  },
});

const StrategyManagementPage: React.FC = () => {
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [notification, setNotification] = useState<string>('');

  // 새 전략 입력을 위한 상태
  const [newStrategyName, setNewStrategyName] = useState('');
  const [newStrategyDescription, setNewStrategyDescription] = useState('');

  // 전략 목록을 가져오는 함수
  const fetchStrategies = async () => {
    setLoading(true);
    try {
      const response = await apiClient.get('/strategies');
      setStrategies(response.data);
      setError(null);
    } catch (err) {
      setError('Failed to fetch strategies.');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  // 컴포넌트 마운트 시 전략 목록 가져오기
  useEffect(() => {
    fetchStrategies();
  }, []);

  // 새 전략 생성 핸들러
  const handleCreateStrategy = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newStrategyName.trim()) {
      alert('Strategy name is required.');
      return;
    }
    try {
      const newStrategy = {
        name: newStrategyName,
        description: newStrategyDescription,
        scan_logic: { // 실제 scan_logic은 Strategy Builder에서 정의해야 합니다.
            "name": newStrategyName,
            "timeframe": "day",
            "variables": [
                {"name": "ma_short", "expression": "ma(5)"},
                {"name": "ma_long", "expression": "ma(20)"}
            ],
            "condition": "ma_short > ma_long"
        },
        is_active: false,
      };
      await apiClient.post('/strategies', newStrategy);
      setNewStrategyName('');
      setNewStrategyDescription('');
      fetchStrategies(); // 목록 새로고침
      showNotification(`Strategy '${newStrategy.name}' created successfully.`);
    } catch (err) {
      setError('Failed to create strategy.');
      console.error(err);
    }
  };

  // 전략 삭제 핸들러
  const handleDeleteStrategy = async (id: number) => {
    if (window.confirm('Are you sure you want to delete this strategy?')) {
      try {
        await apiClient.delete(`/strategies/${id}`);
        fetchStrategies(); // 목록 새로고침
        showNotification(`Strategy #${id} deleted.`);
      } catch (err) {
        setError('Failed to delete strategy.');
        console.error(err);
      }
    }
  };

  // 스캔 실행 핸들러
  const handleRunScan = async (id: number) => {
    try {
      const response = await apiClient.post(`/scans/${id}/run`);
      showNotification(`Scan for strategy #${id} started in the background.`);
    } catch (err) {
      setError(`Failed to start scan for strategy #${id}.`);
      console.error(err);
    }
  };

  // 알림 메시지 표시 함수
  const showNotification = (message: string) => {
    setNotification(message);
    setTimeout(() => setNotification(''), 3000); // 3초 후 사라짐
  };


  if (loading) return <div>Loading...</div>;
  if (error) return <div style={{ color: 'red' }}>ERROR: {error}</div>;

  return (
    <div style={{ padding: '2rem' }}>
      <h2>Strategy Management</h2>

      {notification && <div style={{ color: 'green', marginBottom: '1rem' }}>{notification}</div>}

      {/* ... (새 전략 생성 폼은 이전과 동일) ... */}
      <div style={{ marginBottom: '2rem', padding: '1rem', border: '1px solid #ccc' }}>
        <h3>Create New Strategy</h3>
        <form onSubmit={handleCreateStrategy}>
          <input type="text" placeholder="Strategy Name" value={newStrategyName} onChange={(e) => setNewStrategyName(e.target.value)} required style={{ marginRight: '1rem' }} />
          <input type="text" placeholder="Description" value={newStrategyDescription} onChange={(e) => setNewStrategyDescription(e.target.value)} style={{ marginRight: '1rem' }} />
          <button type="submit">Save New Strategy</button>
        </form>
      </div>

      <h3>Existing Strategies</h3>
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr style={{ borderBottom: '1px solid #ccc' }}>
            <th style={{ padding: '8px', textAlign: 'left' }}>ID</th>
            <th style={{ padding: '8px', textAlign: 'left' }}>Name</th>
            <th style={{ padding: '8px', textAlign: 'left' }}>Active</th>
            <th style={{ padding: '8px', textAlign: 'left' }}>Actions</th>
          </tr>
        </thead>
        <tbody>
          {strategies.map((strategy) => (
            <tr key={strategy.id} style={{ borderBottom: '1px solid #eee' }}>
              <td style={{ padding: '8px' }}>{strategy.id}</td>
              <td style={{ padding: '8px' }}>{strategy.name}</td>
              <td style={{ padding: '8px' }}>{strategy.is_active ? 'Yes' : 'No'}</td>
              <td style={{ padding: '8px' }}>
                <button onClick={() => handleRunScan(strategy.id)} style={{ marginRight: '8px' }}>Run Scan</button>
                <button disabled style={{ marginRight: '8px' }}>Edit</button>
                <button onClick={() => handleDeleteStrategy(strategy.id)}>Delete</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default StrategyManagementPage;
