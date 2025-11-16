import React, { useState, useEffect, useCallback } from 'react';
import axios from 'axios';
import { Strategy } from '../types';

// API 클라이언트 설정
const apiClient = axios.create({
  baseURL: 'http://localhost:8000/api/v1',
  headers: {
    'Content-Type': 'application/json',
  },
});

// WebSocket을 위한 URL
const WS_URL = 'ws://localhost:8000/ws/v1/updates';

const StrategyManagementPage: React.FC = () => {
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [watchlistCounts, setWatchlistCounts] = useState<Record<number, number>>({});
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [notification, setNotification] = useState<string>('');

  // 새 전략 입력을 위한 상태
  const [newStrategyName, setNewStrategyName] = useState('');
  const [newStrategyDescription, setNewStrategyDescription] = useState('');

  const showNotification = (message: string, duration: number = 3000) => {
    setNotification(message);
    setTimeout(() => setNotification(''), duration);
  };

  const fetchStrategies = useCallback(async () => {
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
  }, []);

  useEffect(() => {
    fetchStrategies();

    const ws = new WebSocket(WS_URL);
    ws.onopen = () => {
      console.log('WebSocket connection established');
      showNotification('Real-time connection established.', 2000);
    };
    ws.onmessage = (event) => {
      const message = JSON.parse(event.data);
      console.log('WebSocket message received:', message);

      if (message.event === 'watchlist_updated') {
        const { strategy_name, count } = message.payload;
        const targetStrategy = strategies.find(s => s.name === strategy_name);
        if (targetStrategy) {
          setWatchlistCounts(prev => ({ ...prev, [targetStrategy.id]: count }));
          showNotification(`'${strategy_name}' watchlist updated: ${count} items found.`);
        }
      } else if (message.event === 'scan_result_found') {
        const { strategy_name, results } = message.payload;
        showNotification(`'${strategy_name}' found ${results.length} results from 2nd scan!`);
      }
    };
    ws.onclose = () => {
      console.log('WebSocket connection closed');
    };
    ws.onerror = (err) => {
      console.error('WebSocket error:', err);
      setError('WebSocket connection failed.');
    };

    return () => {
      ws.close();
    };
  }, [fetchStrategies, strategies]);

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
        broker: "upbit",
        market: "KRW",
        scan_logic: { "condition": "close > open" }, // Placeholder logic
        is_active: false,
      };
      await apiClient.post('/strategies', newStrategy);
      setNewStrategyName('');
      setNewStrategyDescription('');
      fetchStrategies();
      showNotification(`Strategy '${newStrategy.name}' created successfully.`);
    } catch (err) {
      setError('Failed to create strategy.');
      console.error(err);
    }
  };

  const handleDeleteStrategy = async (id: number) => {
    if (window.confirm('Are you sure you want to delete this strategy?')) {
      try {
        await apiClient.delete(`/strategies/${id}`);
        fetchStrategies();
        showNotification(`Strategy #${id} deleted.`);
      } catch (err) {
        setError('Failed to delete strategy.');
        console.error(err);
      }
    }
  };

  const handleToggleActive = async (strategy: Strategy) => {
    try {
      const updatedStrategy = { ...strategy, is_active: !strategy.is_active };
      await apiClient.put(`/strategies/${strategy.id}`, updatedStrategy);
      fetchStrategies();
      showNotification(`Strategy '${strategy.name}' status updated.`);
    } catch (err) {
      setError(`Failed to update strategy #${strategy.id}.`);
      console.error(err);
    }
  };

  const handleRun1stScan = async (id: number) => {
    try {
      await apiClient.post(`/scans/${id}/run-1st`);
      showNotification(`1st scan for strategy #${id} started.`);
    } catch (err) {
      setError(`Failed to start 1st scan for strategy #${id}.`);
      console.error(err);
    }
  };

  const handleRun2ndScan = async (id: number) => {
    try {
      await apiClient.post(`/scans/${id}/run-2nd`);
      showNotification(`2nd scan for strategy #${id} started.`);
    } catch (err) {
      const errorData = (err as any).response?.data?.detail;
      const errorMessage = errorData || `Failed to start 2nd scan for strategy #${id}.`;
      setError(errorMessage);
      showNotification(errorMessage, 5000);
      console.error(err);
    }
  };

  if (loading) return <div>Loading strategies...</div>;

  return (
    <div style={{ padding: '2rem', fontFamily: 'sans-serif' }}>
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h2>Strategy Management</h2>
        {notification && <div style={{ color: 'blue', background: '#e0e7ff', padding: '0.5rem 1rem', borderRadius: '5px' }}>{notification}</div>}
      </header>

      {error && <div style={{ color: 'red', background: '#ffebee', padding: '0.5rem 1rem', borderRadius: '5px', marginBottom: '1rem' }}>ERROR: {error}</div>}

      <div style={{ marginBottom: '2rem', padding: '1rem', border: '1px solid #ccc', borderRadius: '5px' }}>
        <h3>Create New Strategy</h3>
        <form onSubmit={handleCreateStrategy} style={{ display: 'flex', gap: '1rem' }}>
          <input type="text" placeholder="Strategy Name" value={newStrategyName} onChange={(e) => setNewStrategyName(e.target.value)} required />
          <input type="text" placeholder="Description" value={newStrategyDescription} onChange={(e) => setNewStrategyDescription(e.target.value)} />
          <button type="submit">Save New Strategy</button>
        </form>
      </div>

      <h3>Existing Strategies</h3>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }}>
        <thead>
          <tr style={{ borderBottom: '2px solid #333', background: '#f4f4f4' }}>
            <th style={{ padding: '12px', textAlign: 'left' }}>Active</th>
            <th style={{ padding: '12px', textAlign: 'left' }}>Name</th>
            <th style={{ padding: '12px', textAlign: 'left' }}>Broker</th>
            <th style={{ padding: '12px', textAlign: 'left' }}>Market</th>
            <th style={{ padding: '12px', textAlign: 'left' }}>Watchlist</th>
            <th style={{ padding: '12px', textAlign: 'left' }}>Created At</th>
            <th style={{ padding: '12px', textAlign: 'left' }}>Updated At</th>
            <th style={{ padding: '12px', textAlign: 'center' }}>Actions</th>
          </tr>
        </thead>
        <tbody>
          {strategies.map((strategy) => (
            <tr key={strategy.id} style={{ borderBottom: '1px solid #eee' }}>
              <td style={{ padding: '12px' }}>
                <input type="checkbox" checked={strategy.is_active} onChange={() => handleToggleActive(strategy)} title="Toggle strategy activation"/>
              </td>
              <td style={{ padding: '12px', fontWeight: 'bold' }}>{strategy.name}</td>
              <td style={{ padding: '12px' }}>{strategy.broker}</td>
              <td style={{ padding: '12px' }}>{strategy.market}</td>
              <td style={{ padding: '12px', textAlign: 'center' }}>
                {watchlistCounts[strategy.id] ?? 'N/A'}
              </td>
              <td style={{ padding: '12px' }}>{new Date(strategy.created_at).toLocaleString()}</td>
              <td style={{ padding: '12px' }}>{new Date(strategy.updated_at).toLocaleString()}</td>
              <td style={{ padding: '12px', textAlign: 'center', display: 'flex', gap: '8px', justifyContent: 'center' }}>
                <button onClick={() => handleRun1stScan(strategy.id)}>1st Scan</button>
                <button onClick={() => handleRun2ndScan(strategy.id)}>2nd Scan</button>
                <button disabled>Edit</button>
                <button onClick={() => handleDeleteStrategy(strategy.id)} style={{ background: '#ef5350', color: 'white' }}>Delete</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default StrategyManagementPage;
