import React from 'react';
import StrategyBuilderPage from './pages/StrategyBuilderPage';
import './App.css';

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <h1>Trading Bot Dashboard</h1>
      </header>
      <main>
        <StrategyBuilderPage />
      </main>
    </div>
  );
}

export default App;