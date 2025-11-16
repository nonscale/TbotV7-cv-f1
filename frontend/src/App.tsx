import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import StrategyBuilderPage from './pages/StrategyBuilderPage.tsx';
import StrategyManagementPage from './pages/StrategyManagementPage.tsx';
import DashboardPage from './pages/DashboardPage.tsx';
import AnalysisPage from './pages/AnalysisPage.tsx';
import './App.css';

function App() {
  return (
    <Router>
      <div className="App">
        <header className="App-header">
          <h1>Trading Bot Dashboard</h1>
          <nav>
            <ul>
              <li><Link to="/strategy-management">Strategy Management</Link></li>
              <li><Link to="/dashboard">Dashboard</Link></li>
              <li><Link to="/analysis">Analysis</Link></li>
              <li><Link to="/strategy-builder">Strategy Builder</Link></li>
            </ul>
          </nav>
        </header>
        <main>
          <Routes>
            <Route path="/strategy-management" element={<StrategyManagementPage />} />
            <Route path="/dashboard" element={<DashboardPage />} />
            <Route path="/analysis" element={<AnalysisPage />} />
            <Route path="/strategy-builder" element={<StrategyBuilderPage />} />
            <Route path="/" element={<DashboardPage />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;
