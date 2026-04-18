import { Routes, Route, NavLink, Navigate } from 'react-router-dom'
import Page1 from './pages/Page1'
import Page2 from './pages/Page2'

export default function App() {
  return (
    <div className="app">
      <header className="navbar">
        <span className="navbar-brand">NYC Transit Analytics</span>
        <nav className="navbar-nav">
          <NavLink
            to="/analytics"
            className={({ isActive }) => 'nav-tab' + (isActive ? ' active' : '')}
          >
            Monthly Analytics
          </NavLink>
          <NavLink
            to="/forecast"
            className={({ isActive }) => 'nav-tab' + (isActive ? ' active' : '')}
          >
            Demand Forecast
          </NavLink>
        </nav>
      </header>
      <main className="page-content">
        <Routes>
          <Route path="/" element={<Navigate to="/analytics" replace />} />
          <Route path="/analytics" element={<Page1 />} />
          <Route path="/forecast" element={<Page2 />} />
        </Routes>
      </main>
    </div>
  )
}