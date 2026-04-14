import { Suspense, lazy } from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'
import { Spin } from 'antd'

import './App.css'

const DashboardPage = lazy(async () => {
  const module = await import('./pages/DashboardPage')
  return { default: module.DashboardPage }
})

const ProjectWorkspacePage = lazy(async () => {
  const module = await import('./pages/ProjectWorkspacePage')
  return { default: module.ProjectWorkspacePage }
})

function App() {
  return (
    <Suspense fallback={<div className="loading-state"><Spin size="large" /></div>}>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/projects/:projectId" element={<ProjectWorkspacePage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Suspense>
  )
}

export default App
