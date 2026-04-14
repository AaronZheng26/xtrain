import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { vi } from 'vitest'

import { ProjectWorkspacePage } from './ProjectWorkspacePage'
import { api } from '../lib/api'

vi.mock('../components/workspace/AnalysisTab', () => ({
  AnalysisTab: () => <div>analysis-tab</div>,
}))
vi.mock('../components/workspace/DataTab', () => ({
  DataTab: () => <div>data-tab</div>,
}))
vi.mock('../components/workspace/FeatureTab', () => ({
  FeatureTab: () => <div>feature-tab</div>,
}))
vi.mock('../components/workspace/PreprocessTab', () => ({
  PreprocessTab: () => <div>preprocess-tab</div>,
}))
vi.mock('../components/workspace/TrainingTab', () => ({
  TrainingTab: () => <div>training-tab</div>,
}))
vi.mock('../components/WorkspaceHeader', () => ({
  WorkspaceHeader: () => <div>workspace-header</div>,
}))

describe('ProjectWorkspacePage', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('renders backend error banner when workspace shell load fails', async () => {
    vi.spyOn(api, 'get').mockRejectedValue(new Error('offline'))

    render(
      <MemoryRouter initialEntries={['/projects/1']}>
        <Routes>
          <Route path="/projects/:projectId" element={<ProjectWorkspacePage />} />
        </Routes>
      </MemoryRouter>,
    )

    await waitFor(() => {
      expect(screen.getByText('无法连接后端，请先启动 FastAPI 服务。')).toBeInTheDocument()
    })
  })

  it('loads llm config only when analysis tab is active', async () => {
    vi.spyOn(api, 'get').mockImplementation(async (url: string) => {
      if (url === '/projects/1') {
        return { data: { id: 1, name: 'demo', description: '', status: 'active', created_at: '2026-04-11T00:00:00Z' } }
      }
      if (url === '/jobs') {
        return { data: [] }
      }
      if (url === '/datasets') {
        return { data: [] }
      }
      if (url === '/analysis/projects/1/llm-config') {
        return {
          data: {
            id: 1,
            project_id: 1,
            provider: 'ollama',
            enabled: true,
            base_url: 'http://127.0.0.1:11434',
            model_name: 'qwen2.5:7b',
            has_api_key: false,
            api_key_hint: null,
            created_at: '2026-04-11T00:00:00Z',
            updated_at: '2026-04-11T00:00:00Z',
          },
        }
      }
      throw new Error(`Unhandled GET ${url}`)
    })

    render(
      <MemoryRouter initialEntries={['/projects/1?tab=analysis']}>
        <Routes>
          <Route path="/projects/:projectId" element={<ProjectWorkspacePage />} />
        </Routes>
      </MemoryRouter>,
    )

    await waitFor(() => {
      expect(screen.getByText('workspace-header')).toBeInTheDocument()
    })

    expect(api.get).toHaveBeenCalledWith('/analysis/projects/1/llm-config')
  })
})
