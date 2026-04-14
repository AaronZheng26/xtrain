import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { vi } from 'vitest'

import App from './App'

vi.mock('./pages/DashboardPage', () => ({
  DashboardPage: () => <div>dashboard-page</div>,
}))

vi.mock('./pages/ProjectWorkspacePage', () => ({
  ProjectWorkspacePage: () => <div>workspace-page</div>,
}))

describe('App routes', () => {
  it('renders dashboard route', async () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <App />
      </MemoryRouter>,
    )

    await waitFor(() => expect(screen.getByText('dashboard-page')).toBeInTheDocument())
  })

  it('renders project workspace route', async () => {
    render(
      <MemoryRouter initialEntries={['/projects/42']}>
        <App />
      </MemoryRouter>,
    )

    await waitFor(() => expect(screen.getByText('workspace-page')).toBeInTheDocument())
  })
})
