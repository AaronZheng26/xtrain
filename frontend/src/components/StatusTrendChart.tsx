import type { Job } from '../types'

type Props = {
  jobs: Job[]
}

const WIDTH = 640
const HEIGHT = 220
const PADDING = { top: 20, right: 18, bottom: 28, left: 34 }

export function StatusTrendChart({ jobs }: Props) {
  const recentJobs = [...jobs].slice(0, 6).reverse()
  const points = recentJobs.map((job, index) => ({
    id: job.id,
    status: job.status,
    x: scale(index, 0, Math.max(recentJobs.length - 1, 1), PADDING.left, WIDTH - PADDING.right),
    y: scale(job.progress, 0, 100, HEIGHT - PADDING.bottom, PADDING.top),
    progress: job.progress,
  }))
  const path = points.map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x} ${point.y}`).join(' ')
  const areaPath = points.length
    ? `${path} L ${points[points.length - 1].x} ${HEIGHT - PADDING.bottom} L ${points[0].x} ${HEIGHT - PADDING.bottom} Z`
    : ''

  return (
    <svg className="status-trend-chart" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img">
      <line x1={PADDING.left} y1={HEIGHT - PADDING.bottom} x2={WIDTH - PADDING.right} y2={HEIGHT - PADDING.bottom} stroke="#d9e2ec" />
      <line x1={PADDING.left} y1={PADDING.top} x2={PADDING.left} y2={HEIGHT - PADDING.bottom} stroke="#d9e2ec" />
      {[0, 50, 100].map((value) => {
        const y = scale(value, 0, 100, HEIGHT - PADDING.bottom, PADDING.top)
        return (
          <g key={value}>
            <line x1={PADDING.left} y1={y} x2={WIDTH - PADDING.right} y2={y} stroke="rgba(16, 42, 67, 0.08)" />
            <text x={8} y={y + 4} className="svg-chart-label">{value}</text>
          </g>
        )
      })}
      {areaPath ? <path d={areaPath} fill="rgba(22, 119, 255, 0.1)" /> : null}
      {path ? <path d={path} fill="none" stroke="#1677ff" strokeWidth={3} strokeLinejoin="round" strokeLinecap="round" /> : null}
      {points.map((point) => (
        <g key={point.id}>
          <circle cx={point.x} cy={point.y} r={5} fill={point.status === 'failed' ? '#ff4d4f' : '#13c2c2'}>
            <title>{`#${point.id} ${point.status} ${point.progress}%`}</title>
          </circle>
          <text x={point.x} y={HEIGHT - 8} textAnchor="middle" className="svg-chart-label">#{point.id}</text>
        </g>
      ))}
    </svg>
  )
}

function scale(value: number, domainMin: number, domainMax: number, rangeMin: number, rangeMax: number) {
  const span = domainMax - domainMin || 1
  return rangeMin + ((value - domainMin) / span) * (rangeMax - rangeMin)
}
