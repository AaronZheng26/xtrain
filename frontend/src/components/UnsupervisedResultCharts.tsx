import { useMemo } from 'react'

import type { ModelAnalysisRead } from '../types'

type Props = {
  analysis: ModelAnalysisRead
}

type Point = {
  x: number
  y: number
  label: string
  score?: number
}

const WIDTH = 640
const HEIGHT = 260
const PADDING = { top: 22, right: 18, bottom: 34, left: 44 }

export function UnsupervisedResultCharts({ analysis }: Props) {
  const scorePoints = useMemo<Point[]>(
    () =>
      analysis.score_points.map((point) => ({
        x: point.sample_index,
        y: point.anomaly_score,
        label: point.predicted_label,
        score: point.anomaly_score,
      })),
    [analysis.score_points],
  )
  const embeddingPoints = useMemo<Point[]>(
    () =>
      analysis.embedding_points.map((point) => ({
        x: point.x,
        y: point.y,
        label: point.predicted_label,
        score: point.anomaly_score,
      })),
    [analysis.embedding_points],
  )

  return (
    <div className="chart-grid">
      <div className="chart-panel">
        <div className="chart-panel-title">异常分数散点图</div>
        <ScatterSvg points={scorePoints} xLabel="样本序号" yLabel="异常分数" />
      </div>
      <div className="chart-panel">
        <div className="chart-panel-title">异常分数分布</div>
        <HistogramSvg analysis={analysis} />
      </div>
      <div className="chart-panel chart-panel-wide">
        <div className="chart-panel-title">二维投影聚类视图</div>
        <ScatterSvg points={embeddingPoints} xLabel="PCA-1" yLabel="PCA-2" tall />
      </div>
    </div>
  )
}

function ScatterSvg({ points, xLabel, yLabel, tall = false }: { points: Point[]; xLabel: string; yLabel: string; tall?: boolean }) {
  const height = tall ? 320 : HEIGHT
  const xValues = points.map((point) => point.x)
  const yValues = points.map((point) => point.y)
  const xScale = createScale(minOrZero(xValues), maxOrOne(xValues), PADDING.left, WIDTH - PADDING.right)
  const yScale = createScale(minOrZero(yValues), maxOrOne(yValues), height - PADDING.bottom, PADDING.top)

  return (
    <svg className="chart-canvas svg-chart" viewBox={`0 0 ${WIDTH} ${height}`} role="img">
      <ChartFrame width={WIDTH} height={height} xLabel={xLabel} yLabel={yLabel} />
      {points.map((point, index) => {
        const isAnomaly = point.label === 'anomaly'
        return (
          <circle
            key={`${point.x}-${point.y}-${index}`}
            cx={xScale(point.x)}
            cy={yScale(point.y)}
            r={isAnomaly ? 5 : 3.5}
            fill={isAnomaly ? '#f5222d' : '#3f8f2f'}
            opacity={isAnomaly ? 0.9 : 0.62}
          >
            <title>{`${isAnomaly ? '异常' : '正常'} / ${xLabel}: ${formatNumber(point.x)} / ${yLabel}: ${formatNumber(point.y)}`}</title>
          </circle>
        )
      })}
      <LegendItems items={[{ label: '正常点', color: '#3f8f2f' }, { label: '异常点', color: '#f5222d' }]} />
    </svg>
  )
}

function HistogramSvg({ analysis }: { analysis: ModelAnalysisRead }) {
  const buckets = analysis.score_histogram
  const maxTotal = Math.max(1, ...buckets.map((bucket) => bucket.normal_count + bucket.anomaly_count))
  const plotWidth = WIDTH - PADDING.left - PADDING.right
  const gap = 4
  const barWidth = buckets.length ? Math.max(4, plotWidth / buckets.length - gap) : 0
  const yScale = createScale(0, maxTotal, HEIGHT - PADDING.bottom, PADDING.top)

  return (
    <svg className="chart-canvas svg-chart" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img">
      <ChartFrame width={WIDTH} height={HEIGHT} xLabel="分数区间" yLabel="样本数" />
      {buckets.map((bucket, index) => {
        const x = PADDING.left + index * (barWidth + gap)
        const normalTop = yScale(bucket.normal_count)
        const totalTop = yScale(bucket.normal_count + bucket.anomaly_count)
        return (
          <g key={bucket.bucket_label}>
            <rect x={x} y={normalTop} width={barWidth} height={HEIGHT - PADDING.bottom - normalTop} fill="#91cc75">
              <title>{`${bucket.bucket_label} 正常: ${bucket.normal_count}`}</title>
            </rect>
            <rect x={x} y={totalTop} width={barWidth} height={normalTop - totalTop} fill="#ee6666">
              <title>{`${bucket.bucket_label} 异常: ${bucket.anomaly_count}`}</title>
            </rect>
          </g>
        )
      })}
      <LegendItems items={[{ label: '正常点', color: '#91cc75' }, { label: '异常点', color: '#ee6666' }]} />
    </svg>
  )
}

function ChartFrame({ width, height, xLabel, yLabel }: { width: number; height: number; xLabel: string; yLabel: string }) {
  return (
    <>
      <line x1={PADDING.left} y1={height - PADDING.bottom} x2={width - PADDING.right} y2={height - PADDING.bottom} stroke="#d9e2ec" />
      <line x1={PADDING.left} y1={PADDING.top} x2={PADDING.left} y2={height - PADDING.bottom} stroke="#d9e2ec" />
      <text x={width / 2} y={height - 8} textAnchor="middle" className="svg-chart-label">{xLabel}</text>
      <text x={12} y={height / 2} textAnchor="middle" transform={`rotate(-90 12 ${height / 2})`} className="svg-chart-label">{yLabel}</text>
    </>
  )
}

function LegendItems({ items }: { items: Array<{ label: string; color: string }> }) {
  return (
    <g transform={`translate(${PADDING.left}, 14)`}>
      {items.map((item, index) => (
        <g key={item.label} transform={`translate(${index * 72}, 0)`}>
          <circle cx={0} cy={0} r={4} fill={item.color} />
          <text x={8} y={4} className="svg-chart-label">{item.label}</text>
        </g>
      ))}
    </g>
  )
}

function createScale(domainMin: number, domainMax: number, rangeMin: number, rangeMax: number) {
  const span = domainMax - domainMin || 1
  return (value: number) => rangeMin + ((value - domainMin) / span) * (rangeMax - rangeMin)
}

function minOrZero(values: number[]) {
  return values.length ? Math.min(...values) : 0
}

function maxOrOne(values: number[]) {
  return values.length ? Math.max(...values) : 1
}

function formatNumber(value: number) {
  return Number.isInteger(value) ? String(value) : value.toFixed(3)
}
