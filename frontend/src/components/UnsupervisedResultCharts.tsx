import { useEffect, useRef } from 'react'
import * as echarts from 'echarts'

import type { ModelAnalysisRead } from '../types'

type Props = {
  analysis: ModelAnalysisRead
}

export function UnsupervisedResultCharts({ analysis }: Props) {
  const scoreRef = useRef<HTMLDivElement | null>(null)
  const histogramRef = useRef<HTMLDivElement | null>(null)
  const embeddingRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (!scoreRef.current || !histogramRef.current || !embeddingRef.current) {
      return
    }

    const scoreChart = echarts.init(scoreRef.current)
    const histogramChart = echarts.init(histogramRef.current)
    const embeddingChart = echarts.init(embeddingRef.current)

    const normalPoints = analysis.score_points
      .filter((point) => point.predicted_label !== 'anomaly')
      .map((point) => [point.sample_index, point.anomaly_score])
    const anomalyPoints = analysis.score_points
      .filter((point) => point.predicted_label === 'anomaly')
      .map((point) => [point.sample_index, point.anomaly_score])

    scoreChart.setOption({
      tooltip: { trigger: 'item' },
      grid: { left: 44, right: 16, top: 24, bottom: 36 },
      legend: { top: 0, textStyle: { color: '#44556c' } },
      xAxis: {
        type: 'value',
        name: '样本序号',
        splitLine: { lineStyle: { color: 'rgba(16, 42, 67, 0.08)' } },
      },
      yAxis: {
        type: 'value',
        name: '异常分数',
        splitLine: { lineStyle: { color: 'rgba(16, 42, 67, 0.08)' } },
      },
      series: [
        {
          name: '正常点',
          type: 'scatter',
          data: normalPoints,
          symbolSize: 8,
          itemStyle: { color: '#7cb305', opacity: 0.68 },
        },
        {
          name: '异常点',
          type: 'scatter',
          data: anomalyPoints,
          symbolSize: 12,
          itemStyle: { color: '#f5222d', opacity: 0.92 },
        },
      ],
    })

    histogramChart.setOption({
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      grid: { left: 44, right: 16, top: 24, bottom: 56 },
      legend: { top: 0, textStyle: { color: '#44556c' } },
      xAxis: {
        type: 'category',
        data: analysis.score_histogram.map((bucket) => bucket.bucket_label),
        axisLabel: { rotate: 30, color: '#5b6b82' },
      },
      yAxis: {
        type: 'value',
        splitLine: { lineStyle: { color: 'rgba(16, 42, 67, 0.08)' } },
      },
      series: [
        {
          name: '正常点',
          type: 'bar',
          stack: 'total',
          data: analysis.score_histogram.map((bucket) => bucket.normal_count),
          itemStyle: { color: '#91cc75' },
        },
        {
          name: '异常点',
          type: 'bar',
          stack: 'total',
          data: analysis.score_histogram.map((bucket) => bucket.anomaly_count),
          itemStyle: { color: '#ee6666' },
        },
      ],
    })

    const normalEmbedding = analysis.embedding_points
      .filter((point) => point.predicted_label !== 'anomaly')
      .map((point) => [point.x, point.y, point.anomaly_score])
    const anomalyEmbedding = analysis.embedding_points
      .filter((point) => point.predicted_label === 'anomaly')
      .map((point) => [point.x, point.y, point.anomaly_score])

    embeddingChart.setOption({
      tooltip: {
        trigger: 'item',
        formatter: (params: { data: number[]; seriesName: string }) =>
          `${params.seriesName}<br/>PCA-1: ${params.data[0].toFixed(2)}<br/>PCA-2: ${params.data[1].toFixed(2)}<br/>异常分数: ${params.data[2].toFixed(3)}`,
      },
      grid: { left: 36, right: 16, top: 24, bottom: 36 },
      legend: { top: 0, textStyle: { color: '#44556c' } },
      xAxis: {
        type: 'value',
        name: 'PCA-1',
        splitLine: { lineStyle: { color: 'rgba(16, 42, 67, 0.08)' } },
      },
      yAxis: {
        type: 'value',
        name: 'PCA-2',
        splitLine: { lineStyle: { color: 'rgba(16, 42, 67, 0.08)' } },
      },
      series: [
        {
          name: '正常点',
          type: 'scatter',
          data: normalEmbedding,
          symbolSize: 8,
          itemStyle: { color: '#5b8ff9', opacity: 0.6 },
        },
        {
          name: '异常点',
          type: 'scatter',
          data: anomalyEmbedding,
          symbolSize: 13,
          itemStyle: { color: '#ff4d4f', opacity: 0.9 },
        },
      ],
    })

    const resizeObserver = new ResizeObserver(() => {
      scoreChart.resize()
      histogramChart.resize()
      embeddingChart.resize()
    })

    resizeObserver.observe(scoreRef.current)
    resizeObserver.observe(histogramRef.current)
    resizeObserver.observe(embeddingRef.current)

    return () => {
      resizeObserver.disconnect()
      scoreChart.dispose()
      histogramChart.dispose()
      embeddingChart.dispose()
    }
  }, [analysis])

  return (
    <div className="chart-grid">
      <div className="chart-panel">
        <div className="chart-panel-title">异常分数散点图</div>
        <div ref={scoreRef} className="chart-canvas" />
      </div>
      <div className="chart-panel">
        <div className="chart-panel-title">异常分数分布</div>
        <div ref={histogramRef} className="chart-canvas" />
      </div>
      <div className="chart-panel chart-panel-wide">
        <div className="chart-panel-title">二维投影聚类视图</div>
        <div ref={embeddingRef} className="chart-canvas chart-canvas-tall" />
      </div>
    </div>
  )
}
