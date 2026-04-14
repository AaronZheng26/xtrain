import { useEffect, useRef } from 'react'
import * as echarts from 'echarts'

import type { Job } from '../types'

type Props = {
  jobs: Job[]
}

export function StatusTrendChart({ jobs }: Props) {
  const chartRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (!chartRef.current) {
      return
    }

    const chart = echarts.init(chartRef.current)
    const recentJobs = [...jobs].slice(0, 6).reverse()

    chart.setOption({
      tooltip: {
        trigger: 'axis',
      },
      grid: {
        left: 28,
        right: 12,
        top: 20,
        bottom: 24,
      },
      xAxis: {
        type: 'category',
        data: recentJobs.map((job) => `#${job.id}`),
        axisLine: {
          lineStyle: { color: '#8da2b5' },
        },
      },
      yAxis: {
        type: 'value',
        max: 100,
        axisLine: {
          show: false,
        },
        splitLine: {
          lineStyle: { color: 'rgba(16, 42, 67, 0.08)' },
        },
      },
      series: [
        {
          type: 'line',
          smooth: true,
          data: recentJobs.map((job) => job.progress),
          lineStyle: {
            width: 3,
            color: '#1677ff',
          },
          itemStyle: {
            color: '#13c2c2',
          },
          areaStyle: {
            color: 'rgba(22, 119, 255, 0.12)',
          },
        },
      ],
    })

    const resizeObserver = new ResizeObserver(() => chart.resize())
    resizeObserver.observe(chartRef.current)

    return () => {
      resizeObserver.disconnect()
      chart.dispose()
    }
  }, [jobs])

  return <div ref={chartRef} style={{ width: '100%', height: 240 }} />
}
