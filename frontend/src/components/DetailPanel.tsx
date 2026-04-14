import { Card, Empty } from 'antd'
import type { ReactNode } from 'react'

type Props = {
  title: string
  extra?: ReactNode
  emptyDescription?: string
  children?: ReactNode
}

export function DetailPanel({ title, extra, emptyDescription, children }: Props) {
  return (
    <Card title={title} extra={extra} className="detail-panel">
      {children ?? <Empty description={emptyDescription ?? '请选择一项查看详情。'} />}
    </Card>
  )
}
