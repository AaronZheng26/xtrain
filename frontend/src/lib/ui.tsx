import { Space, Tag, Typography } from 'antd'
import type { ColumnsType } from 'antd/es/table'

const { Text } = Typography

export function formatPreviewValue(value: unknown) {
  if (value === null || value === undefined || value === '') {
    return <Text type="secondary">-</Text>
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}

export function renderTagGroup(values: string[] | undefined) {
  if (!values?.length) {
    return <Text type="secondary">暂无</Text>
  }
  return (
    <Space wrap>
      {values.map((value) => (
        <Tag key={value}>{value}</Tag>
      ))}
    </Space>
  )
}

export function buildPreviewColumns(columns: string[]): ColumnsType<Record<string, unknown>> {
  return columns.map((column) => ({
    title: column,
    dataIndex: column,
    key: column,
    ellipsis: true,
    render: (value: unknown) => formatPreviewValue(value),
  }))
}
