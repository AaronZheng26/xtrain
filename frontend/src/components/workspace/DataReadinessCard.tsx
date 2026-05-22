import { Alert, Button, Card, Descriptions, Empty, Progress, Space, Tag, Typography } from 'antd'
import type { DatasetReadiness } from '../../types'

const { Text } = Typography

const levelMeta: Record<DatasetReadiness['level'], { label: string; status: 'success' | 'normal' | 'exception'; color: string }> = {
  excellent: { label: '适合直接快速分析', status: 'success', color: 'green' },
  good: { label: '可以分析，建议确认字段', status: 'normal', color: 'blue' },
  limited: { label: '只能做有限分析', status: 'normal', color: 'orange' },
  poor: { label: '不建议直接训练', status: 'exception', color: 'red' },
}

const fieldLabels: Record<string, string> = {
  timestamp: '时间字段',
  source_ip: '源 IP',
  destination_ip: '目标 IP',
  url_path: 'URL / Path',
  status_code: '状态码',
  user: '用户 / 账号',
  host: 'Host',
  process: 'Process',
  label: 'Label',
}

type Props = {
  readiness: DatasetReadiness | null
  loading: boolean
  errorMessage: string | null
  onQuickAnalysis: () => void
  onExpertMode: () => void
}

export function DataReadinessCard({ readiness, loading, errorMessage, onQuickAnalysis, onExpertMode }: Props) {
  const level = readiness ? levelMeta[readiness.level] : null
  const detectedEntries = readiness
    ? Object.entries(readiness.detected_fields).filter(([, fields]) => fields.length)
    : []

  return (
    <Card
      className="data-readiness-card"
      title="数据就绪度"
      loading={loading}
      extra={level ? <Tag color={level.color}>{level.label}</Tag> : null}
    >
      {errorMessage ? <Alert type="warning" showIcon message={errorMessage} /> : null}
      {readiness ? (
        <Space direction="vertical" size={16} className="full-width">
          <div className="readiness-summary-grid">
            <div>
              <Progress type="dashboard" percent={readiness.score} status={level?.status} />
            </div>
            <Descriptions
              column={1}
              size="small"
              items={[
                { key: 'shape', label: '数据规模', children: `${readiness.row_count} 行 / ${readiness.column_count} 列` },
                {
                  key: 'time',
                  label: '时间范围',
                  children: readiness.time_range.start && readiness.time_range.end
                    ? `${readiness.time_range.start} ~ ${readiness.time_range.end}`
                    : '未识别到可用时间范围',
                },
                { key: 'mode', label: '推荐模式', children: readiness.recommended_mode },
                { key: 'templates', label: '推荐模板', children: readiness.recommended_templates.length ? readiness.recommended_templates.join(', ') : '暂无明确模板' },
              ]}
            />
          </div>
          <div>
            <Text strong>已识别关键字段</Text>
            {detectedEntries.length ? (
              <div className="tag-wall">
                {detectedEntries.map(([field, columns]) => (
                  <Tag color="blue" key={field}>{fieldLabels[field] ?? field}: {columns.join(', ')}</Tag>
                ))}
              </div>
            ) : (
              <Text type="secondary"> 暂未识别关键字段。</Text>
            )}
          </div>
          {readiness.missing_fields.length ? (
            <Alert
              type="warning"
              showIcon
              message={`缺失关键字段：${readiness.missing_fields.map((field) => fieldLabels[field] ?? field).join('、')}`}
            />
          ) : null}
          {readiness.warnings.length ? (
            <Space direction="vertical" size={8} className="full-width">
              {readiness.warnings.slice(0, 3).map((warning) => (
                <Alert key={warning} type="info" showIcon message={warning} />
              ))}
            </Space>
          ) : null}
          <div>
            <Text strong>下一步建议</Text>
            <div className="tag-wall">
              {readiness.next_steps.map((step) => (
                <Tag color="geekblue" key={step}>{step}</Tag>
              ))}
            </div>
          </div>
          <Space wrap>
            <Button type="primary" onClick={onQuickAnalysis}>
              进入快速分析路径
            </Button>
            <Button onClick={onExpertMode}>
              进入专家模式
            </Button>
          </Space>
        </Space>
      ) : (
        <Empty description="选择或导入一个可分析数据版本后，这里会显示就绪度评分和下一步建议。" />
      )}
    </Card>
  )
}
