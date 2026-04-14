import { Alert, Button, Card, Col, Empty, Form, Row, Select, Space, Tag, Typography } from 'antd'
import type { FormInstance } from 'antd'

import type { DatasetVersion, FieldMapping } from '../types'

const { Text } = Typography

const standardFieldLabels: Record<string, string> = {
  event_time: '标准时间字段',
  source_ip: '源 IP',
  dest_ip: '目标 IP',
  status_code: '状态码',
  label: '标签列',
  raw_message: '原始消息',
}

type Props = {
  dataset: DatasetVersion | null
  fieldMapping: FieldMapping | null
  loading: boolean
  saving: boolean
  columns: string[]
  form: FormInstance<Record<string, string | undefined>>
  onSave: () => void
}

export function FieldMappingCard({ dataset, fieldMapping, loading, saving, columns, form, onSave }: Props) {
  return (
    <Card
      title="字段映射确认"
      extra={
        fieldMapping ? (
          <Tag color={fieldMapping.confirmed ? 'green' : 'gold'}>
            {fieldMapping.confirmed ? '已确认' : '待确认'}
          </Tag>
        ) : null
      }
    >
      {dataset ? (
        <Space direction="vertical" size={16} style={{ width: '100%' }}>
          <Alert
            type="info"
            showIcon
            message="字段映射会把原始列对齐到平台标准语义，后续预处理和训练都可以直接复用。"
          />
          <Form form={form} layout="vertical">
            <Row gutter={12}>
              {Object.entries(standardFieldLabels).map(([fieldKey, label]) => (
                <Col span={24} key={fieldKey}>
                  <Form.Item label={label} name={fieldKey}>
                    <Select
                      allowClear
                      showSearch
                      loading={loading}
                      placeholder={`为 ${label} 选择一个原始列`}
                      options={columns.map((column) => ({ label: column, value: column }))}
                    />
                  </Form.Item>
                </Col>
              ))}
            </Row>
          </Form>
          <Button type="primary" loading={saving || loading} onClick={onSave}>
            保存字段映射
          </Button>
          <Text type="secondary">建议优先确认时间、标签和原始消息字段，这样后续链路最稳定。</Text>
        </Space>
      ) : (
        <Empty description="选择一个数据集后才能确认字段映射。" />
      )}
    </Card>
  )
}
