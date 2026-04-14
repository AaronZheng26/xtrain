import { Button, Card, Col, Descriptions, Empty, Form, Input, List, Row, Select, Space, Table, Tag, Typography } from 'antd'
import type { FormInstance } from 'antd'
import { NodeIndexOutlined } from '@ant-design/icons'

import type { PreprocessPipeline, PreprocessPreviewRead } from '../types'

const { Text } = Typography

export type PreprocessFormValues = {
  name: string
  fillColumns?: string[]
  fillValue?: string
  castColumn?: string
  castType?: string
  dedupeColumns?: string[]
  selectColumns?: string[]
}

type Props = {
  enabled: boolean
  columns: string[]
  form: FormInstance<PreprocessFormValues>
  running: boolean
  pipelines: PreprocessPipeline[]
  selectedPipelineId: number | null
  selectedPipeline: PreprocessPipeline | null
  preview: PreprocessPreviewRead | null
  listLoading: boolean
  previewLoading: boolean
  onRun: (values: PreprocessFormValues) => void
  onSelectPipeline: (pipelineId: number) => void
}

export function PreprocessPanel({
  enabled,
  columns,
  form,
  running,
  pipelines,
  selectedPipelineId,
  selectedPipeline,
  preview,
  listLoading,
  previewLoading,
  onRun,
  onSelectPipeline,
}: Props) {
  const watchedValues = Form.useWatch([], form)
  const previewColumns = (preview?.columns ?? []).map((column) => ({
    title: column,
    dataIndex: column,
    key: column,
    ellipsis: true,
    render: (value: unknown) => formatPreviewValue(value),
  }))
  const pendingSteps = buildPendingSteps(watchedValues)

  return (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={14}>
        <Card title="预处理编排">
          {enabled ? (
            <Form form={form} layout="vertical" onFinish={onRun}>
              <Row gutter={16}>
                <Col xs={24} md={12}>
                  <Form.Item name="name" label="流水线名称" rules={[{ required: true, message: '请输入流水线名称' }]}>
                    <Input placeholder="例如：dataset-v1-prep-1" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="fillColumns" label="空值填充字段">
                    <Select mode="multiple" allowClear options={columns.map(toOption)} placeholder="选择需要填充的字段" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="fillValue" label="填充值">
                    <Input placeholder="例如：unknown / 0" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="castColumn" label="类型转换字段">
                    <Select allowClear options={columns.map(toOption)} placeholder="选择一个字段" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="castType" label="目标类型">
                    <Select
                      allowClear
                      placeholder="选择目标类型"
                      options={[
                        { label: 'string', value: 'string' },
                        { label: 'int', value: 'int' },
                        { label: 'float', value: 'float' },
                        { label: 'datetime', value: 'datetime' },
                      ]}
                    />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="dedupeColumns" label="去重字段">
                    <Select mode="multiple" allowClear options={columns.map(toOption)} placeholder="留空则整行去重" />
                  </Form.Item>
                </Col>
                <Col xs={24}>
                  <Form.Item name="selectColumns" label="输出字段选择">
                    <Select mode="multiple" allowClear options={columns.map(toOption)} placeholder="选择最终保留的字段，留空则保留全部" />
                  </Form.Item>
                </Col>
              </Row>

              <Space>
                <Button type="primary" htmlType="submit" icon={<NodeIndexOutlined />} loading={running}>
                  运行预处理
                </Button>
                <Text type="secondary">字段映射会在预处理前自动应用。</Text>
              </Space>

              <Card size="small" className="nested-card top-gap" title="当前步骤链预览">
                {pendingSteps.length ? (
                  <Space wrap>
                    {pendingSteps.map((step, index) => (
                      <Tag key={`${step}-${index}`} color="gold">
                        {index + 1}. {step}
                      </Tag>
                    ))}
                  </Space>
                ) : (
                  <Text type="secondary">当前只会应用字段映射，还没有额外的预处理步骤。</Text>
                )}
              </Card>
            </Form>
          ) : (
            <Empty description="选择一个数据集后才能配置预处理。" />
          )}
        </Card>
      </Col>

      <Col xs={24} xl={10}>
        <Card title="预处理版本">
          <List
            loading={listLoading}
            locale={{ emptyText: '当前数据集还没有预处理版本。' }}
            dataSource={pipelines}
            renderItem={(pipeline) => (
              <List.Item
                className={pipeline.id === selectedPipelineId ? 'selectable-row is-selected' : 'selectable-row'}
                onClick={() => onSelectPipeline(pipeline.id)}
              >
                <List.Item.Meta
                  title={
                    <Space wrap>
                      <Text strong>{pipeline.name}</Text>
                      <Tag color={pipeline.status === 'completed' ? 'green' : 'processing'}>{pipeline.status}</Tag>
                    </Space>
                  }
                  description={`输出 ${pipeline.output_row_count} 行，步骤数 ${pipeline.steps.length}`}
                />
              </List.Item>
            )}
          />
        </Card>
      </Col>

      <Col span={24}>
        <Card title="预处理输出预览" extra={selectedPipeline ? <Tag color="geekblue">{selectedPipeline.name}</Tag> : null}>
          {selectedPipeline ? (
            <Space direction="vertical" size={16} style={{ width: '100%' }}>
              <Descriptions
                column={{ xs: 1, md: 2 }}
                items={[
                  { key: 'rows', label: '输出行数', children: selectedPipeline.output_row_count },
                  { key: 'steps', label: '步骤数', children: selectedPipeline.steps.length },
                  { key: 'output', label: '输出文件', children: selectedPipeline.output_path ?? '未生成' },
                  {
                    key: 'summary',
                    label: '步骤摘要',
                    children: selectedPipeline.steps.length ? selectedPipeline.steps.map((step) => step.type).join(' -> ') : '仅应用字段映射',
                  },
                ]}
              />
              <Table<Record<string, unknown>>
                rowKey={(_, index) => String(index)}
                loading={previewLoading}
                columns={previewColumns}
                dataSource={preview?.rows ?? []}
                pagination={{ pageSize: 5, hideOnSinglePage: true }}
                scroll={{ x: 900 }}
                size="small"
              />
            </Space>
          ) : (
            <Empty description="执行一个预处理流水线后，这里会显示输出预览。" />
          )}
        </Card>
      </Col>
    </Row>
  )
}

function formatPreviewValue(value: unknown) {
  if (value === null || value === undefined || value === '') {
    return <Text type="secondary">-</Text>
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}

function toOption(column: string) {
  return { label: column, value: column }
}

function buildPendingSteps(values: PreprocessFormValues | undefined) {
  if (!values) {
    return []
  }

  const steps: string[] = []
  if (values.fillColumns?.length && values.fillValue !== undefined && values.fillValue !== '') {
    steps.push(`空值填充(${values.fillColumns.join(', ')})`)
  }
  if (values.castColumn && values.castType) {
    steps.push(`类型转换(${values.castColumn} -> ${values.castType})`)
  }
  if (values.dedupeColumns?.length) {
    steps.push(`按字段去重(${values.dedupeColumns.join(', ')})`)
  }
  if (values.selectColumns?.length) {
    steps.push(`输出字段选择(${values.selectColumns.join(', ')})`)
  }
  return steps
}
