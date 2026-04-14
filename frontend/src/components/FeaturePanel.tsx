import { Button, Card, Col, Descriptions, Empty, Form, Input, List, Row, Select, Space, Table, Tag, Typography } from 'antd'
import type { FormInstance } from 'antd'
import { ControlOutlined } from '@ant-design/icons'

import type { FeaturePipeline, FeaturePreviewRead, PreprocessPipeline } from '../types'

const { Text } = Typography

export type FeatureFormValues = {
  name: string
  preprocessPipelineId?: number
  selectColumns?: string[]
  timeColumn?: string
  timePrefix?: string
  textLengthColumn?: string
  textLengthOutput?: string
  frequencyColumn?: string
  frequencyOutput?: string
}

type Props = {
  enabled: boolean
  columns: string[]
  preprocessPipelines: PreprocessPipeline[]
  form: FormInstance<FeatureFormValues>
  running: boolean
  pipelines: FeaturePipeline[]
  selectedPipelineId: number | null
  selectedPipeline: FeaturePipeline | null
  preview: FeaturePreviewRead | null
  listLoading: boolean
  previewLoading: boolean
  onRun: (values: FeatureFormValues) => void
  onSelectPipeline: (pipelineId: number) => void
}

export function FeaturePanel({
  enabled,
  columns,
  preprocessPipelines,
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
  const pendingSteps = buildPendingSteps(watchedValues)
  const previewColumns = (preview?.columns ?? []).map((column) => ({
    title: column,
    dataIndex: column,
    key: column,
    ellipsis: true,
    render: (value: unknown) => formatPreviewValue(value),
  }))

  return (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={14}>
        <Card title="特征工程编排">
          {enabled ? (
            <Form form={form} layout="vertical" onFinish={onRun}>
              <Row gutter={16}>
                <Col xs={24} md={12}>
                  <Form.Item name="name" label="特征流水线名称" rules={[{ required: true, message: '请输入特征流水线名称' }]}>
                    <Input placeholder="例如：dataset-v1-features-1" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="preprocessPipelineId" label="输入预处理版本">
                    <Select
                      allowClear
                      placeholder="可选，默认直接基于当前数据集"
                      options={preprocessPipelines.map((pipeline) => ({ label: pipeline.name, value: pipeline.id }))}
                    />
                  </Form.Item>
                </Col>
                <Col xs={24}>
                  <Form.Item name="selectColumns" label="保留字段">
                    <Select mode="multiple" allowClear options={columns.map(toOption)} placeholder="先挑出要参与建模的字段" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="timeColumn" label="时间特征字段">
                    <Select allowClear options={columns.map(toOption)} placeholder="从时间字段派生 hour/dayofweek" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="timePrefix" label="时间特征前缀">
                    <Input placeholder="默认使用原字段名" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="textLengthColumn" label="文本长度字段">
                    <Select allowClear options={columns.map(toOption)} placeholder="为文本列生成长度特征" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="textLengthOutput" label="文本长度输出名">
                    <Input placeholder="默认 xxx_length" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="frequencyColumn" label="频次编码字段">
                    <Select allowClear options={columns.map(toOption)} placeholder="为分类列生成出现频次特征" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="frequencyOutput" label="频次编码输出名">
                    <Input placeholder="默认 xxx_freq" />
                  </Form.Item>
                </Col>
              </Row>

              <Space>
                <Button type="primary" htmlType="submit" icon={<ControlOutlined />} loading={running}>
                  运行特征工程
                </Button>
                <Text type="secondary">特征步骤会基于当前数据集或选定的预处理输出执行。</Text>
              </Space>

              <Card size="small" className="nested-card top-gap" title="当前特征步骤链预览">
                {pendingSteps.length ? (
                  <Space wrap>
                    {pendingSteps.map((step, index) => (
                      <Tag key={`${step}-${index}`} color="cyan">
                        {index + 1}. {step}
                      </Tag>
                    ))}
                  </Space>
                ) : (
                  <Text type="secondary">当前还没有额外的特征步骤。</Text>
                )}
              </Card>
            </Form>
          ) : (
            <Empty description="先准备一个数据集，必要时执行预处理，再继续做特征工程。" />
          )}
        </Card>
      </Col>

      <Col xs={24} xl={10}>
        <Card title="特征版本">
          <List
            loading={listLoading}
            locale={{ emptyText: '当前数据集还没有特征版本。' }}
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
        <Card title="特征输出预览" extra={selectedPipeline ? <Tag color="purple">{selectedPipeline.name}</Tag> : null}>
          {selectedPipeline ? (
            <Space direction="vertical" size={16} style={{ width: '100%' }}>
              <Descriptions
                column={{ xs: 1, md: 2 }}
                items={[
                  { key: 'rows', label: '输出行数', children: selectedPipeline.output_row_count },
                  { key: 'steps', label: '步骤数', children: selectedPipeline.steps.length },
                  { key: 'input', label: '输入预处理版本', children: selectedPipeline.preprocess_pipeline_id ?? '直接基于数据集' },
                  {
                    key: 'summary',
                    label: '步骤摘要',
                    children: selectedPipeline.steps.length ? selectedPipeline.steps.map((step) => step.type).join(' -> ') : '未配置步骤',
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
            <Empty description="运行一个特征流水线后，这里会显示输出预览。" />
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

function buildPendingSteps(values: FeatureFormValues | undefined) {
  if (!values) {
    return []
  }

  const steps: string[] = []
  if (values.selectColumns?.length) {
    steps.push(`字段选择(${values.selectColumns.join(', ')})`)
  }
  if (values.timeColumn) {
    steps.push(`时间派生(${values.timeColumn})`)
  }
  if (values.textLengthColumn) {
    steps.push(`文本长度(${values.textLengthColumn})`)
  }
  if (values.frequencyColumn) {
    steps.push(`频次编码(${values.frequencyColumn})`)
  }
  return steps
}
