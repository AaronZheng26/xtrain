import { Button, Card, Col, Descriptions, Empty, Form, Input, List, Row, Select, Space, Table, Tag, Typography } from 'antd'
import type { FormInstance } from 'antd'
import { ExperimentOutlined } from '@ant-design/icons'

import type { FeaturePipeline, ModelPreviewRead, ModelVersion, PreprocessPipeline } from '../types'

const { Text } = Typography

export type TrainingFormValues = {
  name: string
  mode: 'supervised' | 'unsupervised'
  algorithm: string
  featurePipelineId?: number
  preprocessPipelineId?: number
  targetColumn?: string
  featureColumns?: string[]
}

type Props = {
  enabled: boolean
  columns: string[]
  form: FormInstance<TrainingFormValues>
  running: boolean
  featurePipelines: FeaturePipeline[]
  preprocessPipelines: PreprocessPipeline[]
  models: ModelVersion[]
  selectedModelId: number | null
  selectedModel: ModelVersion | null
  preview: ModelPreviewRead | null
  listLoading: boolean
  previewLoading: boolean
  onRun: (values: TrainingFormValues) => void
  onSelectModel: (modelId: number) => void
}

export function TrainingPanel({
  enabled,
  columns,
  form,
  running,
  featurePipelines,
  preprocessPipelines,
  models,
  selectedModelId,
  selectedModel,
  preview,
  listLoading,
  previewLoading,
  onRun,
  onSelectModel,
}: Props) {
  const mode = Form.useWatch('mode', form) ?? 'supervised'
  const previewColumns = (preview?.columns ?? []).map((column) => ({
    title: column,
    dataIndex: column,
    key: column,
    ellipsis: true,
    render: (value: unknown) => formatPreviewValue(value),
  }))

  const algorithmOptions =
    mode === 'supervised'
      ? [
          { label: 'Logistic Regression', value: 'logistic_regression' },
          { label: 'Random Forest', value: 'random_forest' },
          { label: 'SVM', value: 'svm' },
        ]
      : [
          { label: 'Isolation Forest', value: 'isolation_forest' },
          { label: 'One-Class SVM', value: 'one_class_svm' },
          { label: 'Local Outlier Factor', value: 'local_outlier_factor' },
        ]

  return (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={14}>
        <Card title="训练编排">
          {enabled ? (
            <Form form={form} layout="vertical" onFinish={onRun} initialValues={{ mode: 'supervised', algorithm: 'logistic_regression' }}>
              <Row gutter={16}>
                <Col xs={24} md={12}>
                  <Form.Item name="name" label="模型版本名称" rules={[{ required: true, message: '请输入模型版本名称' }]}>
                    <Input placeholder="例如：baseline-supervised-v1" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="mode" label="训练模式" rules={[{ required: true, message: '请选择训练模式' }]}>
                    <Select
                      options={[
                        { label: '有监督', value: 'supervised' },
                        { label: '无监督', value: 'unsupervised' },
                      ]}
                    />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="algorithm" label="算法" rules={[{ required: true, message: '请选择算法' }]}>
                    <Select options={algorithmOptions} />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="targetColumn" label="目标/标签列">
                    <Select allowClear options={columns.map(toOption)} placeholder="有监督推荐选择 label 列" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="featurePipelineId" label="输入特征版本">
                    <Select allowClear options={featurePipelines.map((pipeline) => ({ label: pipeline.name, value: pipeline.id }))} placeholder="优先使用特征工程输出" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item name="preprocessPipelineId" label="输入预处理版本">
                    <Select allowClear options={preprocessPipelines.map((pipeline) => ({ label: pipeline.name, value: pipeline.id }))} placeholder="没有特征版本时可用" />
                  </Form.Item>
                </Col>
                <Col span={24}>
                  <Form.Item name="featureColumns" label="训练字段">
                    <Select mode="multiple" allowClear options={columns.map(toOption)} placeholder="留空则默认使用除标签外的所有字段" />
                  </Form.Item>
                </Col>
              </Row>

              <Space>
                <Button type="primary" htmlType="submit" icon={<ExperimentOutlined />} loading={running}>
                  启动训练
                </Button>
                <Text type="secondary">训练会生成模型版本、指标摘要和预测预览。</Text>
              </Space>
            </Form>
          ) : (
            <Empty description="先准备数据集、预处理或特征版本，再继续训练。" />
          )}
        </Card>
      </Col>

      <Col xs={24} xl={10}>
        <Card title="模型版本">
          <List
            loading={listLoading}
            locale={{ emptyText: '当前数据集还没有模型版本。' }}
            dataSource={models}
            renderItem={(model) => (
              <List.Item
                className={model.id === selectedModelId ? 'selectable-row is-selected' : 'selectable-row'}
                onClick={() => onSelectModel(model.id)}
              >
                <List.Item.Meta
                  title={
                    <Space wrap>
                      <Text strong>{model.name}</Text>
                      <Tag color={model.status === 'completed' ? 'green' : model.status === 'failed' ? 'red' : 'processing'}>
                        {model.status}
                      </Tag>
                    </Space>
                  }
                  description={`${model.mode} / ${model.algorithm}`}
                />
              </List.Item>
            )}
          />
        </Card>
      </Col>

      <Col span={24}>
        <Card title="训练结果预览" extra={selectedModel ? <Tag color="green">{selectedModel.name}</Tag> : null}>
          {selectedModel ? (
            <Space direction="vertical" size={16} style={{ width: '100%' }}>
              <Descriptions
                column={{ xs: 1, md: 2 }}
                items={[
                  { key: 'mode', label: '模式', children: selectedModel.mode },
                  { key: 'algorithm', label: '算法', children: selectedModel.algorithm },
                  { key: 'target', label: '标签列', children: selectedModel.target_column ?? '无' },
                  { key: 'features', label: '训练字段数', children: selectedModel.feature_columns.length },
                ]}
              />
              <Card size="small" className="nested-card" title="指标摘要">
                <Space wrap>
                  {Object.entries(preview?.metrics ?? selectedModel.metrics).map(([key, value]) => (
                    <Tag key={key} color="blue">
                      {key}: {String(value)}
                    </Tag>
                  ))}
                </Space>
              </Card>
              <Table<Record<string, unknown>>
                rowKey={(_, index) => String(index)}
                loading={previewLoading}
                columns={previewColumns}
                dataSource={preview?.rows ?? []}
                pagination={{ pageSize: 5, hideOnSinglePage: true }}
                scroll={{ x: 1000 }}
                size="small"
              />
            </Space>
          ) : (
            <Empty description="执行一次训练后，这里会显示指标和预测预览。" />
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
