import { useEffect } from 'react'
import { Alert, Button, Card, Descriptions, Empty, Form, Input, List, Select, Space, Table, Tag, Typography } from 'antd'
import { ExperimentOutlined } from '@ant-design/icons'

import { DetailPanel } from '../DetailPanel'
import { StageLayout } from '../StageLayout'
import { UnsupervisedResultCharts } from '../UnsupervisedResultCharts'
import { buildPreviewColumns } from '../../lib/ui'
import type { TrainingFormValues } from '../TrainingPanel'
import type { DatasetVersion, FeaturePipeline, ModelAnalysisRead, ModelPreviewRead, ModelVersion, PreprocessPipeline } from '../../types'

const { Text } = Typography

type Props = {
  dataset: DatasetVersion | null
  columns: string[]
  featurePipelines: FeaturePipeline[]
  preprocessPipelines: PreprocessPipeline[]
  models: ModelVersion[]
  selectedModelId: number | null
  selectedModel: ModelVersion | null
  preview: ModelPreviewRead | null
  analysis: ModelAnalysisRead | null
  listLoading: boolean
  previewLoading: boolean
  analysisLoading: boolean
  running: boolean
  onRun: (values: TrainingFormValues) => void
  onSelectModel: (modelId: number) => void
}

export function TrainingTab(props: Props) {
  const [form] = Form.useForm<TrainingFormValues>()
  const mode = Form.useWatch('mode', form) ?? 'supervised'
  const exclusionReasons = props.selectedModel?.exclusion_reasons ?? {}

  useEffect(() => {
    if (props.dataset) {
      form.setFieldsValue({
        name: `${props.dataset.version_name}-model-${props.models.length + 1}`,
        mode: 'supervised',
        algorithm: 'logistic_regression',
      })
    } else {
      form.resetFields()
    }
  }, [props.dataset, props.models.length, form])

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
    <StageLayout
      main={
        <Space direction="vertical" size={20} className="full-width">
          <Card title="训练编排">
            {props.dataset ? (
              <Form form={form} layout="vertical" onFinish={props.onRun} initialValues={{ mode: 'supervised', algorithm: 'logistic_regression' }}>
                <Form.Item name="name" label="模型版本名称" rules={[{ required: true, message: '请输入模型版本名称' }]}>
                  <Input />
                </Form.Item>
                <Form.Item name="mode" label="训练模式" rules={[{ required: true, message: '请选择训练模式' }]}>
                  <Select options={[{ label: '有监督', value: 'supervised' }, { label: '无监督', value: 'unsupervised' }]} />
                </Form.Item>
                <Form.Item name="algorithm" label="算法" rules={[{ required: true, message: '请选择算法' }]}>
                  <Select options={algorithmOptions} />
                </Form.Item>
                <Form.Item name="targetColumn" label="目标/标签列">
                  <Select allowClear options={props.columns.map(toOption)} />
                </Form.Item>
                <Form.Item name="featurePipelineId" label="输入特征版本">
                  <Select allowClear options={props.featurePipelines.map((pipeline) => ({ label: pipeline.name, value: pipeline.id }))} />
                </Form.Item>
                <Form.Item name="preprocessPipelineId" label="输入预处理版本">
                  <Select allowClear options={props.preprocessPipelines.map((pipeline) => ({ label: pipeline.name, value: pipeline.id }))} />
                </Form.Item>
                <Form.Item name="featureColumns" label="训练字段">
                  <Select mode="multiple" allowClear options={props.columns.map(toOption)} />
                </Form.Item>
                <Button type="primary" htmlType="submit" icon={<ExperimentOutlined />} loading={props.running}>
                  启动训练
                </Button>
              </Form>
            ) : (
              <Empty description="先准备数据集、预处理或特征版本，再继续训练。" />
            )}
          </Card>
          <Card title="模型版本">
            <List
              loading={props.listLoading}
              locale={{ emptyText: '当前数据集还没有模型版本。' }}
              dataSource={props.models}
              renderItem={(model) => (
                <List.Item
                  className={model.id === props.selectedModelId ? 'selectable-row is-selected' : 'selectable-row'}
                  onClick={() => props.onSelectModel(model.id)}
                >
                  <List.Item.Meta
                    title={<Space><Text strong>{model.name}</Text><Tag color={model.status === 'completed' ? 'green' : model.status === 'failed' ? 'red' : 'processing'}>{model.status}</Tag></Space>}
                    description={`${model.mode} / ${model.algorithm}`}
                  />
                </List.Item>
              )}
            />
          </Card>
        </Space>
      }
      detail={
        <DetailPanel title="训练结果" extra={props.selectedModel ? <Tag color="green">{props.selectedModel.name}</Tag> : null}>
          {props.selectedModel ? (
            <Space direction="vertical" size={16} className="full-width">
              <Descriptions
                column={1}
                items={[
                  { key: 'mode', label: '模式', children: props.selectedModel.mode },
                  { key: 'algorithm', label: '算法', children: props.selectedModel.algorithm },
                  { key: 'target', label: '标签列', children: props.selectedModel.target_column ?? '无' },
                  { key: 'features', label: '实际训练字段数', children: props.selectedModel.used_feature_columns.length || props.selectedModel.feature_columns.length },
                  { key: 'excluded', label: '排除字段数', children: props.selectedModel.excluded_feature_columns.length },
                ]}
              />
              {props.selectedModel.excluded_feature_columns.length > 0 ? (
                <Alert
                  type="warning"
                  showIcon
                  message={`已自动排除 ${props.selectedModel.excluded_feature_columns.length} 个字段，避免标签泄漏、ID 干扰或高基数字段直接进入训练。`}
                />
              ) : null}
              <Card size="small" className="nested-card" title="指标摘要">
                <Space wrap>
                  {Object.entries(props.preview?.metrics ?? props.selectedModel.metrics).map(([key, value]) => (
                    <Tag key={key} color="blue">{key}: {String(value)}</Tag>
                  ))}
                </Space>
              </Card>
              <Card size="small" className="nested-card" title="字段选择摘要">
                <Space direction="vertical" size={12} className="full-width">
                  <div>
                    <Text strong>参与训练</Text>
                    <div className="tag-wall">
                      {(props.selectedModel.used_feature_columns.length > 0 ? props.selectedModel.used_feature_columns : props.selectedModel.feature_columns).map((column) => (
                        <Tag key={`used-${column}`} color="green">{column}</Tag>
                      ))}
                    </div>
                  </div>
                  {props.selectedModel.excluded_feature_columns.length > 0 ? (
                    <div>
                      <Text strong>自动排除</Text>
                      <div className="tag-wall">
                        {props.selectedModel.excluded_feature_columns.map((column) => (
                          <Tag key={`excluded-${column}`} color="volcano">
                            {column}: {exclusionReasons[column] ?? 'excluded'}
                          </Tag>
                        ))}
                      </div>
                    </div>
                  ) : null}
                </Space>
              </Card>
              {props.selectedModel.mode === 'unsupervised' ? (
                props.analysis ? (
                  <Card size="small" className="nested-card" title="异常可视化">
                    <Space direction="vertical" size={12} className="full-width">
                      <Space wrap>
                        <Tag color="red">异常样本: {props.analysis.anomaly_count}</Tag>
                        <Tag color="geekblue">可视化采样: {props.analysis.sample_size}</Tag>
                      </Space>
                      <UnsupervisedResultCharts analysis={props.analysis} />
                    </Space>
                  </Card>
                ) : (
                  <Alert type="info" showIcon message={props.analysisLoading ? '正在生成异常可视化…' : '选择无监督模型后，这里会展示异常分数和二维聚类视图。'} />
                )
              ) : null}
              <Table<Record<string, unknown>>
                rowKey={(_, index) => String(index)}
                loading={props.previewLoading}
                columns={buildPreviewColumns(props.preview?.columns ?? [])}
                dataSource={props.preview?.rows ?? []}
                pagination={{ pageSize: 5, hideOnSinglePage: true }}
                scroll={{ x: 1000 }}
                size="small"
              />
            </Space>
          ) : (
            <Empty description="执行一次训练后，这里会显示指标和预测预览。" />
          )}
        </DetailPanel>
      }
    />
  )
}

function toOption(column: string) {
  return { label: column, value: column }
}
