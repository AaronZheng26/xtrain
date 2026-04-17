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
  const featurePipelineId = Form.useWatch('featurePipelineId', form)
  const preprocessPipelineId = Form.useWatch('preprocessPipelineId', form)
  const exclusionReasons = props.selectedModel?.exclusion_reasons ?? {}
  const selectedInputFeaturePipeline = props.featurePipelines.find((pipeline) => pipeline.id === featurePipelineId) ?? null
  const selectedInputPreprocessPipeline = props.preprocessPipelines.find((pipeline) => pipeline.id === preprocessPipelineId) ?? null
  const featureColumnOptions = selectedInputFeaturePipeline?.output_schema.map((field) => field.name)
    ?? selectedInputPreprocessPipeline?.output_schema.map((field) => field.name)
    ?? props.columns
  const targetColumnOptions = Array.from(
    new Set([
      ...(props.dataset?.schema_snapshot.map((field) => field.name) ?? []),
      ...featureColumnOptions,
    ]),
  )
  const recommendedTrainingColumns = selectedInputFeaturePipeline?.training_candidate_columns ?? []
  const analysisRetainedColumns = selectedInputFeaturePipeline?.analysis_retained_columns ?? []
  const selectionSource = String(props.selectedModel?.report_json?.selection_source ?? '')

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

  useEffect(() => {
    if (!selectedInputFeaturePipeline) {
      return
    }
    form.setFieldValue('featureColumns', selectedInputFeaturePipeline.training_candidate_columns)
  }, [form, selectedInputFeaturePipeline])

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
                  <Select allowClear options={targetColumnOptions.map(toOption)} />
                </Form.Item>
                <Form.Item name="featurePipelineId" label="输入特征版本">
                  <Select allowClear options={props.featurePipelines.map((pipeline) => ({ label: pipeline.name, value: pipeline.id }))} />
                </Form.Item>
                {selectedInputFeaturePipeline ? (
                  <Alert
                    type="info"
                    showIcon
                    message="已切换到特征页推荐训练字段"
                    description={`默认会使用 ${recommendedTrainingColumns.length} 个训练候选字段；${analysisRetainedColumns.length} 个字段仅保留作分析与解释，你仍然可以在下面手动覆盖。`}
                  />
                ) : null}
                <Form.Item name="preprocessPipelineId" label="输入预处理版本">
                  <Select allowClear options={props.preprocessPipelines.map((pipeline) => ({ label: pipeline.name, value: pipeline.id }))} />
                </Form.Item>
                <Form.Item
                  name="featureColumns"
                  label="训练字段"
                  extra={selectedInputFeaturePipeline ? '默认使用特征页推荐训练字段；这里的修改属于手动覆盖。' : '留空时会按当前数据来源使用默认安全筛选。'}
                >
                  <Select mode="multiple" allowClear options={featureColumnOptions.map(toOption)} />
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
              {props.selectedModel.status !== 'completed' ? (
                <Alert
                  type={props.selectedModel.status === 'failed' ? 'error' : 'info'}
                  showIcon
                  message={
                    props.selectedModel.status === 'failed'
                      ? '该训练任务执行失败，请查看任务状态和训练参数后重试。'
                      : '该训练任务正在后台执行，完成后会自动刷新指标和预测结果。'
                  }
                />
              ) : null}
              {props.selectedModel.excluded_feature_columns.length > 0 ? (
                <Alert
                  type="warning"
                  showIcon
                  message={
                    props.selectedModel.feature_pipeline_id
                      ? `有 ${props.selectedModel.excluded_feature_columns.length} 个字段未进入训练，它们主要是分析保留列，或在生成后未通过常量/重复等硬性校验。`
                      : `已自动排除 ${props.selectedModel.excluded_feature_columns.length} 个字段，避免标签泄漏、ID 干扰或高基数字段直接进入训练。`
                  }
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
                  <Descriptions
                    column={1}
                    size="small"
                    items={[
                      {
                        key: 'selectionSource',
                        label: '字段来源',
                        children:
                          selectionSource === 'feature_pipeline_candidates'
                            ? '特征页推荐训练字段'
                            : selectionSource === 'explicit_request'
                              ? '手动覆盖'
                              : selectionSource || '默认筛选',
                      },
                    ]}
                  />
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
                      <Text strong>{props.selectedModel.feature_pipeline_id ? '未纳入本次训练' : '自动排除'}</Text>
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
                props.selectedModel.status === 'completed' && props.analysis ? (
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
              {props.selectedModel.status === 'completed' ? (
                <Table<Record<string, unknown>>
                  rowKey={(_, index) => String(index)}
                  loading={props.previewLoading}
                  columns={buildPreviewColumns(props.preview?.columns ?? [])}
                  dataSource={props.preview?.rows ?? []}
                  pagination={{ pageSize: 5, hideOnSinglePage: true }}
                  scroll={{ x: 1000 }}
                  size="small"
                />
              ) : null}
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
