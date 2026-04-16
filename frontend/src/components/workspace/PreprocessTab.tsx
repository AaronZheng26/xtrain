import { useEffect, useRef, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Descriptions,
  Empty,
  Form,
  Input,
  List,
  Select,
  Space,
  Switch,
  Table,
  Tag,
  Typography,
} from 'antd'
import {
  ArrowDownOutlined,
  ArrowUpOutlined,
  MinusCircleOutlined,
  NodeIndexOutlined,
  PlusOutlined,
} from '@ant-design/icons'

import { DetailPanel } from '../DetailPanel'
import { StageLayout } from '../StageLayout'
import { buildPreviewColumns } from '../../lib/ui'
import type {
  DatasetVersion,
  FieldAdvice,
  FeatureHandoff,
  PreprocessOutputMode,
  PreprocessPipeline,
  PreprocessPreviewRead,
  PreprocessStep,
  PreprocessStepPreviewRead,
  PreprocessTrainingAdvisorRead,
  PreprocessTrainingAdvisorRunRead,
  RecommendedPreprocessStepDraft,
} from '../../types'

const { Text } = Typography
const { TextArea } = Input

type DraftStepType =
  | 'fill_null'
  | 'cast_type'
  | 'trim_text'
  | 'lowercase'
  | 'normalize_datetime'
  | 'filter_rows'
  | 'rename_columns'
  | 'drop_duplicates'
  | 'select_columns'

export type PreprocessFormValues = {
  name: string
  steps: PreprocessStepDraft[]
}

export type PreprocessStepDraft = {
  step_id: string
  step_type: DraftStepType
  enabled: boolean
  input_selector: {
    mode: 'explicit'
    columns: string[]
  }
  params: {
    value?: string
    target_type?: 'string' | 'int' | 'float' | 'datetime'
    input_format?: string
    output_format?: string
    operator?: 'eq' | 'ne' | 'contains' | 'gt' | 'gte' | 'lt' | 'lte' | 'is_null' | 'not_null'
    rename_map?: string
  }
  output_mode: PreprocessOutputMode
}

type Props = {
  dataset: DatasetVersion | null
  columns: string[]
  pipelines: PreprocessPipeline[]
  selectedPipelineId: number | null
  selectedPipeline: PreprocessPipeline | null
  preview: PreprocessPreviewRead | null
  stepPreview: PreprocessStepPreviewRead | null
  stepPreviewLoading: boolean
  listLoading: boolean
  previewLoading: boolean
  running: boolean
  advisor: PreprocessTrainingAdvisorRead | null
  advisorLoading: boolean
  sampledAdvisorRun: PreprocessTrainingAdvisorRunRead | null
  sampledAdvisorLoading: boolean
  onRun: (values: PreprocessFormValues) => void | Promise<void>
  onPreviewStep: (stepIndex: number, values: PreprocessFormValues) => void | Promise<void>
  onAnalyzeAdvisor: (values: PreprocessFormValues) => void | Promise<void>
  onRunSampledAdvisor: (values: PreprocessFormValues) => void | Promise<void>
  onFeatureHandoff: (handoff: FeatureHandoff) => void
  onSelectPipeline: (pipelineId: number) => void
}

const STEP_TYPE_OPTIONS: Array<{ label: string; value: DraftStepType }> = [
  { label: '空值填充', value: 'fill_null' },
  { label: '类型转换', value: 'cast_type' },
  { label: '文本去空白', value: 'trim_text' },
  { label: '文本小写化', value: 'lowercase' },
  { label: '时间标准化', value: 'normalize_datetime' },
  { label: '条件过滤', value: 'filter_rows' },
  { label: '字段重命名', value: 'rename_columns' },
  { label: '按字段去重', value: 'drop_duplicates' },
  { label: '输出字段选择', value: 'select_columns' },
]

export function PreprocessTab(props: Props) {
  const [form] = Form.useForm<PreprocessFormValues>()
  const watchedValues = Form.useWatch([], form)
  const [pendingRecommendedSteps, setPendingRecommendedSteps] = useState<RecommendedPreprocessStepDraft[]>([])
  const lastAdvisorRequestKeyRef = useRef('')

  useEffect(() => {
    if (props.dataset) {
      form.setFieldsValue({
        name: `${props.dataset.version_name}-prep-${props.pipelines.length + 1}`,
        steps: form.getFieldValue('steps') ?? [],
      })
    } else {
      form.resetFields()
    }
  }, [props.dataset, props.pipelines.length, form])

  const draftSteps = watchedValues?.steps ?? []
  const advisorRequestKey = props.dataset
    ? JSON.stringify({
      datasetId: props.dataset.id,
      pipelineCount: props.pipelines.length,
      steps: draftSteps,
    })
    : ''

  function queueRecommendedStep(recommendation: RecommendedPreprocessStepDraft) {
    setPendingRecommendedSteps((current) => (
      current.some((item) => item.recommendation_id === recommendation.recommendation_id)
        ? current
        : [...current, recommendation]
    ))
  }

  function removePendingRecommendation(recommendationId: string) {
    setPendingRecommendedSteps((current) => current.filter((item) => item.recommendation_id !== recommendationId))
  }

  function applyPendingRecommendations() {
    const existingSteps = (form.getFieldValue('steps') ?? []) as PreprocessStepDraft[]
    const mergedSteps = pendingRecommendedSteps.reduce(
      (currentSteps, recommendation) => mergeRecommendedStep(currentSteps, recommendation.step),
      existingSteps,
    )
    form.setFieldValue('steps', mergedSteps)
    setPendingRecommendedSteps([])
  }

  useEffect(() => {
    if (!props.dataset) {
      lastAdvisorRequestKeyRef.current = ''
      const timer = window.setTimeout(() => {
        setPendingRecommendedSteps([])
      }, 0)
      return () => window.clearTimeout(timer)
    }
    if (advisorRequestKey === lastAdvisorRequestKeyRef.current) {
      return
    }
    const timer = window.setTimeout(() => {
      lastAdvisorRequestKeyRef.current = advisorRequestKey
      props.onAnalyzeAdvisor(form.getFieldsValue(true))
    }, 500)
    return () => window.clearTimeout(timer)
  }, [advisorRequestKey, form, props.dataset, props.onAnalyzeAdvisor])

  return (
    <StageLayout
      main={
        <Space direction="vertical" size={20} className="full-width">
          <Card title="预处理步骤链">
            {props.dataset ? (
              <Form form={form} layout="vertical" onFinish={props.onRun} initialValues={{ steps: [] }}>
                <Form.Item name="name" label="流水线名称" rules={[{ required: true, message: '请输入流水线名称' }]}>
                  <Input placeholder="例如：dataset-v1-prep-2" />
                </Form.Item>

                <Form.List name="steps">
                  {(fields, { add, remove, move }) => (
                    <Space direction="vertical" size={16} className="full-width">
                      <Space wrap>
                        <Button
                          icon={<PlusOutlined />}
                          onClick={() => add(createStepDraft())}
                        >
                          新增步骤
                        </Button>
                        <Text type="secondary">
                          每一步都可以同时处理多个字段，也可以把结果写到新字段，方便对同一个字段连续做不同处理。
                        </Text>
                      </Space>

                      {fields.length ? (
                        fields.map((field, index) => {
                          const currentStep = draftSteps[field.name] ?? createStepDraft()
                          const stepType = currentStep.step_type
                          const supportsOutputMode = supportsDerivedOutput(stepType)
                          const selectedColumns = currentStep.input_selector?.columns ?? []
                          const filterOperator = currentStep.params?.operator ?? 'eq'

                          return (
                            <Card
                              key={field.key}
                              size="small"
                              className="preprocess-step-card"
                              title={
                                <Space wrap>
                                  <Tag color="gold">{index + 1}</Tag>
                                  <Text strong>{getStepLabel(stepType)}</Text>
                                </Space>
                              }
                              extra={
                                <Space>
                                  <Button
                                    size="small"
                                    icon={<ArrowUpOutlined />}
                                    disabled={index === 0}
                                    onClick={() => move(index, index - 1)}
                                  />
                                  <Button
                                    size="small"
                                    icon={<ArrowDownOutlined />}
                                    disabled={index === fields.length - 1}
                                    onClick={() => move(index, index + 1)}
                                  />
                                  <Button
                                    size="small"
                                    danger
                                    icon={<MinusCircleOutlined />}
                                    onClick={() => remove(field.name)}
                                  />
                                  <Button
                                    size="small"
                                    type="default"
                                    onClick={() => props.onPreviewStep(index, form.getFieldsValue(true))}
                                  >
                                    预览此步
                                  </Button>
                                </Space>
                              }
                            >
                              <div className="step-grid">
                                <Form.Item name={[field.name, 'step_id']} hidden>
                                  <Input />
                                </Form.Item>

                                <Form.Item name={[field.name, 'step_type']} label="步骤类型" rules={[{ required: true, message: '请选择步骤类型' }]}>
                                  <Select options={STEP_TYPE_OPTIONS} />
                                </Form.Item>

                                <Form.Item name={[field.name, 'enabled']} label="启用" valuePropName="checked">
                                  <Switch />
                                </Form.Item>

                                <Form.Item
                                  name={[field.name, 'input_selector', 'columns']}
                                  label={
                                    stepType === 'select_columns'
                                      ? '保留字段'
                                      : stepType === 'filter_rows'
                                        ? '条件字段'
                                        : '目标字段'
                                  }
                                >
                                  <Select
                                    mode={stepType === 'filter_rows' ? undefined : 'multiple'}
                                    allowClear
                                    options={props.columns.map(toOption)}
                                    placeholder={
                                      stepType === 'select_columns'
                                        ? '选择最终保留的字段'
                                        : stepType === 'filter_rows'
                                          ? '选择一个条件字段'
                                          : '选择一个或多个字段'
                                    }
                                  />
                                </Form.Item>

                                {stepType === 'fill_null' ? (
                                  <Form.Item
                                    name={[field.name, 'params', 'value']}
                                    label="填充值"
                                    rules={[{ required: true, message: '请输入填充值' }]}
                                  >
                                    <Input placeholder="例如：unknown / 0" />
                                  </Form.Item>
                                ) : null}

                                {stepType === 'cast_type' ? (
                                  <Form.Item
                                    name={[field.name, 'params', 'target_type']}
                                    label="目标类型"
                                    rules={[{ required: true, message: '请选择目标类型' }]}
                                  >
                                    <Select
                                      options={[
                                        { label: 'string', value: 'string' },
                                        { label: 'int', value: 'int' },
                                        { label: 'float', value: 'float' },
                                        { label: 'datetime', value: 'datetime' },
                                      ]}
                                    />
                                  </Form.Item>
                                ) : null}

                                {stepType === 'normalize_datetime' ? (
                                  <>
                                    <Form.Item name={[field.name, 'params', 'input_format']} label="输入时间格式">
                                      <Input placeholder="例如：%Y-%m-%d %H:%M:%S，可留空自动识别" />
                                    </Form.Item>
                                    <Form.Item name={[field.name, 'params', 'output_format']} label="输出时间格式">
                                      <Input placeholder="%Y-%m-%d %H:%M:%S" />
                                    </Form.Item>
                                  </>
                                ) : null}

                                {stepType === 'filter_rows' ? (
                                  <>
                                    <Form.Item
                                      name={[field.name, 'params', 'operator']}
                                      label="过滤条件"
                                      rules={[{ required: true, message: '请选择过滤条件' }]}
                                    >
                                      <Select
                                        options={[
                                          { label: '等于', value: 'eq' },
                                          { label: '不等于', value: 'ne' },
                                          { label: '包含', value: 'contains' },
                                          { label: '大于', value: 'gt' },
                                          { label: '大于等于', value: 'gte' },
                                          { label: '小于', value: 'lt' },
                                          { label: '小于等于', value: 'lte' },
                                          { label: '为空', value: 'is_null' },
                                          { label: '非空', value: 'not_null' },
                                        ]}
                                      />
                                    </Form.Item>
                                    {filterOperator !== 'is_null' && filterOperator !== 'not_null' ? (
                                      <Form.Item
                                        name={[field.name, 'params', 'value']}
                                        label="比较值"
                                        rules={[{ required: true, message: '请输入比较值' }]}
                                      >
                                        <Input placeholder="例如：anomaly / 500 / auth" />
                                      </Form.Item>
                                    ) : null}
                                  </>
                                ) : null}

                                {stepType === 'rename_columns' ? (
                                  <Form.Item name={[field.name, 'params', 'rename_map']} label="字段重命名映射">
                                    <TextArea rows={4} placeholder='例如：{"message":"raw_message","timestamp":"event_time"}' />
                                  </Form.Item>
                                ) : null}

                                {supportsOutputMode ? (
                                  <>
                                    <Form.Item name={[field.name, 'output_mode', 'mode']} label="输出方式">
                                      <Select
                                        options={[
                                          { label: '覆盖原字段', value: 'inplace' },
                                          { label: '输出到新字段', value: 'new_column' },
                                        ]}
                                      />
                                    </Form.Item>

                                    {currentStep.output_mode?.mode === 'new_column' ? (
                                      selectedColumns.length <= 1 ? (
                                        <Form.Item name={[field.name, 'output_mode', 'output_column']} label="输出字段名">
                                          <Input placeholder="留空则自动追加后缀" />
                                        </Form.Item>
                                      ) : null
                                    ) : null}

                                    {currentStep.output_mode?.mode === 'new_column' ? (
                                      <Form.Item name={[field.name, 'output_mode', 'suffix']} label="输出后缀">
                                        <Input placeholder="_clean / _trim / _norm" />
                                      </Form.Item>
                                    ) : null}
                                  </>
                                ) : null}
                              </div>

                              <Card size="small" className="nested-card" title="步骤说明">
                                <Space wrap>
                                  <Tag>{describeStep(currentStep)}</Tag>
                                  {supportsOutputMode && currentStep.output_mode?.mode === 'new_column' ? (
                                    <Tag color="blue">{describeOutputMode(currentStep.output_mode, selectedColumns.length)}</Tag>
                                  ) : (
                                    <Tag color="default">覆盖原字段</Tag>
                                  )}
                                </Space>
                              </Card>
                            </Card>
                          )
                        })
                      ) : (
                        <Empty description="还没有预处理步骤。你可以先新增一个“空值填充”或“文本去空白”步骤开始。" />
                      )}
                    </Space>
                  )}
                </Form.List>

                <Space className="top-gap">
                  <Button type="primary" htmlType="submit" icon={<NodeIndexOutlined />} loading={props.running}>
                    运行预处理
                  </Button>
                  <Text type="secondary">字段映射会在预处理前自动应用。</Text>
                </Space>
              </Form>
            ) : (
              <Empty description="先在数据页准备一个数据集。" />
            )}
          </Card>

          <Card title="预处理版本">
            <List
              loading={props.listLoading}
              locale={{ emptyText: '当前数据集还没有预处理版本。' }}
              dataSource={props.pipelines}
              renderItem={(pipeline) => (
                <List.Item
                  className={pipeline.id === props.selectedPipelineId ? 'selectable-row is-selected' : 'selectable-row'}
                  onClick={() => props.onSelectPipeline(pipeline.id)}
                >
                  <List.Item.Meta
                    title={
                      <Space>
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
        </Space>
      }
      detail={
        <DetailPanel title="预处理详情" extra={props.selectedPipeline ? <Tag color="gold">{props.selectedPipeline.name}</Tag> : null}>
          <Space direction="vertical" size={16} className="full-width">
            <Card size="small" className="nested-card" title="当前步骤摘要">
              {draftSteps.length ? (
                <Space wrap>
                  {draftSteps.map((step, index) => (
                    <Tag key={step.step_id ?? `${step.step_type}-${index}`} color="gold">
                      {index + 1}. {describeStep(step)}
                    </Tag>
                  ))}
                </Space>
              ) : (
                <Text type="secondary">当前只会应用字段映射，还没有额外的预处理步骤。</Text>
              )}
            </Card>

            <Card
              size="small"
              className="nested-card"
              title="训练影响助手"
              extra={props.advisor ? <Tag color="cyan">{props.advisor.analysis_mode === 'quick' ? '快速建议' : '采样建议'}</Tag> : null}
              loading={props.advisorLoading}
            >
              {props.advisor ? (
                <Space direction="vertical" size={16} className="full-width">
                  <Descriptions
                    column={1}
                    items={[
                      { key: 'basis', label: '分析基于', children: props.advisor.summary.analysis_basis },
                      { key: 'target', label: '目标列', children: props.advisor.summary.target_column ?? '未识别' },
                      { key: 'sample', label: '采样行数', children: props.advisor.sample_size },
                      { key: 'generated', label: '更新时间', children: new Date(props.advisor.generated_at).toLocaleString('zh-CN') },
                    ]}
                  />
                  <Space wrap>
                    <Tag color="green">可直接训练: {props.advisor.summary.direct_trainable_fields}</Tag>
                    <Tag color="volcano">高风险: {props.advisor.summary.high_risk_fields}</Tag>
                    <Tag color="gold">待调整: {props.advisor.summary.pending_fields}</Tag>
                    <Tag>总字段: {props.advisor.summary.total_fields}</Tag>
                  </Space>
                  <Card size="small" title="字段影响列表">
                    <Table<FieldAdvice>
                      rowKey="field"
                      columns={[
                        {
                          title: '字段',
                          dataIndex: 'field',
                          key: 'field',
                          width: 180,
                        },
                        {
                          title: '状态',
                          dataIndex: 'status',
                          key: 'status',
                          width: 150,
                          render: (status: string) => <Tag color={getAdviceStatusColor(status)}>{getAdviceStatusLabel(status)}</Tag>,
                        },
                        {
                          title: '推荐动作',
                          dataIndex: 'recommended_action',
                          key: 'recommended_action',
                          width: 140,
                          render: (action: string) => <Tag>{getRecommendedActionLabel(action)}</Tag>,
                        },
                        {
                          title: '说明',
                          dataIndex: 'reason_text',
                          key: 'reason_text',
                        },
                        {
                          title: '特征承接',
                          key: 'feature_handoff',
                          width: 160,
                          render: (_value: unknown, record: FieldAdvice) => (
                            record.feature_handoff?.issue_type === 'behavior_tracking' ? (
                              <Button size="small" onClick={() => props.onFeatureHandoff(record.feature_handoff!)}>
                                带入行为追踪
                              </Button>
                            ) : (
                              <Text type="secondary">无</Text>
                            )
                          ),
                        },
                      ]}
                      dataSource={props.advisor.field_advice}
                      pagination={{ pageSize: 6, hideOnSinglePage: true }}
                      scroll={{ x: 900 }}
                      size="small"
                    />
                  </Card>
                  <Card size="small" title="建议动作">
                    <Space direction="vertical" size={12} className="full-width">
                      {props.advisor.recommended_steps.length ? (
                        props.advisor.recommended_steps.map((recommendation) => (
                          <Card
                            key={recommendation.recommendation_id}
                            size="small"
                            type="inner"
                            title={recommendation.title}
                            extra={<Button size="small" onClick={() => queueRecommendedStep(recommendation)}>加入待应用草稿</Button>}
                          >
                            <Text type="secondary">{recommendation.description}</Text>
                          </Card>
                        ))
                      ) : (
                        <Text type="secondary">当前步骤链下没有新的推荐预处理草稿。</Text>
                      )}
                    </Space>
                  </Card>
                  {pendingRecommendedSteps.length ? (
                    <Card size="small" title="待加入草稿">
                      <Space direction="vertical" size={12} className="full-width">
                        {pendingRecommendedSteps.map((recommendation) => (
                          <Space key={recommendation.recommendation_id} className="full-width" align="start">
                            <Tag color="purple">{recommendation.title}</Tag>
                            <Text className="full-width">{recommendation.description}</Text>
                            <Button size="small" onClick={() => removePendingRecommendation(recommendation.recommendation_id)}>
                              移除
                            </Button>
                          </Space>
                        ))}
                        <Space>
                          <Button type="primary" onClick={applyPendingRecommendations}>
                            应用到步骤链
                          </Button>
                          <Button onClick={() => setPendingRecommendedSteps([])}>
                            清空
                          </Button>
                        </Space>
                      </Space>
                    </Card>
                  ) : null}
                  <Card
                    size="small"
                    title="采样训练适配分析"
                    extra={
                      <Button
                        size="small"
                        loading={props.sampledAdvisorLoading}
                        onClick={() => props.onRunSampledAdvisor(form.getFieldsValue(true))}
                      >
                        运行采样训练适配分析
                      </Button>
                    }
                  >
                    {props.sampledAdvisorRun?.result ? (
                      <Descriptions
                        column={1}
                        items={[
                          { key: 'status', label: '状态', children: <Tag color={props.sampledAdvisorRun.status === 'completed' ? 'green' : props.sampledAdvisorRun.status === 'failed' ? 'red' : 'processing'}>{props.sampledAdvisorRun.status}</Tag> },
                          { key: 'sample', label: '采样行数', children: props.sampledAdvisorRun.sample_size || props.sampledAdvisorRun.result.sample_size },
                          { key: 'generated', label: '最近完成', children: new Date(props.sampledAdvisorRun.result.generated_at).toLocaleString('zh-CN') },
                          { key: 'suggested', label: '建议训练字段', children: props.sampledAdvisorRun.result.summary.suggested_training_columns.join(', ') || '无' },
                        ]}
                      />
                    ) : (
                      <Alert
                        type="info"
                        showIcon
                        message={props.sampledAdvisorLoading ? '正在后台运行采样训练适配分析…' : '点击按钮后会基于当前步骤链做一次采样训练适配分析。'}
                      />
                    )}
                  </Card>
                </Space>
              ) : (
                <Text type="secondary">正在根据当前字段和步骤链生成训练影响建议。</Text>
              )}
            </Card>

            <Card
              size="small"
              className="nested-card"
              title="步骤级预览"
              loading={props.stepPreviewLoading}
            >
              {props.stepPreview ? (
                <Space direction="vertical" size={16} className="full-width">
                  <Descriptions
                    column={1}
                    items={[
                      {
                        key: 'step',
                        label: '当前步骤',
                        children: `${props.stepPreview.preview_step_index + 1}. ${describePersistedStep(props.stepPreview.step)}`,
                      },
                      {
                        key: 'rows',
                        label: '行数变化',
                        children: `${props.stepPreview.before_row_count} -> ${props.stepPreview.after_row_count}`,
                      },
                      {
                        key: 'added',
                        label: '新增字段',
                        children: props.stepPreview.added_columns.length ? (
                          <Space wrap>{props.stepPreview.added_columns.map((column) => <Tag color="green" key={column}>{column}</Tag>)}</Space>
                        ) : '无',
                      },
                      {
                        key: 'removed',
                        label: '移除字段',
                        children: props.stepPreview.removed_columns.length ? (
                          <Space wrap>{props.stepPreview.removed_columns.map((column) => <Tag color="red" key={column}>{column}</Tag>)}</Space>
                        ) : '无',
                      },
                    ]}
                  />
                  <Card size="small" title="执行前样本">
                    <Table<Record<string, unknown>>
                      rowKey={(_, index) => `before-${index}`}
                      columns={buildPreviewColumns(props.stepPreview.before_columns)}
                      dataSource={props.stepPreview.before_rows}
                      pagination={{ pageSize: 3, hideOnSinglePage: true }}
                      scroll={{ x: 900 }}
                      size="small"
                    />
                  </Card>
                  <Card size="small" title="执行后样本">
                    <Table<Record<string, unknown>>
                      rowKey={(_, index) => `after-${index}`}
                      columns={buildPreviewColumns(props.stepPreview.after_columns)}
                      dataSource={props.stepPreview.after_rows}
                      pagination={{ pageSize: 3, hideOnSinglePage: true }}
                      scroll={{ x: 900 }}
                      size="small"
                    />
                  </Card>
                </Space>
              ) : (
                <Text type="secondary">点击某一步上的“预览此步”，这里会显示执行前后对比。</Text>
              )}
            </Card>

            {props.selectedPipeline ? (
              <>
                {props.selectedPipeline.status !== 'completed' ? (
                  <Alert
                    type={props.selectedPipeline.status === 'failed' ? 'error' : 'info'}
                    showIcon
                    message={props.selectedPipeline.status === 'failed' ? '该预处理任务执行失败，请查看任务消息后重试。' : '该预处理任务正在后台执行，完成后会自动刷新结果。'}
                  />
                ) : null}
                <Descriptions
                  column={1}
                  items={[
                    { key: 'rows', label: '输出行数', children: props.selectedPipeline.output_row_count },
                    { key: 'steps', label: '步骤数', children: props.selectedPipeline.steps.length },
                    { key: 'output', label: '输出文件', children: props.selectedPipeline.output_path ?? '未生成' },
                    {
                      key: 'summary',
                      label: '执行摘要',
                      children: props.selectedPipeline.steps.length
                        ? props.selectedPipeline.steps.map((step, index) => `${index + 1}. ${describePersistedStep(step)}`).join(' / ')
                        : '仅应用字段映射',
                    },
                  ]}
                />
                {props.selectedPipeline.status === 'completed' ? (
                  <Table<Record<string, unknown>>
                    rowKey={(_, index) => String(index)}
                    loading={props.previewLoading}
                    columns={buildPreviewColumns(props.preview?.columns ?? [])}
                    dataSource={props.preview?.rows ?? []}
                    pagination={{ pageSize: 5, hideOnSinglePage: true }}
                    scroll={{ x: 900 }}
                    size="small"
                  />
                ) : null}
              </>
            ) : (
              <Empty description="运行一个预处理流水线后，这里会显示输出预览。" />
            )}
          </Space>
        </DetailPanel>
      }
    />
  )
}

function createStepDraft(stepType: DraftStepType = 'fill_null'): PreprocessStepDraft {
  return {
    step_id: `step_${Math.random().toString(36).slice(2, 10)}`,
    step_type: stepType,
    enabled: true,
    input_selector: {
      mode: 'explicit',
      columns: [],
    },
    params: {},
    output_mode: {
      mode: 'inplace',
      suffix: '_processed',
    },
  }
}

function getStepLabel(stepType: string | undefined) {
  return STEP_TYPE_OPTIONS.find((option) => option.value === stepType)?.label ?? stepType ?? '未命名步骤'
}

function describeStep(step: Partial<PreprocessStepDraft>) {
  const stepType = step.step_type
  const columns = step.input_selector?.columns ?? []

  if (stepType === 'fill_null') {
    return `空值填充(${columns.join(', ') || '未选字段'} -> ${step.params?.value ?? '未设置填充值'})`
  }
  if (stepType === 'cast_type') {
    return `类型转换(${columns.join(', ') || '未选字段'} -> ${step.params?.target_type ?? '未选类型'})`
  }
  if (stepType === 'trim_text') {
    return `文本去空白(${columns.join(', ') || '未选字段'})`
  }
  if (stepType === 'lowercase') {
    return `文本小写化(${columns.join(', ') || '未选字段'})`
  }
  if (stepType === 'normalize_datetime') {
    return `时间标准化(${columns.join(', ') || '未选字段'} -> ${step.params?.output_format ?? '%Y-%m-%d %H:%M:%S'})`
  }
  if (stepType === 'filter_rows') {
    return `条件过滤(${columns[0] || '未选字段'} ${step.params?.operator ?? 'eq'} ${step.params?.value ?? ''})`
  }
  if (stepType === 'rename_columns') {
    return `字段重命名(${step.params?.rename_map ? '已配置映射' : '未配置映射'})`
  }
  if (stepType === 'drop_duplicates') {
    return `按字段去重(${columns.join(', ') || '整行去重'})`
  }
  if (stepType === 'select_columns') {
    return `输出字段选择(${columns.join(', ') || '保留全部字段'})`
  }
  return stepType ?? '未命名步骤'
}

function describePersistedStep(step: PreprocessStep) {
  const stepType = step.step_type ?? step.type
  const columns = step.input_selector?.columns ?? ((step.params.columns as string[] | undefined) ?? (step.params.column ? [String(step.params.column)] : []))

  if (stepType === 'fill_null') {
    return `空值填充(${columns.join(', ') || '未选字段'} -> ${String(step.params.value ?? '未设置填充值')})`
  }
  if (stepType === 'cast_type') {
    return `类型转换(${columns.join(', ') || '未选字段'} -> ${String(step.params.target_type ?? '未选类型')})`
  }
  if (stepType === 'trim_text') {
    return `文本去空白(${columns.join(', ') || '未选字段'})`
  }
  if (stepType === 'lowercase') {
    return `文本小写化(${columns.join(', ') || '未选字段'})`
  }
  if (stepType === 'normalize_datetime') {
    return `时间标准化(${columns.join(', ') || '未选字段'} -> ${String(step.params.output_format ?? '%Y-%m-%d %H:%M:%S')})`
  }
  if (stepType === 'filter_rows') {
    return `条件过滤(${columns[0] || '未选字段'} ${String(step.params.operator ?? 'eq')} ${String(step.params.value ?? '')})`
  }
  if (stepType === 'rename_columns') {
    return `字段重命名(${step.params.rename_map ? '已配置映射' : '未配置映射'})`
  }
  if (stepType === 'drop_duplicates') {
    return `按字段去重(${columns.join(', ') || '整行去重'})`
  }
  if (stepType === 'select_columns') {
    return `输出字段选择(${columns.join(', ') || '保留全部字段'})`
  }
  return stepType ?? '未命名步骤'
}

function describeOutputMode(outputMode: PreprocessOutputMode | undefined, selectedColumnCount: number) {
  if (!outputMode || outputMode.mode !== 'new_column') {
    return '覆盖原字段'
  }
  if (selectedColumnCount <= 1 && outputMode.output_column) {
    return `输出到新字段: ${outputMode.output_column}`
  }
  return `输出到新字段后缀: ${outputMode.suffix || '_processed'}`
}

function supportsDerivedOutput(stepType: DraftStepType | undefined) {
  return (
    stepType === 'fill_null'
    || stepType === 'cast_type'
    || stepType === 'trim_text'
    || stepType === 'lowercase'
    || stepType === 'normalize_datetime'
  )
}

function toOption(column: string) {
  return { label: column, value: column }
}

function toDraftStep(step: PreprocessStep): PreprocessStepDraft {
  return {
    step_id: step.step_id ?? `step_${Math.random().toString(36).slice(2, 10)}`,
    step_type: (step.step_type ?? step.type ?? 'fill_null') as DraftStepType,
    enabled: step.enabled ?? true,
    input_selector: {
      mode: 'explicit',
      columns: step.input_selector?.columns ?? [],
    },
    params: {
      value: typeof step.params.value === 'string' ? step.params.value : step.params.value !== undefined ? String(step.params.value) : undefined,
      target_type: step.params.target_type as PreprocessStepDraft['params']['target_type'] | undefined,
      input_format: step.params.input_format as string | undefined,
      output_format: step.params.output_format as string | undefined,
      operator: step.params.operator as PreprocessStepDraft['params']['operator'] | undefined,
      rename_map: step.params.rename_map ? JSON.stringify(step.params.rename_map, null, 2) : undefined,
    },
    output_mode: {
      mode: (step.output_mode?.mode as PreprocessOutputMode['mode']) ?? 'inplace',
      output_column: step.output_mode?.output_column,
      suffix: step.output_mode?.suffix,
    },
  }
}

function mergeRecommendedStep(currentSteps: PreprocessStepDraft[], step: PreprocessStep): PreprocessStepDraft[] {
  const nextStep = toDraftStep(step)
  if (nextStep.step_type === 'fill_null') {
    const existingIndex = currentSteps.findIndex((currentStep) => currentStep.step_type === 'fill_null' && currentStep.params.value === nextStep.params.value)
    if (existingIndex >= 0) {
      return currentSteps.map((currentStep, index) => (
        index === existingIndex
          ? {
            ...currentStep,
            input_selector: {
              ...currentStep.input_selector,
              columns: dedupeColumns([...(currentStep.input_selector.columns ?? []), ...(nextStep.input_selector.columns ?? [])]),
            },
          }
          : currentStep
      ))
    }
  }

  if (nextStep.step_type === 'cast_type') {
    const existingIndex = currentSteps.findIndex((currentStep) => currentStep.step_type === 'cast_type' && currentStep.params.target_type === nextStep.params.target_type)
    if (existingIndex >= 0) {
      return currentSteps.map((currentStep, index) => (
        index === existingIndex
          ? {
            ...currentStep,
            input_selector: {
              ...currentStep.input_selector,
              columns: dedupeColumns([...(currentStep.input_selector.columns ?? []), ...(nextStep.input_selector.columns ?? [])]),
            },
          }
          : currentStep
      ))
    }
  }

  if (nextStep.step_type === 'select_columns') {
    const existingIndex = currentSteps.findIndex((currentStep) => currentStep.step_type === 'select_columns')
    if (existingIndex >= 0) {
      const nextColumns = new Set(nextStep.input_selector.columns ?? [])
      return currentSteps.map((currentStep, index) => (
        index === existingIndex
          ? {
            ...currentStep,
            input_selector: {
              ...currentStep.input_selector,
              columns: (currentStep.input_selector.columns ?? []).filter((column) => nextColumns.has(column)),
            },
          }
          : currentStep
      ))
    }
  }

  return [...currentSteps, nextStep]
}

function dedupeColumns(columns: string[]) {
  return columns.filter((column, index) => columns.indexOf(column) === index)
}

function getAdviceStatusLabel(status: string) {
  if (status === 'recommended_keep') return '推荐保留'
  if (status === 'suggest_convert') return '建议转换'
  if (status === 'suggest_delete') return '建议删除'
  if (status === 'high_cardinality_risk') return '高基数风险'
  if (status === 'suspected_label_leak') return '疑似标签泄漏'
  if (status === 'raw_text') return '原始大文本'
  if (status === 'suspected_id') return '疑似 ID'
  if (status === 'high_missing') return '高缺失'
  return status
}

function getAdviceStatusColor(status: string) {
  if (status === 'recommended_keep') return 'green'
  if (status === 'suggest_convert') return 'gold'
  if (status === 'suggest_delete') return 'volcano'
  if (status === 'high_cardinality_risk') return 'orange'
  if (status === 'suspected_label_leak') return 'red'
  if (status === 'raw_text') return 'purple'
  if (status === 'suspected_id') return 'magenta'
  if (status === 'high_missing') return 'geekblue'
  return 'default'
}

function getRecommendedActionLabel(action: string) {
  if (action === 'keep') return '直接保留'
  if (action === 'fill_null') return '先补空值'
  if (action === 'cast_numeric') return '转成数值'
  if (action === 'cast_datetime') return '转成时间'
  if (action === 'drop_from_training') return '训练时排除'
  if (action === 'exclude_column') return '预处理中移除'
  if (action === 'move_to_feature_engineering') return '改走特征工程'
  return action
}
