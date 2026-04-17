import { useEffect, useMemo, useRef, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Collapse,
  Descriptions,
  Divider,
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
  PreprocessFieldIssueGroup,
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
  const [selectedIssueGroup, setSelectedIssueGroup] = useState<string | null>(null)
  const [selectedActionFilter, setSelectedActionFilter] = useState<string | null>(null)
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

  const issueGroups = props.advisor?.issue_groups ?? []
  const fieldsRoutedToFeatureEngineering = useMemo(
    () => props.advisor?.field_advice.filter((item) => item.recommended_action === 'move_to_feature_engineering') ?? [],
    [props.advisor],
  )
  const stepSummary = draftSteps.map((step, index) => `${index + 1}. ${describeStep(step)}`)
  const groupedColumns = useMemo(
    () => buildFieldIssueBuckets(props.columns, props.advisor?.field_advice ?? []),
    [props.columns, props.advisor?.field_advice],
  )
  const recommendationCards = useMemo(
    () => buildRecommendationCards(props.advisor?.recommended_steps ?? [], props.advisor?.field_advice ?? []),
    [props.advisor?.recommended_steps, props.advisor?.field_advice],
  )
  const activeIssueGroup = useMemo(
    () => issueGroups.find((group) => group.issue_type === selectedIssueGroup) ?? issueGroups[0] ?? null,
    [issueGroups, selectedIssueGroup],
  )
  const visibleFieldAdvice = useMemo(() => {
    const allowedFields = new Set(activeIssueGroup?.fields ?? [])
    const source = props.advisor?.field_advice ?? []
    return source.filter((record) => {
      if (activeIssueGroup && !allowedFields.has(record.field)) return false
      if (selectedActionFilter && record.recommended_action !== selectedActionFilter) return false
      return true
    })
  }, [activeIssueGroup, props.advisor?.field_advice, selectedActionFilter])
  const visibleRecommendationCards = useMemo(() => {
    const allowedFields = new Set(activeIssueGroup?.fields ?? [])
    return recommendationCards.filter((card) => {
      if (activeIssueGroup && !card.fields.some((field) => allowedFields.has(field))) return false
      if (selectedActionFilter && card.action !== selectedActionFilter) return false
      return true
    })
  }, [activeIssueGroup, recommendationCards, selectedActionFilter])
  const pendingAffectedFieldCount = useMemo(
    () => new Set(pendingRecommendedSteps.flatMap((recommendation) => recommendation.step.input_selector?.columns ?? [])).size,
    [pendingRecommendedSteps],
  )

  return (
    <StageLayout
      main={
        <Space direction="vertical" size={20} className="full-width">
          <Card title="第一步：字段整理与训练建议">
            {props.dataset ? (
              <Form form={form} layout="vertical" onFinish={props.onRun} initialValues={{ steps: [] }}>
                <Space direction="vertical" size={16} className="full-width">
                  <Alert
                    type="info"
                    showIcon
                    message="先在这里完成字段整理，再把不适合直接训练的字段承接到特征工程。"
                    description="这一段把字段问题、推荐动作和待应用托盘放在同一条操作流里。你先看字段属于哪类问题，再决定要补空值、转类型、排除，还是改走特征工程。"
                  />
                  <div className="wizard-chip-row">
                    <Tag color="blue">补空值</Tag>
                    <Tag color="geekblue">类型转换</Tag>
                    <Tag color="purple">重命名</Tag>
                    <Tag color="gold">保留 / 移除字段</Tag>
                  </div>
                  <Space wrap>
                    <Button
                      size="small"
                      loading={props.sampledAdvisorLoading}
                      onClick={() => props.onRunSampledAdvisor(form.getFieldsValue(true))}
                    >
                      运行采样训练适配分析
                    </Button>
                    <Text type="secondary">
                      快速建议会自动刷新；当你需要更接近训练阶段的判断时，再主动跑采样分析。
                    </Text>
                  </Space>
                  <div className="issue-bucket-grid">
                    <Card
                      size="small"
                      className={`issue-bucket-card ${activeIssueGroup?.issue_type === 'direct_trainable' ? 'is-active' : ''}`}
                      title="可直接训练"
                      onClick={() => {
                        setSelectedIssueGroup('direct_trainable')
                        setSelectedActionFilter(null)
                      }}
                    >
                      <Text type="secondary">这类字段当前风险较低，可以先保留。</Text>
                      <div className="tag-wall">
                        {groupedColumns.directTrainable.length
                          ? groupedColumns.directTrainable.map((column) => <Tag color="green" key={column}>{column}</Tag>)
                          : <Text type="secondary">暂无</Text>}
                      </div>
                    </Card>
                    <Card
                      size="small"
                      className={`issue-bucket-card ${activeIssueGroup?.issue_type === 'needs_cleaning' ? 'is-active' : ''}`}
                      title="建议先清洗"
                      onClick={() => {
                        setSelectedIssueGroup('needs_cleaning')
                        setSelectedActionFilter(null)
                      }}
                    >
                      <Text type="secondary">这类字段更适合先补空值或转类型，再进入训练。</Text>
                      <div className="tag-wall">
                        {groupedColumns.needsCleaning.length
                          ? groupedColumns.needsCleaning.map((column) => <Tag color="gold" key={column}>{column}</Tag>)
                          : <Text type="secondary">暂无</Text>}
                      </div>
                    </Card>
                    <Card
                      size="small"
                      className={`issue-bucket-card ${activeIssueGroup?.issue_type === 'route_to_features' ? 'is-active' : ''}`}
                      title="建议改走特征工程"
                      onClick={() => {
                        setSelectedIssueGroup('route_to_features')
                        setSelectedActionFilter(null)
                      }}
                    >
                      <Text type="secondary">这类字段原值不适合直接训练，但很适合生成统计、复杂度或行为追踪特征。</Text>
                      <div className="tag-wall">
                        {groupedColumns.routeToFeatures.length
                          ? groupedColumns.routeToFeatures.map((column) => <Tag color="purple" key={column}>{column}</Tag>)
                          : <Text type="secondary">暂无</Text>}
                      </div>
                    </Card>
                    <Card
                      size="small"
                      className={`issue-bucket-card ${activeIssueGroup?.issue_type === 'remove_from_output' ? 'is-active' : ''}`}
                      title="建议移除"
                      onClick={() => {
                        setSelectedIssueGroup('remove_from_output')
                        setSelectedActionFilter(null)
                      }}
                    >
                      <Text type="secondary">这类字段大多没有稳定信号，或容易干扰训练。</Text>
                      <div className="tag-wall">
                        {groupedColumns.removeFromOutput.length
                          ? groupedColumns.removeFromOutput.map((column) => <Tag color="red" key={column}>{column}</Tag>)
                          : <Text type="secondary">暂无</Text>}
                      </div>
                    </Card>
                  </div>
                  <Card
                    size="small"
                    className="nested-card"
                    title={activeIssueGroup ? `字段建议层：${activeIssueGroup.title}` : '字段建议层'}
                    extra={activeIssueGroup ? <Tag color={getIssueGroupColor(activeIssueGroup.issue_type)}>{activeIssueGroup.fields.length} 个字段</Tag> : null}
                    loading={props.advisorLoading}
                  >
                    {props.advisor ? (
                      <Space direction="vertical" size={16} className="full-width">
                        <Text type="secondary">{activeIssueGroup?.description ?? '正在根据当前字段和步骤链整理问题分组。'}</Text>
                        <div className="action-card-grid">
                          {visibleRecommendationCards.length ? (
                            visibleRecommendationCards.map((card) => (
                              <Card
                                key={card.id}
                                size="small"
                                className={`action-card ${selectedActionFilter === card.action ? 'is-active' : ''}`}
                                title={card.title}
                                extra={(
                                  card.action === 'move_to_feature_engineering' ? (
                                    <Button
                                      size="small"
                                      type="default"
                                      onClick={() => setSelectedActionFilter((current) => current === card.action ? null : card.action)}
                                    >
                                      查看承接字段
                                    </Button>
                                  ) : (
                                    <Button
                                      size="small"
                                      type="default"
                                      onClick={() => queueRecommendedStep(card.recommendation)}
                                    >
                                      加入待应用托盘
                                    </Button>
                                  )
                                )}
                                onClick={() => setSelectedActionFilter((current) => current === card.action ? null : card.action)}
                              >
                                <Space direction="vertical" size={8} className="full-width">
                                  <Text type="secondary">{card.description}</Text>
                                  <div className="tag-wall">
                                    {card.fields.map((field) => <Tag key={`${card.id}-${field}`}>{field}</Tag>)}
                                  </div>
                                  <Text type="secondary">
                                    {card.action === 'move_to_feature_engineering'
                                      ? '这些字段会直接承接到特征页对应入口，不会写入预处理步骤链。'
                                      : `将生成：${describePersistedStep(card.recommendation.step)}`}
                                  </Text>
                                </Space>
                              </Card>
                            ))
                          ) : (
                            <Empty description="当前问题组没有可直接加入托盘的建议动作。" />
                          )}
                        </div>
                        <Card size="small" title={selectedActionFilter ? `字段明细：${getRecommendedActionLabel(selectedActionFilter)}` : '字段明细'}>
                          <Space direction="vertical" size={12} className="full-width">
                            {visibleFieldAdvice.length ? visibleFieldAdvice.map((record) => (
                              <Card key={record.field} size="small" type="inner">
                                <Space direction="vertical" size={8} className="full-width">
                                  <Space wrap>
                                    <Tag color={getAdviceStatusColor(record.status)}>{getAdviceStatusLabel(record.status)}</Tag>
                                    <Text strong>{record.field}</Text>
                                    <Tag>{getRecommendedActionLabel(record.recommended_action)}</Tag>
                                  </Space>
                                  <Text type="secondary">{record.reason_text}</Text>
                                  <Space wrap>
                                    {record.feature_handoff ? (
                                      <Button
                                        size="small"
                                        onClick={() => props.onFeatureHandoff(
                                          buildFeatureHandoff(record, activeIssueGroup),
                                        )}
                                      >
                                        {getFeatureHandoffLabel(record.feature_handoff)}
                                      </Button>
                                    ) : null}
                                    {findRecommendationForField(props.advisor?.recommended_steps ?? [], record.field) ? (
                                      <Button
                                        size="small"
                                        onClick={() => queueRecommendedStep(findRecommendationForField(props.advisor?.recommended_steps ?? [], record.field)!)}
                                      >
                                        加入待应用托盘
                                      </Button>
                                    ) : null}
                                  </Space>
                                </Space>
                              </Card>
                            )) : (
                              <Empty description="当前筛选条件下没有字段明细。" />
                            )}
                          </Space>
                        </Card>
                        <Card
                          size="small"
                          title="待应用托盘"
                          extra={pendingRecommendedSteps.length ? <Tag color="purple">{pendingRecommendedSteps.length} 条建议</Tag> : null}
                        >
                          <Space direction="vertical" size={12} className="full-width">
                            {pendingRecommendedSteps.length ? (
                              pendingRecommendedSteps.map((recommendation) => (
                                <div key={recommendation.recommendation_id} className="pending-recommendation-row">
                                  <div className="pending-recommendation-copy">
                                    <Space wrap>
                                      <Tag color="purple">{recommendation.title}</Tag>
                                      <Tag>{describePersistedStep(recommendation.step)}</Tag>
                                    </Space>
                                    <Text type="secondary">{recommendation.description}</Text>
                                  </div>
                                  <Button size="small" onClick={() => removePendingRecommendation(recommendation.recommendation_id)}>
                                    移除
                                  </Button>
                                </div>
                              ))
                            ) : (
                              <Text type="secondary">建议动作会先进入托盘，确认后再写入当前预处理草稿。</Text>
                            )}
                            <Space wrap>
                              <Button type="default" disabled={!pendingRecommendedSteps.length} onClick={applyPendingRecommendations}>
                                应用到预处理草稿
                              </Button>
                              <Button disabled={!pendingRecommendedSteps.length} onClick={() => setPendingRecommendedSteps([])}>
                                清空托盘
                              </Button>
                            </Space>
                          </Space>
                        </Card>
                      </Space>
                    ) : (
                      <Text type="secondary">正在根据当前字段和步骤链生成训练影响建议。</Text>
                    )}
                  </Card>
                </Space>

                <Divider>基础整理与专家微调</Divider>
                <Form.Item name="name" label="流水线名称" rules={[{ required: true, message: '请输入流水线名称' }]}>
                  <Input placeholder="例如：dataset-v1-prep-2" />
                </Form.Item>

                <Collapse
                  items={[
                    {
                      key: 'advanced-steps',
                      label: '专家微调（当推荐动作不够时，再精细编辑步骤）',
                      children: (
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
                                  这里保留专家模式。你可以对同一个字段连续做多次处理，也可以把结果写到新字段。
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
                                <Empty description="如果基础整理不够用，再新增一个高级步骤。" />
                              )}
                            </Space>
                          )}
                        </Form.List>
                      ),
                    },
                  ]}
                />
              </Form>
            ) : (
              <Empty description="先在数据页准备一个数据集。" />
            )}
          </Card>

          <Card title="第二步：确认预处理输出">
            <Space direction="vertical" size={16} className="full-width">
              <Descriptions
                column={1}
                items={[
                  { key: 'steps', label: '当前步骤链摘要', children: stepSummary.length ? stepSummary.join(' / ') : '当前还没有高级步骤，默认只应用字段映射。' },
                  {
                    key: 'feature-route',
                    label: '将继续进入特征工程的字段',
                    children: fieldsRoutedToFeatureEngineering.length ? (
                      <div className="tag-wall">
                        {fieldsRoutedToFeatureEngineering.map((item) => (
                          <Tag color="purple" key={`feature-route-${item.field}`}>{item.field}</Tag>
                        ))}
                      </div>
                    ) : '暂无',
                  },
                  {
                    key: 'sampled-status',
                    label: '采样训练适配分析',
                    children: props.sampledAdvisorRun?.result
                      ? `${props.sampledAdvisorRun.status} / ${props.sampledAdvisorRun.sample_size || props.sampledAdvisorRun.result.sample_size} 行`
                      : props.sampledAdvisorLoading
                        ? '正在后台运行采样分析'
                        : '未运行',
                  },
                ]}
              />
              <Space>
                <Button type="primary" icon={<NodeIndexOutlined />} loading={props.running} onClick={() => form.submit()}>
                  运行预处理
                </Button>
                <Text type="secondary">字段映射会在预处理前自动应用，运行完成后会刷新预处理版本。</Text>
              </Space>
            </Space>
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
              title="解释与确认"
              extra={props.advisor ? <Tag color="cyan">{props.advisor.analysis_mode === 'quick' ? '快速建议' : '采样建议'}</Tag> : null}
              loading={props.advisorLoading}
            >
              {props.advisor ? (
                <Space direction="vertical" size={12} className="full-width">
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
                  <Card size="small" title="待应用托盘摘要">
                    <Descriptions
                      column={1}
                      items={[
                        { key: 'tray-count', label: '待应用建议', children: pendingRecommendedSteps.length },
                        { key: 'tray-fields', label: '影响字段数', children: pendingAffectedFieldCount },
                      ]}
                    />
                  </Card>
                  <div>
                    <Text strong>建议继续进入特征工程：</Text>
                    <div className="tag-wall">
                      {fieldsRoutedToFeatureEngineering.length
                        ? fieldsRoutedToFeatureEngineering.map((item) => <Tag color="purple" key={`sidebar-feature-${item.field}`}>{item.field}</Tag>)
                        : <Text type="secondary">暂无</Text>}
                    </div>
                  </div>
                  {props.sampledAdvisorRun?.result ? (
                    <Card size="small" title="采样分析摘要">
                      <Descriptions
                        column={1}
                        items={[
                          { key: 'sample-status', label: '状态', children: <Tag color={props.sampledAdvisorRun.status === 'completed' ? 'green' : props.sampledAdvisorRun.status === 'failed' ? 'red' : 'processing'}>{props.sampledAdvisorRun.status}</Tag> },
                          { key: 'sample-size', label: '采样行数', children: props.sampledAdvisorRun.sample_size || props.sampledAdvisorRun.result.sample_size },
                          { key: 'sample-fields', label: '建议训练字段', children: props.sampledAdvisorRun.result.summary.suggested_training_columns.join(', ') || '无' },
                        ]}
                      />
                    </Card>
                  ) : null}
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

function getIssueGroupColor(issueType: PreprocessFieldIssueGroup['issue_type']) {
  if (issueType === 'direct_trainable') return 'green'
  if (issueType === 'needs_cleaning') return 'gold'
  if (issueType === 'route_to_features') return 'purple'
  if (issueType === 'remove_from_output') return 'red'
  return 'default'
}

function getFeatureHandoffLabel(handoff: FeatureHandoff) {
  if (handoff.task_category === 'text_complexity') return '改走文本特征'
  if (handoff.task_category === 'high_cardinality') return '改走高基数特征'
  if (handoff.task_category === 'behavior_tracking') return '改走行为追踪特征'
  return '带入推荐特征方案'
}

function buildFeatureHandoff(record: FieldAdvice, issueGroup: PreprocessFieldIssueGroup | null): FeatureHandoff {
  return {
    ...(record.feature_handoff as FeatureHandoff),
    source_issue_group_title: issueGroup?.title,
    source_reason_text: record.reason_text,
  }
}

function buildRecommendationCards(
  recommendations: RecommendedPreprocessStepDraft[],
  fieldAdvice: FieldAdvice[],
) {
  const featureFields = fieldAdvice.filter((record) => record.recommended_action === 'move_to_feature_engineering')
  const cards = recommendations.map((recommendation) => {
    const stepType = recommendation.step.step_type ?? recommendation.step.type
    const targetType = recommendation.step.params?.target_type
    let action = 'fill_null'
    let title = recommendation.title

    if (stepType === 'cast_type' && targetType === 'datetime') {
      action = 'cast_datetime'
      title = '转成时间'
    } else if (stepType === 'cast_type') {
      action = 'cast_numeric'
      title = '转成数值'
    } else if (stepType === 'select_columns') {
      action = 'exclude_column'
      title = '排除字段'
    } else if (stepType === 'fill_null') {
      action = 'fill_null'
      title = '补空值'
    }

    return {
      id: recommendation.recommendation_id,
      action,
      title,
      description: recommendation.description,
      fields: recommendation.step.input_selector?.columns ?? [],
      recommendation,
    }
  })

  if (featureFields.length) {
    cards.push({
      id: 'feature-handoff',
      action: 'move_to_feature_engineering',
      title: '改走特征工程',
      description: '这些字段原值不适合直接训练，更适合进入文本复杂度、高基数或行为追踪特征流程。',
      fields: featureFields.map((record) => record.field),
      recommendation: {
        recommendation_id: 'feature-handoff',
        title: '改走特征工程',
        description: '通过字段卡片直接跳转到特征页推荐入口。',
        step: {
          step_id: 'feature_handoff_only',
          step_type: 'select_columns',
          params: {},
          input_selector: { mode: 'explicit', columns: [] },
          output_mode: { mode: 'inplace' },
        },
      },
    })
  }

  return cards
}

function findRecommendationForField(
  recommendations: RecommendedPreprocessStepDraft[],
  field: string,
) {
  return recommendations.find((recommendation) => {
    const columns = recommendation.step.input_selector?.columns ?? []
    return Array.isArray(columns) && columns.includes(field)
  })
}

function buildFieldIssueBuckets(columns: string[], fieldAdvice: FieldAdvice[]) {
  const directTrainable = new Set(columns)
  const needsCleaning = new Set<string>()
  const routeToFeatures = new Set<string>()
  const removeFromOutput = new Set<string>()

  for (const advice of fieldAdvice) {
    if (advice.recommended_action === 'move_to_feature_engineering') {
      routeToFeatures.add(advice.field)
      directTrainable.delete(advice.field)
    } else if (advice.recommended_action === 'exclude_column' || advice.recommended_action === 'drop_from_training') {
      removeFromOutput.add(advice.field)
      directTrainable.delete(advice.field)
    } else if (advice.recommended_action === 'fill_null' || advice.recommended_action === 'cast_numeric' || advice.recommended_action === 'cast_datetime') {
      needsCleaning.add(advice.field)
      directTrainable.delete(advice.field)
    }
  }

  return {
    directTrainable: Array.from(directTrainable),
    needsCleaning: Array.from(needsCleaning),
    routeToFeatures: Array.from(routeToFeatures),
    removeFromOutput: Array.from(removeFromOutput),
  }
}
