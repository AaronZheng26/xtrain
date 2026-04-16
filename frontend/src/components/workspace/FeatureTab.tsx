import { useEffect, useMemo } from 'react'
import {
  Alert,
  Button,
  Card,
  Descriptions,
  Empty,
  Form,
  Input,
  List,
  Segmented,
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
  ControlOutlined,
  MinusCircleOutlined,
  PlusOutlined,
  SaveOutlined,
} from '@ant-design/icons'

import { DetailPanel } from '../DetailPanel'
import { StageLayout } from '../StageLayout'
import { buildPreviewColumns } from '../../lib/ui'
import type {
  DatasetVersion,
  FeaturePipeline,
  FeaturePreviewRead,
  FeatureTaskCategory,
  FeatureTaskCategoryId,
  FeatureStep,
  FeatureStepPreviewRead,
  FeatureTemplate,
  FeatureHandoff,
  PreprocessPipeline,
} from '../../types'

const { Text } = Typography
const { TextArea } = Input

type DraftFeatureStepType =
  | 'select_features'
  | 'derive_time_parts'
  | 'text_length'
  | 'byte_length'
  | 'token_count'
  | 'shannon_entropy'
  | 'char_composition'
  | 'unique_char_ratio'
  | 'regex_match_count'
  | 'pattern_flags'
  | 'keyword_count'
  | 'frequency_encode'
  | 'category_encode'
  | 'numeric_bucket'
  | 'numeric_scale'
  | 'ratio_feature'
  | 'difference_feature'
  | 'concat_fields'
  | 'equality_flag'
  | 'group_frequency'
  | 'group_unique_count'
  | 'group_duration'
  | 'group_event_order'
  | 'time_since_previous_event'
  | 'time_until_next_event'
  | 'group_value_change_flag'
  | 'time_window_count'
  | 'window_unique_count'
  | 'window_target_unique_count'
  | 'window_status_change_count'
  | 'window_spike_flag'
  | 'ip_features'
  | 'port_features'
  | 'path_features'
  | 'status_category'
  | 'value_map'
  | 'boolean_flag'

export type FeatureFormValues = {
  mode: 'quick' | 'advanced'
  name: string
  preprocessPipelineId?: number
  quickStrategy?: 'template' | FeatureTaskCategoryId
  templateId?: string
  quickTaskConfig?: {
    targetColumns: string[]
    timeColumn?: string
    groupColumns: string[]
  }
  behaviorTracking?: {
    trackingType: 'flow' | 'entity'
    groupKey?: string
    timeColumn?: string
    targetColumns: string[]
    recipeKinds: Array<'base' | 'window' | 'sequence'>
  }
  steps: FeatureStepDraft[]
  templateSaveName?: string
  templateSaveDescription?: string
  templateSaveLogType?: string
}

export type FeatureStepDraft = {
  step_id: string
  step_type: DraftFeatureStepType
  enabled: boolean
  input_selector: {
    mode: 'explicit' | 'dtype' | 'role_tag' | 'name_pattern'
    columns: string[]
    dtype?: 'string' | 'numeric' | 'datetime'
    role_tag?: 'text' | 'path' | 'user_agent' | 'domain' | 'ip'
    name_pattern?: string
  }
  params: {
    prefix?: string
    keywordsText?: string
    regexPattern?: string
    patternFlags?: Array<'ip' | 'url' | 'hash' | 'hex_like' | 'base64_like' | 'email'>
    bins?: number
    method?: 'zscore' | 'minmax'
    separator?: string
    targetColumn?: string
    timeColumn?: string
    windowMinutes?: number
    threshold?: number
    mappingText?: string
    defaultValue?: string
    operator?: 'contains' | 'eq' | 'ne' | 'gt' | 'lt' | 'is_null' | 'not_null'
    value?: string
  }
  output_mode: {
    mode?: 'append_new_columns' | 'replace_existing'
    output_column?: string
    suffix?: string
  }
}

type Props = {
  projectId: number | null
  dataset: DatasetVersion | null
  preprocessPipelines: PreprocessPipeline[]
  pipelines: FeaturePipeline[]
  templates: FeatureTemplate[]
  templatesLoading: boolean
  selectedPipelineId: number | null
  selectedPipeline: FeaturePipeline | null
  preview: FeaturePreviewRead | null
  stepPreview: FeatureStepPreviewRead | null
  listLoading: boolean
  previewLoading: boolean
  stepPreviewLoading: boolean
  running: boolean
  savingTemplate: boolean
  featureHandoff: FeatureHandoff | null
  onClearFeatureHandoff: () => void
  onRun: (values: FeatureFormValues) => void
  onPreviewStep: (stepIndex: number, values: FeatureFormValues) => void
  onSaveTemplate: (values: FeatureFormValues) => void
  onSelectPipeline: (pipelineId: number) => void
}

const STEP_TYPE_OPTIONS: Array<{ label: string; value: DraftFeatureStepType }> = [
  { label: '字段选择', value: 'select_features' },
  { label: '时间派生', value: 'derive_time_parts' },
  { label: '文本长度', value: 'text_length' },
  { label: '字节长度', value: 'byte_length' },
  { label: 'Token 数', value: 'token_count' },
  { label: '香农熵', value: 'shannon_entropy' },
  { label: '字符组成比例', value: 'char_composition' },
  { label: '唯一字符占比', value: 'unique_char_ratio' },
  { label: '正则命中次数', value: 'regex_match_count' },
  { label: '模式布尔标记', value: 'pattern_flags' },
  { label: '关键词计数', value: 'keyword_count' },
  { label: '频次编码', value: 'frequency_encode' },
  { label: '类别编码', value: 'category_encode' },
  { label: '数值分桶', value: 'numeric_bucket' },
  { label: '数值标准化', value: 'numeric_scale' },
  { label: '字段比值', value: 'ratio_feature' },
  { label: '字段差值', value: 'difference_feature' },
  { label: '字段拼接', value: 'concat_fields' },
  { label: '字段相等标记', value: 'equality_flag' },
  { label: '组内频次', value: 'group_frequency' },
  { label: '组内去重数', value: 'group_unique_count' },
  { label: '组内持续时长', value: 'group_duration' },
  { label: '组内事件序号', value: 'group_event_order' },
  { label: '距离上一事件', value: 'time_since_previous_event' },
  { label: '距离下一事件', value: 'time_until_next_event' },
  { label: '组内值变化标记', value: 'group_value_change_flag' },
  { label: '时间窗计数', value: 'time_window_count' },
  { label: '时间窗去重数', value: 'window_unique_count' },
  { label: '时间窗目标去重数', value: 'window_target_unique_count' },
  { label: '时间窗状态变化数', value: 'window_status_change_count' },
  { label: '时间窗突增标记', value: 'window_spike_flag' },
  { label: 'IP 基础特征', value: 'ip_features' },
  { label: '端口基础特征', value: 'port_features' },
  { label: '路径特征', value: 'path_features' },
  { label: '状态码类别', value: 'status_category' },
  { label: '值映射', value: 'value_map' },
  { label: '布尔标记', value: 'boolean_flag' },
]

const SELECTOR_MODE_OPTIONS = [
  { label: '显式字段', value: 'explicit' },
  { label: '按字段类型', value: 'dtype' },
  { label: '按角色标签', value: 'role_tag' },
  { label: '按名称规则', value: 'name_pattern' },
]

const FEATURE_DTYPE_OPTIONS = [
  { label: '字符串', value: 'string' },
  { label: '数值', value: 'numeric' },
  { label: '时间', value: 'datetime' },
]

const ROLE_TAG_OPTIONS = [
  { label: '文本字段', value: 'text' },
  { label: '路径/URL', value: 'path' },
  { label: 'User-Agent', value: 'user_agent' },
  { label: '域名/主机', value: 'domain' },
  { label: 'IP 字段', value: 'ip' },
]

const PATTERN_FLAG_OPTIONS = [
  { label: 'IP', value: 'ip' },
  { label: 'URL', value: 'url' },
  { label: 'Hash', value: 'hash' },
  { label: 'Hex-like', value: 'hex_like' },
  { label: 'Base64-like', value: 'base64_like' },
  { label: '邮箱', value: 'email' },
]

const LOG_TYPE_OPTIONS = [
  { label: 'Nginx 访问日志', value: 'nginx_access' },
  { label: '程序运行日志', value: 'program_runtime' },
  { label: 'NTA 流量日志', value: 'nta_flow' },
  { label: '通用日志', value: 'generic_log' },
]

const FEATURE_TASK_CATEGORIES: FeatureTaskCategory[] = [
  {
    id: 'text_complexity',
    title: '文本复杂度特征',
    description: '适合原始大文本、命令、URL、域名等字段。把原值转成长度、熵、模式命中等更适合训练的数值特征。',
    recommended_for: ['raw_message', 'message', 'url', 'domain', 'helo', 'user_agent'],
    default_recipe_ids: ['text_complexity_core'],
  },
  {
    id: 'high_cardinality',
    title: '高基数 / 类别特征',
    description: '适合唯一值很多的类别字段。优先生成频次、窗口活跃度等统计特征，而不是直接编码原值。',
    recommended_for: ['request_id', 'session_id', 'user_id', 'path', 'mail_from'],
    default_recipe_ids: ['high_cardinality_frequency', 'high_cardinality_window'],
  },
  {
    id: 'time_behavior',
    title: '时间行为特征',
    description: '适合所有已经标准化好的时间字段。用来生成时间拆分、时间窗活跃度和突增信号。',
    recommended_for: ['event_time', 'timestamp', 'login_time'],
    default_recipe_ids: ['time_behavior_core'],
  },
  {
    id: 'behavior_tracking',
    title: '行为追踪特征',
    description: '适合 request/session 等流程 ID，或 user/host/source_ip 等主体 ID。先按行为追踪意图配置，再由系统生成步骤链。',
    recommended_for: ['request_id', 'session_id', 'source_ip', 'user_id', 'host_id'],
    default_recipe_ids: ['behavior_tracking_base', 'behavior_tracking_window', 'behavior_tracking_sequence'],
  },
  {
    id: 'numeric_statistics',
    title: '数值统计特征',
    description: '适合 bytes、duration、latency 等数值字段。优先做分桶、标准化和简单统计，提升异常检测稳定性。',
    recommended_for: ['bytes', 'duration', 'latency', 'packet_count'],
    default_recipe_ids: ['numeric_statistics_core'],
  },
]

const BEHAVIOR_RECIPE_OPTIONS = [
  { label: '基础行为统计', value: 'base' },
  { label: '时间窗行为', value: 'window' },
  { label: '顺序特征', value: 'sequence' },
]

function isFeatureTaskCategoryId(value: string | undefined): value is FeatureTaskCategoryId {
  return FEATURE_TASK_CATEGORIES.some((category) => category.id === value)
}

export function FeatureTab(props: Props) {
  const [form] = Form.useForm<FeatureFormValues>()
  const watchedValues = Form.useWatch([], form)
  const mode = watchedValues?.mode ?? 'quick'
  const quickStrategy = watchedValues?.quickStrategy ?? (isFeatureTaskCategoryId(props.featureHandoff?.task_category) ? props.featureHandoff.task_category : 'text_complexity')
  const selectedPreprocessPipelineId = watchedValues?.preprocessPipelineId
  const selectedTemplateId = watchedValues?.templateId
  const behaviorTracking = watchedValues?.behaviorTracking
  const quickTaskConfig = watchedValues?.quickTaskConfig

  const availableColumns = useMemo(() => {
    if (!props.dataset) return []
    if (!selectedPreprocessPipelineId) {
      return props.dataset.schema_snapshot.map((field) => field.name)
    }
    return props.preprocessPipelines.find((pipeline) => pipeline.id === selectedPreprocessPipelineId)?.output_schema.map((field) => field.name)
      ?? props.dataset.schema_snapshot.map((field) => field.name)
  }, [props.dataset, props.preprocessPipelines, selectedPreprocessPipelineId])

  const selectedTemplate = props.templates.find((template) => template.id === selectedTemplateId) ?? null
  const matchedColumns = useMemo(() => {
    if (!selectedTemplate) return []
    const hintColumns = [...(selectedTemplate.field_hints.required_columns ?? []), ...(selectedTemplate.field_hints.optional_columns ?? [])]
    return hintColumns.filter((column, index) => hintColumns.indexOf(column) === index && availableColumns.includes(column))
  }, [selectedTemplate, availableColumns])
  const missingColumns = useMemo(() => {
    if (!selectedTemplate) return []
    return (selectedTemplate.field_hints.required_columns ?? []).filter((column) => !availableColumns.includes(column))
  }, [selectedTemplate, availableColumns])

  useEffect(() => {
    if (props.dataset) {
      form.setFieldsValue({
        mode: form.getFieldValue('mode') ?? 'quick',
        quickStrategy: form.getFieldValue('quickStrategy') ?? (isFeatureTaskCategoryId(props.featureHandoff?.task_category) ? props.featureHandoff.task_category : 'text_complexity'),
        name: `${props.dataset.version_name}-features-${props.pipelines.length + 1}`,
        behaviorTracking: form.getFieldValue('behaviorTracking') ?? createBehaviorTrackingDefaults(props.featureHandoff, availableColumns),
        quickTaskConfig: form.getFieldValue('quickTaskConfig') ?? createQuickTaskDefaults(props.featureHandoff, availableColumns),
        steps: form.getFieldValue('steps') ?? [],
        templateSaveLogType: inferLogType(props.dataset.parser_profile),
      })
    } else {
      form.resetFields()
    }
  }, [availableColumns, form, props.dataset, props.featureHandoff, props.pipelines.length])

  useEffect(() => {
    if (!props.dataset || !props.templates.length || form.getFieldValue('templateId')) {
      return
    }
    const recommendedTemplateId = recommendTemplateId(props.dataset.parser_profile, availableColumns, props.templates)
    if (recommendedTemplateId) {
      form.setFieldValue('templateId', recommendedTemplateId)
    }
  }, [props.dataset, props.templates, availableColumns, form])

  useEffect(() => {
    if (!props.featureHandoff) return
    form.setFieldsValue({
      mode: 'quick',
      quickStrategy: isFeatureTaskCategoryId(props.featureHandoff.task_category) ? props.featureHandoff.task_category : 'behavior_tracking',
      behaviorTracking: createBehaviorTrackingDefaults(props.featureHandoff, availableColumns),
      quickTaskConfig: createQuickTaskDefaults(props.featureHandoff, availableColumns),
    })
  }, [availableColumns, form, props.featureHandoff])

  const draftSteps = watchedValues?.steps ?? []
  const behaviorTrackingSteps = useMemo(
    () => buildBehaviorTrackingSteps(behaviorTracking, availableColumns),
    [availableColumns, behaviorTracking],
  )
  const behaviorTrackingSummary = useMemo(
    () => describeBehaviorTrackingPlan(behaviorTracking, behaviorTrackingSteps),
    [behaviorTracking, behaviorTrackingSteps],
  )
  const selectedTaskCategory = FEATURE_TASK_CATEGORIES.find((category) => category.id === quickStrategy) ?? null
  const quickTaskSteps = useMemo(
    () => buildQuickTaskSteps(quickStrategy, quickTaskConfig, availableColumns),
    [availableColumns, quickStrategy, quickTaskConfig],
  )
  const quickTaskPreview = useMemo(
    () => describeQuickTaskPlan(quickStrategy, quickTaskConfig, quickTaskSteps),
    [quickStrategy, quickTaskConfig, quickTaskSteps],
  )

  function handleSubmit(values: FeatureFormValues) {
    if (values.mode === 'quick' && values.quickStrategy === 'behavior_tracking') {
      props.onRun({
        ...values,
        steps: behaviorTrackingSteps,
      })
      return
    }
    if (values.mode === 'quick' && values.quickStrategy === 'template' && selectedTemplate) {
      props.onRun({ ...values, steps: selectedTemplate.steps.map(toDraftFromPersistedStep) })
      return
    }
    if (values.mode === 'quick') {
      props.onRun({ ...values, steps: quickTaskSteps })
      return
    }
    props.onRun(values)
  }

  function applyTemplateToAdvanced() {
    if (!selectedTemplate) return
    form.setFieldsValue({
      mode: 'advanced',
      steps: selectedTemplate.steps.map(toDraftFromPersistedStep),
      templateSaveLogType: selectedTemplate.log_type,
    })
  }

  return (
    <StageLayout
      main={
        <Space direction="vertical" size={20} className="full-width">
          <Card title="特征工程工作台" extra={props.dataset ? <Tag color="cyan">{props.dataset.version_name}</Tag> : null}>
            {props.dataset ? (
              <Form form={form} layout="vertical" onFinish={handleSubmit} initialValues={{ mode: 'quick', steps: [] }}>
                <div className="step-grid">
                  <Form.Item name="name" label="特征流水线名称" rules={[{ required: true, message: '请输入特征流水线名称' }]}>
                    <Input placeholder="例如：dataset-v2-features-3" />
                  </Form.Item>
                  <Form.Item name="preprocessPipelineId" label="输入预处理版本">
                    <Select
                      allowClear
                      placeholder="默认直接基于当前数据集"
                      options={props.preprocessPipelines.map((pipeline) => ({ label: pipeline.name, value: pipeline.id }))}
                    />
                  </Form.Item>
                </div>

                <Form.Item name="mode" label="特征模式">
                  <Segmented
                    block
                    options={[
                      { label: '快速模式', value: 'quick' },
                      { label: '高级模式', value: 'advanced' },
                    ]}
                  />
                </Form.Item>

                {mode === 'quick' ? (
                  <Space direction="vertical" size={16} className="full-width">
                    <Card size="small" className="nested-card" title="任务入口">
                      <Space direction="vertical" size={16} className="full-width">
                        <Alert
                          type="info"
                          showIcon
                          message="先选择字段问题，再让系统给出推荐配方。"
                          description="首屏按任务入口组织，不再要求你先理解算子名。点击对应入口后，系统会自动生成推荐草稿，必要时再进入高级微调。"
                        />
                        <div className="task-category-grid">
                          {FEATURE_TASK_CATEGORIES.map((category) => (
                            <Card
                              key={category.id}
                              size="small"
                              hoverable
                              className={`task-category-card ${quickStrategy === category.id ? 'is-active' : ''}`}
                              onClick={() => form.setFieldValue('quickStrategy', category.id)}
                            >
                              <Space direction="vertical" size={8} className="full-width">
                                <Text strong>{category.title}</Text>
                                <Text type="secondary">{category.description}</Text>
                                <div className="tag-wall">
                                  {category.recommended_for.map((item) => (
                                    <Tag key={`${category.id}-${item}`}>{item}</Tag>
                                  ))}
                                </div>
                              </Space>
                            </Card>
                          ))}
                          <Card
                            size="small"
                            hoverable
                            className={`task-category-card ${quickStrategy === 'template' ? 'is-active' : ''}`}
                            onClick={() => form.setFieldValue('quickStrategy', 'template')}
                          >
                            <Space direction="vertical" size={8} className="full-width">
                              <Text strong>日志模板</Text>
                              <Text type="secondary">如果你已经有一套现成模板，仍然可以直接套用。</Text>
                            </Space>
                          </Card>
                        </div>
                      </Space>
                    </Card>

                    {quickStrategy === 'behavior_tracking' ? (
                      <Card size="small" className="nested-card" title="行为追踪特征">
                        <Space direction="vertical" size={16} className="full-width">
                          {props.featureHandoff ? (
                            <Alert
                              type="info"
                              showIcon
                              message={`已从预处理页带入 ${props.featureHandoff.recommended_group_key} 的行为追踪推荐`}
                              description={`推荐按${props.featureHandoff.tracking_type === 'flow' ? '流程追踪' : '主体追踪'}建模，优先使用 ${props.featureHandoff.recommended_time_columns[0] ?? '时间字段'} 作为时间轴。`}
                              action={<Button size="small" onClick={props.onClearFeatureHandoff}>清除承接</Button>}
                            />
                          ) : null}
                          <div className="step-grid">
                            <Form.Item name={['behaviorTracking', 'trackingType']} label="追踪类型" rules={[{ required: true, message: '请选择追踪类型' }]}>
                              <Select
                                options={[
                                  { label: '按流程追踪', value: 'flow' },
                                  { label: '按主体追踪', value: 'entity' },
                                ]}
                              />
                            </Form.Item>
                            <Form.Item name={['behaviorTracking', 'groupKey']} label="追踪键" rules={[{ required: true, message: '请选择一个追踪键' }]}>
                              <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} />
                            </Form.Item>
                            <Form.Item name={['behaviorTracking', 'timeColumn']} label="时间字段" rules={[{ required: true, message: '请选择时间字段' }]}>
                              <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} />
                            </Form.Item>
                            <Form.Item name={['behaviorTracking', 'targetColumns']} label="目标字段（可选）">
                              <Select mode="multiple" allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} />
                            </Form.Item>
                            <Form.Item name={['behaviorTracking', 'recipeKinds']} label="推荐配方">
                              <Select mode="multiple" allowClear options={BEHAVIOR_RECIPE_OPTIONS} />
                            </Form.Item>
                          </div>
                          <Card size="small" type="inner" title="推荐说明">
                            <Space direction="vertical" size={8} className="full-width">
                              <Text>{behaviorTrackingSummary.description}</Text>
                              <Space wrap>
                                {behaviorTrackingSummary.highlights.map((item) => (
                                  <Tag color="cyan" key={item}>{item}</Tag>
                                ))}
                              </Space>
                              <Space wrap>
                                {behaviorTrackingSummary.generatedColumns.map((column) => (
                                  <Tag color="green" key={column}>{column}</Tag>
                                ))}
                              </Space>
                            </Space>
                          </Card>
                        </Space>
                      </Card>
                    ) : quickStrategy === 'template' ? (
                      <Card size="small" className="nested-card" title="日志类型模板">
                        <Space direction="vertical" size={16} className="full-width">
                          <Form.Item name="templateId" label="选择模板" rules={[{ required: quickStrategy === 'template', message: '请选择一个模板' }]}>
                            <Select
                              loading={props.templatesLoading}
                              placeholder="选择一个内置或项目内模板"
                              options={props.templates.map((template) => ({
                                label: `${template.scope === 'builtin' ? '内置' : '项目'} · ${template.name}`,
                                value: template.id,
                              }))}
                            />
                          </Form.Item>
                          {selectedTemplate ? (
                            <Space direction="vertical" size={12} className="full-width">
                              <Text>{selectedTemplate.description}</Text>
                              <Space wrap>
                                <Tag color="blue">{selectedTemplate.log_type}</Tag>
                                <Tag>{selectedTemplate.scope === 'builtin' ? '内置模板' : '项目模板'}</Tag>
                              </Space>
                              <Space wrap>
                                {(selectedTemplate.steps ?? []).map((step, index) => (
                                  <Tag color="cyan" key={`${selectedTemplate.id}-${index}`}>{index + 1}. {describePersistedStep(step)}</Tag>
                                ))}
                              </Space>
                              <Space wrap>
                                <Text strong>已匹配字段：</Text>
                                {matchedColumns.length ? matchedColumns.map((column) => <Tag color="green" key={column}>{column}</Tag>) : <Text type="secondary">暂无</Text>}
                              </Space>
                              <Space wrap>
                                <Text strong>缺失字段：</Text>
                                {missingColumns.length ? missingColumns.map((column) => <Tag color="red" key={column}>{column}</Tag>) : <Text type="secondary">无</Text>}
                              </Space>
                              <Button onClick={applyTemplateToAdvanced}>带入高级模式继续微调</Button>
                            </Space>
                          ) : (
                            <Text type="secondary">选择模板后，这里会显示模板说明和字段匹配结果。</Text>
                          )}
                        </Space>
                      </Card>
                    ) : (
                      <Card size="small" className="nested-card" title={selectedTaskCategory?.title ?? '推荐特征配方'}>
                        <Space direction="vertical" size={16} className="full-width">
                          {props.featureHandoff && props.featureHandoff.task_category === quickStrategy ? (
                            <Alert
                              type="info"
                              showIcon
                              message={`已从预处理页带入 ${props.featureHandoff.recommended_group_key} 的推荐方案`}
                              description="这套配置已经根据字段问题做了预设，你可以直接运行，也可以微调目标字段和时间字段。"
                              action={<Button size="small" onClick={props.onClearFeatureHandoff}>清除承接</Button>}
                            />
                          ) : null}
                          <Text>{selectedTaskCategory?.description}</Text>
                          <div className="step-grid">
                            <Form.Item
                              name={['quickTaskConfig', 'targetColumns']}
                              label="目标字段"
                              rules={[{ required: quickStrategy !== 'time_behavior', message: '请选择至少一个目标字段' }]}
                            >
                              <Select mode="multiple" allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} />
                            </Form.Item>
                            <Form.Item
                              name={['quickTaskConfig', 'timeColumn']}
                              label="时间字段"
                              rules={[{ required: quickStrategy === 'time_behavior' || quickStrategy === 'high_cardinality', message: '请选择时间字段' }]}
                            >
                              <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} />
                            </Form.Item>
                            <Form.Item name={['quickTaskConfig', 'groupColumns']} label="上下文字段 / 分组字段">
                              <Select mode="multiple" allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} />
                            </Form.Item>
                          </div>
                          <Card size="small" type="inner" title="推荐说明">
                            <Space direction="vertical" size={8} className="full-width">
                              <Text>{quickTaskPreview.description}</Text>
                              <div className="tag-wall">
                                {quickTaskPreview.highlights.map((item) => (
                                  <Tag color="cyan" key={item}>{item}</Tag>
                                ))}
                              </div>
                              <div className="tag-wall">
                                {quickTaskPreview.generatedColumns.length ? (
                                  quickTaskPreview.generatedColumns.map((column) => (
                                    <Tag color="green" key={column}>{column}</Tag>
                                  ))
                                ) : (
                                  <Text type="secondary">先选择字段后，这里会展示建议新增的特征列。</Text>
                                )}
                              </div>
                            </Space>
                          </Card>
                          <Button onClick={() => form.setFieldsValue({ mode: 'advanced', steps: quickTaskSteps })}>
                            带入高级微调
                          </Button>
                        </Space>
                      </Card>
                    )}
                  </Space>
                ) : (
                  <>
                    <Form.List name="steps">
                      {(fields, { add, remove, move }) => (
                        <Space direction="vertical" size={16} className="full-width">
                          <Space wrap>
                            <Button icon={<PlusOutlined />} onClick={() => add(createFeatureStepDraft())}>
                              新增步骤
                            </Button>
                            <Text type="secondary">特征工程默认保留原字段，新增特征统一追加，便于训练与异常分析同时使用。</Text>
                          </Space>

                          {fields.length ? (
                            fields.map((field, index) => {
                              const currentStep = draftSteps[field.name] ?? createFeatureStepDraft()
                              const stepType = currentStep.step_type
                              const selectorMode = currentStep.input_selector?.mode ?? 'explicit'
                              const selectedColumns = currentStep.input_selector?.columns ?? []
                              const supportsOutputMode = supportsOutputModeConfig(stepType)
                              const outputModeOptions = getOutputModeOptions(stepType)
                              const operator = currentStep.params.operator ?? 'contains'

                              return (
                                <Card
                                  key={field.key}
                                  size="small"
                                  className="preprocess-step-card"
                                  title={
                                    <Space wrap>
                                      <Tag color="cyan">{index + 1}</Tag>
                                      <Text strong>{getFeatureStepLabel(stepType)}</Text>
                                    </Space>
                                  }
                                  extra={
                                    <Space>
                                      <Button size="small" icon={<ArrowUpOutlined />} disabled={index === 0} onClick={() => move(index, index - 1)} />
                                      <Button size="small" icon={<ArrowDownOutlined />} disabled={index === fields.length - 1} onClick={() => move(index, index + 1)} />
                                      <Button size="small" onClick={() => props.onPreviewStep(index, form.getFieldsValue(true))}>预览此步</Button>
                                      <Button size="small" danger icon={<MinusCircleOutlined />} onClick={() => remove(field.name)} />
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
                                    <Form.Item name={[field.name, 'input_selector', 'mode']} label="字段选择方式">
                                      <Select options={SELECTOR_MODE_OPTIONS} />
                                    </Form.Item>
                                    {selectorMode === 'explicit' ? (
                                      <Form.Item name={[field.name, 'input_selector', 'columns']} label={getSelectorLabel(stepType)}>
                                        <Select mode="multiple" allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="选择一个或多个字段" />
                                      </Form.Item>
                                    ) : null}
                                    {selectorMode === 'dtype' ? (
                                      <Form.Item name={[field.name, 'input_selector', 'dtype']} label="字段类型">
                                        <Select options={FEATURE_DTYPE_OPTIONS} />
                                      </Form.Item>
                                    ) : null}
                                    {selectorMode === 'role_tag' ? (
                                      <Form.Item name={[field.name, 'input_selector', 'role_tag']} label="角色标签">
                                        <Select options={ROLE_TAG_OPTIONS} />
                                      </Form.Item>
                                    ) : null}
                                    {selectorMode === 'name_pattern' ? (
                                      <Form.Item name={[field.name, 'input_selector', 'name_pattern']} label="名称匹配正则">
                                        <Input placeholder="例如 message|payload|query" />
                                      </Form.Item>
                                    ) : null}
                                    {stepType === 'derive_time_parts' ? <Form.Item name={[field.name, 'params', 'prefix']} label="特征前缀"><Input placeholder="留空则使用原字段名" /></Form.Item> : null}
                                    {stepType === 'regex_match_count' ? <Form.Item name={[field.name, 'params', 'regexPattern']} label="正则表达式"><Input placeholder="例如 (error|failed|timeout)" /></Form.Item> : null}
                                    {stepType === 'pattern_flags' ? <Form.Item name={[field.name, 'params', 'patternFlags']} label="检测模式"><Select mode="multiple" allowClear options={PATTERN_FLAG_OPTIONS} placeholder="默认启用全部内置模式" /></Form.Item> : null}
                                    {stepType === 'keyword_count' ? <Form.Item name={[field.name, 'params', 'keywordsText']} label="关键词列表"><Input placeholder="逗号分隔，例如 error,exception,timeout" /></Form.Item> : null}
                                    {stepType === 'numeric_bucket' ? <Form.Item name={[field.name, 'params', 'bins']} label="分桶数量"><Input type="number" placeholder="默认 5" /></Form.Item> : null}
                                    {stepType === 'numeric_scale' ? <Form.Item name={[field.name, 'params', 'method']} label="标准化方式"><Select options={[{ label: 'Z-Score', value: 'zscore' }, { label: 'Min-Max', value: 'minmax' }]} /></Form.Item> : null}
                                    {stepType === 'concat_fields' ? <Form.Item name={[field.name, 'params', 'separator']} label="拼接分隔符"><Input placeholder="默认 |" /></Form.Item> : null}
                                    {stepType === 'group_unique_count' ? (
                                      <Form.Item name={[field.name, 'params', 'targetColumn']} label="去重目标字段">
                                        <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="选择组内统计 unique 的字段" />
                                      </Form.Item>
                                    ) : null}
                                    {stepType === 'group_duration' ? (
                                      <Form.Item name={[field.name, 'params', 'timeColumn']} label="时间字段">
                                        <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="选择时间字段" />
                                      </Form.Item>
                                    ) : null}
                                    {stepType === 'group_event_order' || stepType === 'time_since_previous_event' || stepType === 'time_until_next_event' ? (
                                      <Form.Item name={[field.name, 'params', 'timeColumn']} label="时间字段">
                                        <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="选择时间字段" />
                                      </Form.Item>
                                    ) : null}
                                    {stepType === 'group_value_change_flag' ? (
                                      <>
                                        <Form.Item name={[field.name, 'params', 'timeColumn']} label="时间字段">
                                          <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="选择时间字段" />
                                        </Form.Item>
                                        <Form.Item name={[field.name, 'params', 'targetColumn']} label="变化检测字段">
                                          <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="例如 status_code / path / process_name" />
                                        </Form.Item>
                                      </>
                                    ) : null}
                                    {stepType === 'time_window_count' ? (
                                      <>
                                        <Form.Item name={[field.name, 'params', 'timeColumn']} label="时间字段">
                                          <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="选择时间字段" />
                                        </Form.Item>
                                        <Form.Item name={[field.name, 'params', 'windowMinutes']} label="窗口分钟数">
                                          <Input type="number" placeholder="默认 15" />
                                        </Form.Item>
                                      </>
                                    ) : null}
                                    {stepType === 'window_unique_count' ? (
                                      <>
                                        <Form.Item name={[field.name, 'params', 'timeColumn']} label="时间字段">
                                          <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="选择时间字段" />
                                        </Form.Item>
                                        <Form.Item name={[field.name, 'params', 'targetColumn']} label="窗口内去重目标字段">
                                          <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="选择窗口内统计 unique 的字段" />
                                        </Form.Item>
                                        <Form.Item name={[field.name, 'params', 'windowMinutes']} label="窗口分钟数">
                                          <Input type="number" placeholder="默认 15" />
                                        </Form.Item>
                                      </>
                                    ) : null}
                                    {stepType === 'window_target_unique_count' ? (
                                      <>
                                        <Form.Item name={[field.name, 'params', 'timeColumn']} label="时间字段">
                                          <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="选择时间字段" />
                                        </Form.Item>
                                        <Form.Item name={[field.name, 'params', 'targetColumn']} label="窗口内去重目标字段">
                                          <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="例如 path / dest_ip / process_name" />
                                        </Form.Item>
                                        <Form.Item name={[field.name, 'params', 'windowMinutes']} label="窗口分钟数">
                                          <Input type="number" placeholder="默认 15" />
                                        </Form.Item>
                                      </>
                                    ) : null}
                                    {stepType === 'window_status_change_count' ? (
                                      <>
                                        <Form.Item name={[field.name, 'params', 'timeColumn']} label="时间字段">
                                          <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="选择时间字段" />
                                        </Form.Item>
                                        <Form.Item name={[field.name, 'params', 'targetColumn']} label="状态字段">
                                          <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="例如 status_code / result / process_name" />
                                        </Form.Item>
                                        <Form.Item name={[field.name, 'params', 'windowMinutes']} label="窗口分钟数">
                                          <Input type="number" placeholder="默认 15" />
                                        </Form.Item>
                                      </>
                                    ) : null}
                                    {stepType === 'window_spike_flag' ? (
                                      <>
                                        <Form.Item name={[field.name, 'params', 'timeColumn']} label="时间字段">
                                          <Select allowClear options={availableColumns.map((column) => ({ label: column, value: column }))} placeholder="选择时间字段" />
                                        </Form.Item>
                                        <Form.Item name={[field.name, 'params', 'windowMinutes']} label="窗口分钟数">
                                          <Input type="number" placeholder="默认 15" />
                                        </Form.Item>
                                        <Form.Item name={[field.name, 'params', 'threshold']} label="突增阈值">
                                          <Input type="number" placeholder="默认 10" />
                                        </Form.Item>
                                      </>
                                    ) : null}
                                    {stepType === 'value_map' ? (
                                      <>
                                        <Form.Item name={[field.name, 'params', 'mappingText']} label="映射 JSON"><TextArea rows={4} placeholder='例如：{"tcp":1,"udp":2,"icmp":3}' /></Form.Item>
                                        <Form.Item name={[field.name, 'params', 'defaultValue']} label="默认值"><Input placeholder="未命中映射时使用" /></Form.Item>
                                      </>
                                    ) : null}
                                    {stepType === 'boolean_flag' ? (
                                      <>
                                        <Form.Item name={[field.name, 'params', 'operator']} label="判断条件">
                                          <Select options={[{ label: '包含', value: 'contains' }, { label: '等于', value: 'eq' }, { label: '不等于', value: 'ne' }, { label: '大于', value: 'gt' }, { label: '小于', value: 'lt' }, { label: '为空', value: 'is_null' }, { label: '非空', value: 'not_null' }]} />
                                        </Form.Item>
                                        {operator !== 'is_null' && operator !== 'not_null' ? <Form.Item name={[field.name, 'params', 'value']} label="比较值"><Input placeholder="例如：admin / 500 / anomaly" /></Form.Item> : null}
                                      </>
                                    ) : null}
                                    {supportsOutputMode ? (
                                      <>
                                        <Form.Item name={[field.name, 'output_mode', 'mode']} label="输出方式">
                                          <Select options={outputModeOptions} />
                                        </Form.Item>
                                        {currentStep.output_mode?.mode === 'append_new_columns' && supportsExplicitOutputColumn(stepType, selectorMode, selectedColumns) ? <Form.Item name={[field.name, 'output_mode', 'output_column']} label="输出字段名"><Input placeholder="留空则自动追加后缀" /></Form.Item> : null}
                                        {currentStep.output_mode?.mode === 'append_new_columns' ? <Form.Item name={[field.name, 'output_mode', 'suffix']} label="输出后缀"><Input placeholder="_freq / _flag / _bucket" /></Form.Item> : null}
                                      </>
                                    ) : null}
                                  </div>
                                  <Card size="small" className="nested-card" title="步骤说明">
                                    <Space wrap>
                                      <Tag>{describeDraftStep(currentStep)}</Tag>
                                    </Space>
                                  </Card>
                                </Card>
                              )
                            })
                          ) : (
                            <Empty description="还没有特征步骤。你可以先新增一个“频次编码”或“关键词计数”步骤。" />
                          )}
                        </Space>
                      )}
                    </Form.List>

                    <Card size="small" className="nested-card" title="另存为项目模板">
                      <div className="step-grid">
                        <Form.Item name="templateSaveName" label="模板名称">
                          <Input placeholder="例如：NTA v1 模板" />
                        </Form.Item>
                        <Form.Item name="templateSaveLogType" label="模板类型">
                          <Select options={LOG_TYPE_OPTIONS} />
                        </Form.Item>
                      </div>
                      <Form.Item name="templateSaveDescription" label="模板说明">
                        <TextArea rows={3} placeholder="说明这个模板适合的日志类型和用途" />
                      </Form.Item>
                      <Button icon={<SaveOutlined />} loading={props.savingTemplate} onClick={() => props.onSaveTemplate(form.getFieldsValue(true))}>
                        保存为项目模板
                      </Button>
                    </Card>
                  </>
                )}

                <Space className="top-gap">
                  <Button type="primary" htmlType="submit" icon={<ControlOutlined />} loading={props.running}>
                    运行特征工程
                  </Button>
                  <Text type="secondary">当前输入字段数：{availableColumns.length}</Text>
                </Space>
              </Form>
            ) : (
              <Empty description="先在数据页准备一个数据集，必要时执行预处理，再继续做特征工程。" />
            )}
          </Card>

          <Card title="特征版本">
            <List
              loading={props.listLoading}
              locale={{ emptyText: '当前数据集还没有特征版本。' }}
              dataSource={props.pipelines}
              renderItem={(pipeline) => (
                <List.Item
                  className={pipeline.id === props.selectedPipelineId ? 'selectable-row is-selected' : 'selectable-row'}
                  onClick={() => props.onSelectPipeline(pipeline.id)}
                >
                  <List.Item.Meta
                    title={<Space><Text strong>{pipeline.name}</Text><Tag color={pipeline.status === 'completed' ? 'green' : 'processing'}>{pipeline.status}</Tag></Space>}
                    description={`输出 ${pipeline.output_row_count} 行，步骤数 ${pipeline.steps.length}`}
                  />
                </List.Item>
              )}
            />
          </Card>
        </Space>
      }
      detail={
        <Space direction="vertical" size={20} className="full-width">
          <DetailPanel title="特征详情" extra={props.selectedPipeline ? <Tag color="cyan">{props.selectedPipeline.name}</Tag> : null}>
            <Space direction="vertical" size={16} className="full-width">
              {mode === 'quick' ? (
                <Card size="small" className="nested-card" title={quickStrategy === 'behavior_tracking' ? '行为追踪摘要' : '模板摘要'}>
                  {quickStrategy === 'behavior_tracking' ? (
                    <Space direction="vertical" size={12} className="full-width">
                      <Descriptions
                        column={1}
                        items={[
                          { key: 'tracking', label: '追踪类型', children: behaviorTracking?.trackingType === 'flow' ? '按流程追踪' : '按主体追踪' },
                          { key: 'group', label: '追踪键', children: behaviorTracking?.groupKey ?? '未选择' },
                          { key: 'time', label: '时间字段', children: behaviorTracking?.timeColumn ?? '未选择' },
                          { key: 'targets', label: '目标字段', children: behaviorTracking?.targetColumns?.join(', ') || '未选择' },
                        ]}
                      />
                      <Text>{behaviorTrackingSummary.description}</Text>
                      <Space wrap>
                        {behaviorTrackingSummary.highlights.map((item) => (
                          <Tag color="cyan" key={item}>{item}</Tag>
                        ))}
                      </Space>
                      <Space wrap>
                        {behaviorTrackingSummary.generatedColumns.map((column) => (
                          <Tag color="green" key={column}>{column}</Tag>
                        ))}
                      </Space>
                    </Space>
                  ) : selectedTemplate ? (
                    <Descriptions
                      column={1}
                      items={[
                        { key: 'template', label: '模板名称', children: selectedTemplate.name },
                        { key: 'scope', label: '模板范围', children: selectedTemplate.scope === 'builtin' ? '内置模板' : '项目模板' },
                        { key: 'matched', label: '已匹配字段', children: matchedColumns.length ? matchedColumns.join(', ') : '暂无' },
                        { key: 'missing', label: '缺失字段', children: missingColumns.length ? missingColumns.join(', ') : '无' },
                      ]}
                    />
                  ) : (
                    <Text type="secondary">选择一个模板后，这里会显示模板摘要。</Text>
                  )}
                </Card>
              ) : (
                <>
                  <Card size="small" className="nested-card" title="当前步骤摘要">
                    {draftSteps.length ? <Space wrap>{draftSteps.map((step, index) => <Tag color="cyan" key={step.step_id ?? `${step.step_type}-${index}`}>{index + 1}. {describeDraftStep(step)}</Tag>)}</Space> : <Text type="secondary">当前还没有配置特征步骤。</Text>}
                  </Card>
                  <Card size="small" className="nested-card" title="步骤级预览" loading={props.stepPreviewLoading}>
                    {props.stepPreview ? (
                      <Space direction="vertical" size={16} className="full-width">
                        <Descriptions
                          column={1}
                          items={[
                            { key: 'step', label: '当前步骤', children: `${props.stepPreview.preview_step_index + 1}. ${describePersistedStep(props.stepPreview.step)}` },
                            { key: 'rows', label: '行数变化', children: `${props.stepPreview.before_row_count} -> ${props.stepPreview.after_row_count}` },
                            { key: 'added', label: '新增字段', children: props.stepPreview.added_columns.length ? props.stepPreview.added_columns.map((column) => <Tag color="green" key={column}>{column}</Tag>) : '无' },
                          ]}
                        />
                        <Card size="small" title="执行前样本">
                          <Table<Record<string, unknown>> rowKey={(_, index) => `feature-before-${index}`} columns={buildPreviewColumns(props.stepPreview.before_columns)} dataSource={props.stepPreview.before_rows} pagination={{ pageSize: 3, hideOnSinglePage: true }} scroll={{ x: 900 }} size="small" />
                        </Card>
                        <Card size="small" title="执行后样本">
                          <Table<Record<string, unknown>> rowKey={(_, index) => `feature-after-${index}`} columns={buildPreviewColumns(props.stepPreview.after_columns)} dataSource={props.stepPreview.after_rows} pagination={{ pageSize: 3, hideOnSinglePage: true }} scroll={{ x: 900 }} size="small" />
                        </Card>
                      </Space>
                    ) : (
                      <Text type="secondary">点击某一步上的“预览此步”，这里会显示单步特征生成前后的变化。</Text>
                    )}
                  </Card>
                </>
              )}
            </Space>
          </DetailPanel>

          <DetailPanel title="特征输出预览" extra={props.selectedPipeline ? <Tag color="purple">{props.selectedPipeline.name}</Tag> : null}>
            {props.selectedPipeline ? (
              <Space direction="vertical" size={16} className="full-width">
                <Descriptions column={1} items={[{ key: 'rows', label: '输出行数', children: props.selectedPipeline.output_row_count }, { key: 'steps', label: '步骤数', children: props.selectedPipeline.steps.length }, { key: 'input', label: '输入预处理版本', children: props.selectedPipeline.preprocess_pipeline_id ?? '直接基于数据集' }, { key: 'columns', label: '输出列数', children: props.selectedPipeline.output_schema.length }]} />
                {props.selectedPipeline.status !== 'completed' ? (
                  <Alert
                    type={props.selectedPipeline.status === 'failed' ? 'error' : 'info'}
                    showIcon
                    message={
                      props.selectedPipeline.status === 'failed'
                        ? '该特征任务执行失败，请查看任务状态和步骤参数后重试。'
                        : '该特征任务正在后台执行，完成后会自动刷新输出结果。'
                    }
                  />
                ) : (
                  <Table<Record<string, unknown>> rowKey={(_, index) => String(index)} loading={props.previewLoading} columns={buildPreviewColumns(props.preview?.columns ?? [])} dataSource={props.preview?.rows ?? []} pagination={{ pageSize: 5, hideOnSinglePage: true }} scroll={{ x: 900 }} size="small" />
                )}
              </Space>
            ) : (
              <Empty description="运行一个特征流水线后，这里会显示输出预览。" />
            )}
          </DetailPanel>
        </Space>
      }
    />
  )
}

function createBehaviorTrackingDefaults(featureHandoff: FeatureHandoff | null, availableColumns: string[]) {
  const recommendedTime = featureHandoff?.recommended_time_columns.find((column) => availableColumns.includes(column))
  const recommendedTargets = (featureHandoff?.recommended_target_columns ?? []).filter((column) => availableColumns.includes(column)).slice(0, 2)
  const recipeKinds: Array<'base' | 'window' | 'sequence'> = featureHandoff?.tracking_type === 'entity' ? ['base', 'window'] : ['base', 'sequence']
  return {
    trackingType: (featureHandoff?.tracking_type === 'entity' ? 'entity' : 'flow') as 'flow' | 'entity',
    groupKey: featureHandoff?.recommended_group_key && availableColumns.includes(featureHandoff.recommended_group_key)
      ? featureHandoff.recommended_group_key
      : undefined,
    timeColumn: recommendedTime,
    targetColumns: recommendedTargets,
    recipeKinds,
  }
}

function createQuickTaskDefaults(featureHandoff: FeatureHandoff | null, availableColumns: string[]) {
  return {
    targetColumns: featureHandoff?.recommended_group_key && availableColumns.includes(featureHandoff.recommended_group_key)
      ? [featureHandoff.recommended_group_key]
      : [],
    timeColumn: featureHandoff?.recommended_time_columns.find((column) => availableColumns.includes(column)),
    groupColumns: (featureHandoff?.recommended_target_columns ?? []).filter((column) => availableColumns.includes(column)).slice(0, 2),
  }
}

function buildQuickTaskSteps(
  quickStrategy: FeatureFormValues['quickStrategy'] | undefined,
  quickTaskConfig: FeatureFormValues['quickTaskConfig'] | undefined,
  availableColumns: string[],
) {
  const targetColumns = (quickTaskConfig?.targetColumns ?? []).filter((column) => availableColumns.includes(column))
  const groupColumns = (quickTaskConfig?.groupColumns ?? []).filter((column) => availableColumns.includes(column))
  const timeColumn = quickTaskConfig?.timeColumn && availableColumns.includes(quickTaskConfig.timeColumn)
    ? quickTaskConfig.timeColumn
    : undefined

  if (!targetColumns.length) {
    return []
  }

  if (quickStrategy === 'text_complexity') {
    return [
      createFeatureTaskStep('text_length', targetColumns),
      createFeatureTaskStep('byte_length', targetColumns),
      createFeatureTaskStep('token_count', targetColumns),
      createFeatureTaskStep('shannon_entropy', targetColumns),
      createFeatureTaskStep('keyword_count', targetColumns, { keywordsText: 'error,failed,timeout,exception,denied' }),
      createFeatureTaskStep('pattern_flags', targetColumns),
    ]
  }

  if (quickStrategy === 'high_cardinality') {
    const steps = [
      createFeatureTaskStep('frequency_encode', targetColumns),
    ]
    if (timeColumn) {
      steps.push(
        createFeatureTaskStep('time_window_count', [targetColumns[0]], { timeColumn, windowMinutes: 15 }),
        createFeatureTaskStep('window_spike_flag', [targetColumns[0]], { timeColumn, windowMinutes: 15, threshold: 10 }),
      )
      if (groupColumns[0]) {
        steps.push(
          createFeatureTaskStep('window_target_unique_count', [targetColumns[0]], {
            timeColumn,
            targetColumn: groupColumns[0],
            windowMinutes: 15,
          }),
        )
      }
    }
    return steps
  }

  if (quickStrategy === 'time_behavior') {
    if (!timeColumn && !targetColumns[0]) {
      return []
    }
    const resolvedTimeColumn = timeColumn ?? targetColumns[0]
    const steps = [createFeatureTaskStep('derive_time_parts', [resolvedTimeColumn])]
    if (groupColumns[0]) {
      steps.push(
        createFeatureTaskStep('time_window_count', [groupColumns[0]], { timeColumn: resolvedTimeColumn, windowMinutes: 15 }),
        createFeatureTaskStep('window_spike_flag', [groupColumns[0]], { timeColumn: resolvedTimeColumn, windowMinutes: 15, threshold: 10 }),
      )
    }
    return steps
  }

  if (quickStrategy === 'numeric_statistics') {
    return [
      createFeatureTaskStep('numeric_bucket', targetColumns, { bins: 5 }),
      createFeatureTaskStep('numeric_scale', targetColumns, { method: 'zscore' }),
    ]
  }

  return []
}

function createFeatureTaskStep(
  stepType: DraftFeatureStepType,
  columns: string[],
  params: Partial<FeatureStepDraft['params']> = {},
): FeatureStepDraft {
  return {
    ...createFeatureStepDraft(stepType),
    input_selector: {
      ...createFeatureStepDraft(stepType).input_selector,
      mode: 'explicit',
      columns,
    },
    params: {
      ...createFeatureStepDraft(stepType).params,
      ...params,
    },
    output_mode: {
      ...createFeatureStepDraft(stepType).output_mode,
      mode: (getOutputModeOptions(stepType)[0]?.value ?? 'append_new_columns') as 'append_new_columns' | 'replace_existing',
    },
  }
}

function describeQuickTaskPlan(
  quickStrategy: FeatureFormValues['quickStrategy'] | undefined,
  quickTaskConfig: FeatureFormValues['quickTaskConfig'] | undefined,
  steps: FeatureStepDraft[],
) {
  const targetColumns = quickTaskConfig?.targetColumns ?? []
  const timeColumn = quickTaskConfig?.timeColumn
  const groupColumns = quickTaskConfig?.groupColumns ?? []

  if (quickStrategy === 'text_complexity') {
    return {
      description: '系统会把原始文本字段转成长度、熵、关键词命中和模式标记等更适合异常检测的数值特征。',
      highlights: [`目标字段: ${targetColumns.join(', ') || '待选择'}`],
      generatedColumns: steps.map(describeGeneratedColumn),
    }
  }
  if (quickStrategy === 'high_cardinality') {
    return {
      description: '系统会优先生成频次、窗口活跃度和目标分散度等统计特征，避免直接编码高基数原值。',
      highlights: [
        `目标字段: ${targetColumns.join(', ') || '待选择'}`,
        timeColumn ? `时间字段: ${timeColumn}` : '可选时间字段可进一步生成窗口行为特征',
      ],
      generatedColumns: steps.map(describeGeneratedColumn),
    }
  }
  if (quickStrategy === 'time_behavior') {
    return {
      description: '系统会先生成时间拆分特征，再按需要补时间窗活跃度和突增信号。',
      highlights: [
        `时间字段: ${timeColumn || targetColumns[0] || '待选择'}`,
        groupColumns[0] ? `分组字段: ${groupColumns.join(', ')}` : '可选分组字段可进一步生成窗口行为',
      ],
      generatedColumns: steps.map(describeGeneratedColumn),
    }
  }
  if (quickStrategy === 'numeric_statistics') {
    return {
      description: '系统会把数值字段做分桶和标准化，让它们更容易被异常检测模型利用。',
      highlights: [`目标字段: ${targetColumns.join(', ') || '待选择'}`],
      generatedColumns: steps.map(describeGeneratedColumn),
    }
  }
  return {
    description: '选择任务入口后，这里会展示推荐生成的特征和业务解释。',
    highlights: [],
    generatedColumns: steps.map(describeGeneratedColumn),
  }
}

function describeGeneratedColumn(step: FeatureStepDraft) {
  return step.output_mode.output_column || `${(step.input_selector.columns ?? []).join('_')}${step.output_mode.suffix ?? ''}`
}

function buildBehaviorTrackingSteps(
  behaviorTracking: FeatureFormValues['behaviorTracking'] | undefined,
  availableColumns: string[],
) {
  if (!behaviorTracking?.groupKey || !behaviorTracking.timeColumn) {
    return []
  }
  const groupKey = behaviorTracking.groupKey
  const timeColumn = behaviorTracking.timeColumn
  if (!availableColumns.includes(groupKey) || !availableColumns.includes(timeColumn)) {
    return []
  }

  const targetColumns = (behaviorTracking.targetColumns ?? []).filter((column) => availableColumns.includes(column) && column !== groupKey && column !== timeColumn)
  const primaryTarget = targetColumns[0]
  const steps: FeatureStepDraft[] = []
  const recipeKinds = new Set(behaviorTracking.recipeKinds ?? [])

  if (recipeKinds.has('base')) {
    steps.push(
      createBehaviorTrackingStep('group_frequency', [groupKey], { output_column: `${groupKey}_event_count` }),
      createBehaviorTrackingStep('group_duration', [groupKey], { timeColumn, output_column: `${groupKey}_duration_seconds` }),
    )
    if (primaryTarget) {
      steps.push(
        createBehaviorTrackingStep('group_unique_count', [groupKey], {
          timeColumn,
          targetColumn: primaryTarget,
          output_column: `${groupKey}_${primaryTarget}_unique_count`,
        }),
      )
    }
  }

  if (recipeKinds.has('window')) {
    steps.push(
      createBehaviorTrackingStep('time_window_count', [groupKey], {
        timeColumn,
        windowMinutes: 15,
        output_column: `${groupKey}_15m_count`,
      }),
      createBehaviorTrackingStep('window_spike_flag', [groupKey], {
        timeColumn,
        windowMinutes: 15,
        threshold: behaviorTracking.trackingType === 'entity' ? 10 : 6,
        output_column: `${groupKey}_15m_spike`,
      }),
    )
    if (primaryTarget) {
      steps.push(
        createBehaviorTrackingStep('window_target_unique_count', [groupKey], {
          timeColumn,
          targetColumn: primaryTarget,
          windowMinutes: 15,
          output_column: `${groupKey}_${primaryTarget}_15m_unique_count`,
        }),
      )
    }
  }

  if (recipeKinds.has('sequence')) {
    steps.push(
      createBehaviorTrackingStep('group_event_order', [groupKey], { timeColumn, output_column: `${groupKey}_event_order` }),
      createBehaviorTrackingStep('time_since_previous_event', [groupKey], { timeColumn, output_column: `${groupKey}_seconds_since_previous` }),
      createBehaviorTrackingStep('time_until_next_event', [groupKey], { timeColumn, output_column: `${groupKey}_seconds_until_next` }),
    )
    if (primaryTarget) {
      steps.push(
        createBehaviorTrackingStep('group_value_change_flag', [groupKey], {
          timeColumn,
          targetColumn: primaryTarget,
          output_column: `${groupKey}_${primaryTarget}_changed`,
        }),
      )
    }
  }

  if (behaviorTracking.trackingType === 'flow' && primaryTarget && (recipeKinds.has('sequence') || recipeKinds.has('window'))) {
    steps.push(
      createBehaviorTrackingStep('window_status_change_count', [groupKey], {
        timeColumn,
        targetColumn: primaryTarget,
        windowMinutes: 15,
        output_column: `${groupKey}_${primaryTarget}_15m_change_count`,
      }),
    )
  }

  return dedupeBehaviorTrackingSteps(steps)
}

function createBehaviorTrackingStep(
  stepType: DraftFeatureStepType,
  columns: string[],
  options: {
    timeColumn?: string
    targetColumn?: string
    windowMinutes?: number
    threshold?: number
    output_column: string
  },
): FeatureStepDraft {
  return {
    ...createFeatureStepDraft(stepType),
    input_selector: { mode: 'explicit', columns },
    params: {
      ...createFeatureStepDraft(stepType).params,
      timeColumn: options.timeColumn,
      targetColumn: options.targetColumn,
      windowMinutes: options.windowMinutes,
      threshold: options.threshold,
    },
    output_mode: {
      mode: 'append_new_columns',
      output_column: options.output_column,
      suffix: inferDefaultSuffix(stepType),
    },
  }
}

function dedupeBehaviorTrackingSteps(steps: FeatureStepDraft[]) {
  const seen = new Set<string>()
  return steps.filter((step) => {
    const fingerprint = JSON.stringify({
      stepType: step.step_type,
      columns: step.input_selector.columns,
      target: step.params.targetColumn,
      time: step.params.timeColumn,
      output: step.output_mode.output_column,
    })
    if (seen.has(fingerprint)) return false
    seen.add(fingerprint)
    return true
  })
}

function describeBehaviorTrackingPlan(
  behaviorTracking: FeatureFormValues['behaviorTracking'] | undefined,
  steps: FeatureStepDraft[],
) {
  const highlights = [
    behaviorTracking?.trackingType === 'entity' ? '按主体追踪行为' : '按流程追踪行为',
    behaviorTracking?.groupKey ? `追踪键: ${behaviorTracking.groupKey}` : '待选择追踪键',
    behaviorTracking?.timeColumn ? `时间轴: ${behaviorTracking.timeColumn}` : '待选择时间字段',
  ]
  return {
    description: behaviorTracking?.trackingType === 'entity'
      ? '主体追踪会把用户、主机、设备或来源 IP 作为行为主体，生成活跃度、窗口行为和突增相关特征。'
      : '流程追踪会把 request/session/trace 等流程 ID 作为分组键，生成事件数、持续时长、前后时序和状态变化特征。',
    highlights,
    generatedColumns: steps.map((step) => step.output_mode.output_column || `${(step.input_selector.columns ?? []).join('_')}${step.output_mode.suffix ?? ''}`),
  }
}

function createFeatureStepDraft(stepType: DraftFeatureStepType = 'frequency_encode'): FeatureStepDraft {
  return {
    step_id: `feature_step_${Math.random().toString(36).slice(2, 10)}`,
    step_type: stepType,
    enabled: true,
    input_selector: {
      mode: 'explicit',
      columns: [],
      dtype: 'string',
      role_tag: 'text',
    },
    params: {
      method: 'zscore',
      operator: 'contains',
      patternFlags: ['ip', 'url', 'hash'],
      windowMinutes: 15,
      threshold: 10,
    },
    output_mode: {
      mode: 'append_new_columns',
      suffix: inferDefaultSuffix(stepType),
    },
  }
}

function toDraftFromPersistedStep(step: FeatureStep): FeatureStepDraft {
  const stepType = (step.step_type ?? step.type ?? 'frequency_encode') as DraftFeatureStepType
  const keywords = Array.isArray(step.params.keywords) ? step.params.keywords.map(String).join(',') : ''
  const mappingEntries = step.params.mapping && typeof step.params.mapping === 'object'
    ? JSON.stringify(step.params.mapping, null, 2)
    : ''
  const selectorMode = step.input_selector?.mode ?? 'explicit'

  return {
    step_id: step.step_id ?? `feature_step_${Math.random().toString(36).slice(2, 10)}`,
    step_type: stepType,
    enabled: step.enabled ?? true,
    input_selector: {
      mode: selectorMode,
      columns: step.input_selector?.columns ?? ((step.params.columns as string[] | undefined) ?? (step.params.column ? [String(step.params.column)] : [])),
      dtype: step.input_selector?.dtype as FeatureStepDraft['input_selector']['dtype'],
      role_tag: step.input_selector?.role_tag as FeatureStepDraft['input_selector']['role_tag'],
      name_pattern: step.input_selector?.name_pattern as string | undefined,
    },
    params: {
      prefix: step.params.prefix as string | undefined,
      keywordsText: keywords,
      regexPattern: step.params.pattern as string | undefined,
      patternFlags: Array.isArray(step.params.patterns) ? step.params.patterns as FeatureStepDraft['params']['patternFlags'] : ['ip', 'url', 'hash'],
      bins: Number(step.params.bins ?? 5),
      method: (step.params.method as 'zscore' | 'minmax' | undefined) ?? 'zscore',
      separator: step.params.separator ? String(step.params.separator) : undefined,
      targetColumn: step.params.target_column ? String(step.params.target_column) : undefined,
      timeColumn: step.params.time_column ? String(step.params.time_column) : undefined,
      windowMinutes: Number(step.params.window_minutes ?? 15),
      threshold: Number(step.params.threshold ?? 10),
      mappingText: mappingEntries,
      defaultValue: step.params.default_value ? String(step.params.default_value) : undefined,
      operator: (step.params.operator as FeatureStepDraft['params']['operator']) ?? 'contains',
      value: step.params.value ? String(step.params.value) : undefined,
    },
    output_mode: {
      mode: (step.output_mode?.mode as 'append_new_columns' | 'replace_existing' | undefined) ?? 'append_new_columns',
      output_column: step.output_mode?.output_column,
      suffix: step.output_mode?.suffix ?? inferDefaultSuffix(stepType),
    },
  }
}

function inferDefaultSuffix(stepType: DraftFeatureStepType) {
  if (stepType === 'text_length') return '_length'
  if (stepType === 'byte_length') return '_bytes'
  if (stepType === 'token_count') return '_tokens'
  if (stepType === 'shannon_entropy') return '_entropy'
  if (stepType === 'keyword_count') return '_keyword_hits'
  if (stepType === 'frequency_encode') return '_freq'
  if (stepType === 'category_encode') return '_code'
  if (stepType === 'numeric_bucket') return '_bucket'
  if (stepType === 'numeric_scale') return '_scaled'
  if (stepType === 'ratio_feature') return '_ratio'
  if (stepType === 'difference_feature') return '_diff'
  if (stepType === 'concat_fields') return '_concat'
  if (stepType === 'equality_flag') return '_equal'
  if (stepType === 'group_frequency') return '_group_count'
  if (stepType === 'group_unique_count') return '_unique_count'
  if (stepType === 'group_duration') return '_duration_seconds'
  if (stepType === 'group_event_order') return '_event_order'
  if (stepType === 'time_since_previous_event') return '_seconds_since_previous'
  if (stepType === 'time_until_next_event') return '_seconds_until_next'
  if (stepType === 'group_value_change_flag') return '_value_changed'
  if (stepType === 'time_window_count') return '_15m_count'
  if (stepType === 'window_unique_count') return '_15m_unique_count'
  if (stepType === 'window_target_unique_count') return '_15m_target_unique_count'
  if (stepType === 'window_status_change_count') return '_15m_status_change_count'
  if (stepType === 'window_spike_flag') return '_15m_spike'
  if (stepType === 'status_category') return '_category'
  if (stepType === 'value_map') return '_mapped'
  if (stepType === 'boolean_flag') return '_flag'
  return '_feature'
}

function supportsOutputModeConfig(stepType: DraftFeatureStepType) {
  return !['select_features', 'ip_features', 'port_features', 'path_features', 'char_composition', 'unique_char_ratio', 'pattern_flags'].includes(stepType)
}

function supportsExplicitOutputColumn(
  stepType: DraftFeatureStepType,
  selectorMode: FeatureStepDraft['input_selector']['mode'],
  selectedColumns: string[],
) {
  if (['ratio_feature', 'difference_feature', 'concat_fields', 'equality_flag', 'group_frequency', 'group_unique_count', 'group_duration', 'group_event_order', 'time_since_previous_event', 'time_until_next_event', 'group_value_change_flag', 'time_window_count', 'window_unique_count', 'window_target_unique_count', 'window_status_change_count', 'window_spike_flag'].includes(stepType)) {
    return true
  }
  return selectorMode === 'explicit' && selectedColumns.length <= 1
}

function getOutputModeOptions(stepType: DraftFeatureStepType) {
  if (['ratio_feature', 'difference_feature', 'concat_fields', 'equality_flag', 'group_frequency', 'group_unique_count', 'group_duration', 'group_event_order', 'time_since_previous_event', 'time_until_next_event', 'group_value_change_flag', 'time_window_count', 'window_unique_count', 'window_target_unique_count', 'window_status_change_count', 'window_spike_flag'].includes(stepType)) {
    return [{ label: '追加新特征', value: 'append_new_columns' }]
  }
  return [{ label: '追加新特征', value: 'append_new_columns' }, { label: '覆盖原字段', value: 'replace_existing' }]
}

function getSelectorLabel(stepType: DraftFeatureStepType) {
  if (stepType === 'select_features') return '保留字段'
  if (stepType === 'concat_fields') return '拼接字段'
  if (stepType === 'ratio_feature' || stepType === 'difference_feature' || stepType === 'equality_flag') {
    return '目标字段（前两个用于计算）'
  }
  if (stepType === 'group_frequency' || stepType === 'group_unique_count' || stepType === 'time_window_count' || stepType === 'window_unique_count' || stepType === 'window_spike_flag') {
    return '分组字段'
  }
  if (stepType === 'group_duration' || stepType === 'group_event_order' || stepType === 'time_since_previous_event' || stepType === 'time_until_next_event' || stepType === 'group_value_change_flag' || stepType === 'window_target_unique_count' || stepType === 'window_status_change_count') {
    return '追踪键 / 分组字段'
  }
  return '目标字段'
}

function getFeatureStepLabel(stepType: string | undefined) {
  return STEP_TYPE_OPTIONS.find((option) => option.value === stepType)?.label ?? stepType ?? '未命名步骤'
}

function inferLogType(parserProfile: string) {
  if (parserProfile === 'nginx_access') return 'nginx_access'
  if (parserProfile === 'generic_log') return 'program_runtime'
  return 'generic_log'
}

function recommendTemplateId(parserProfile: string, columns: string[], templates: FeatureTemplate[]) {
  const lowerColumns = columns.map((column) => column.toLowerCase())
  if (parserProfile === 'nginx_access') {
    return templates.find((template) => template.log_type === 'nginx_access')?.id
  }
  if (lowerColumns.includes('source_ip') && lowerColumns.includes('dest_ip') && (lowerColumns.includes('src_port') || lowerColumns.includes('dest_port'))) {
    return templates.find((template) => template.log_type === 'nta_flow')?.id
  }
  if (lowerColumns.includes('severity') || lowerColumns.includes('raw_message')) {
    return templates.find((template) => template.log_type === 'program_runtime')?.id
  }
  return templates[0]?.id
}

function describeDraftStep(step: Partial<FeatureStepDraft>) {
  const stepType = step.step_type
  const selectorDescription = describeSelector(step.input_selector)
  if (stepType === 'select_features') return `字段选择(${selectorDescription || '保留全部'})`
  if (stepType === 'derive_time_parts') return `时间派生(${selectorDescription})`
  if (stepType === 'text_length') return `文本长度(${selectorDescription})`
  if (stepType === 'byte_length') return `字节长度(${selectorDescription})`
  if (stepType === 'token_count') return `Token 数(${selectorDescription})`
  if (stepType === 'shannon_entropy') return `香农熵(${selectorDescription})`
  if (stepType === 'char_composition') return `字符组成比例(${selectorDescription})`
  if (stepType === 'unique_char_ratio') return `唯一字符占比(${selectorDescription})`
  if (stepType === 'regex_match_count') return `正则命中(${selectorDescription} -> ${step.params?.regexPattern || '未填正则'})`
  if (stepType === 'pattern_flags') return `模式布尔标记(${selectorDescription})`
  if (stepType === 'keyword_count') return `关键词计数(${selectorDescription} -> ${step.params?.keywordsText || '未填关键词'})`
  if (stepType === 'frequency_encode') return `频次编码(${selectorDescription})`
  if (stepType === 'category_encode') return `类别编码(${selectorDescription})`
  if (stepType === 'numeric_bucket') return `数值分桶(${selectorDescription} -> ${step.params?.bins || 5} 桶)`
  if (stepType === 'numeric_scale') return `数值标准化(${selectorDescription} -> ${step.params?.method || 'zscore'})`
  if (stepType === 'ratio_feature') return `字段比值(${selectorDescription})`
  if (stepType === 'difference_feature') return `字段差值(${selectorDescription})`
  if (stepType === 'concat_fields') return `字段拼接(${selectorDescription} -> ${step.params?.separator || '|'})`
  if (stepType === 'equality_flag') return `字段相等标记(${selectorDescription})`
  if (stepType === 'group_frequency') return `组内频次(${selectorDescription})`
  if (stepType === 'group_unique_count') return `组内去重数(${selectorDescription} -> ${step.params?.targetColumn || '未选目标字段'})`
  if (stepType === 'group_duration') return `组内持续时长(${selectorDescription} -> ${step.params?.timeColumn || '未选时间字段'})`
  if (stepType === 'group_event_order') return `组内事件序号(${selectorDescription} -> ${step.params?.timeColumn || '未选时间字段'})`
  if (stepType === 'time_since_previous_event') return `距离上一事件(${selectorDescription} -> ${step.params?.timeColumn || '未选时间字段'})`
  if (stepType === 'time_until_next_event') return `距离下一事件(${selectorDescription} -> ${step.params?.timeColumn || '未选时间字段'})`
  if (stepType === 'group_value_change_flag') return `组内值变化标记(${selectorDescription} -> ${step.params?.targetColumn || '未选变化字段'} / ${step.params?.timeColumn || '未选时间字段'})`
  if (stepType === 'time_window_count') return `时间窗计数(${selectorDescription || '全量'} -> ${step.params?.timeColumn || '未选时间字段'} / ${step.params?.windowMinutes || 15} 分钟)`
  if (stepType === 'window_unique_count') return `时间窗去重数(${selectorDescription || '全量'} -> ${step.params?.targetColumn || '未选目标字段'} / ${step.params?.timeColumn || '未选时间字段'} / ${step.params?.windowMinutes || 15} 分钟)`
  if (stepType === 'window_target_unique_count') return `时间窗目标去重数(${selectorDescription || '全量'} -> ${step.params?.targetColumn || '未选目标字段'} / ${step.params?.timeColumn || '未选时间字段'} / ${step.params?.windowMinutes || 15} 分钟)`
  if (stepType === 'window_status_change_count') return `时间窗状态变化数(${selectorDescription || '全量'} -> ${step.params?.targetColumn || '未选状态字段'} / ${step.params?.timeColumn || '未选时间字段'} / ${step.params?.windowMinutes || 15} 分钟)`
  if (stepType === 'window_spike_flag') return `时间窗突增标记(${selectorDescription || '全量'} -> ${step.params?.timeColumn || '未选时间字段'} / ${step.params?.windowMinutes || 15} 分钟 / 阈值 ${step.params?.threshold || 10})`
  if (stepType === 'ip_features') return `IP 基础特征(${selectorDescription})`
  if (stepType === 'port_features') return `端口基础特征(${selectorDescription})`
  if (stepType === 'path_features') return `路径特征(${selectorDescription})`
  if (stepType === 'status_category') return `状态码类别(${selectorDescription})`
  if (stepType === 'value_map') return `值映射(${selectorDescription})`
  if (stepType === 'boolean_flag') return `布尔标记(${selectorDescription} ${step.params?.operator || 'contains'} ${step.params?.value || ''})`
  return stepType ?? '未命名步骤'
}

function describePersistedStep(step: FeatureStep) {
  return describeDraftStep(toDraftFromPersistedStep(step))
}

function describeSelector(selector: Partial<FeatureStepDraft['input_selector']> | undefined) {
  if (!selector) return '未选字段'
  if (selector.mode === 'dtype') return `按类型:${selector.dtype ?? '未选'}`
  if (selector.mode === 'role_tag') return `按角色:${selector.role_tag ?? '未选'}`
  if (selector.mode === 'name_pattern') return `按名称规则:${selector.name_pattern ?? '未填'}`
  return selector.columns?.join(', ') || '未选字段'
}
