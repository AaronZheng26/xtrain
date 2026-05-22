import { useEffect, useMemo, useState } from 'react'
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
  FeatureRecipe,
  FeatureStepPreviewRead,
  FeatureTemplate,
  FeatureHandoff,
  PreprocessPipeline,
} from '../../types'
import {
  BEHAVIOR_RECIPE_OPTIONS,
  FEATURE_DTYPE_OPTIONS,
  FEATURE_TASK_CATEGORIES,
  LOG_TYPE_OPTIONS,
  PATTERN_FLAG_OPTIONS,
  ROLE_TAG_OPTIONS,
  SELECTOR_MODE_OPTIONS,
  STEP_TYPE_OPTIONS,
  buildBehaviorTrackingSteps,
  buildFeatureRecipes,
  buildQuickTaskSteps,
  createBehaviorTrackingDefaults,
  createFeatureStepDraft,
  createQuickTaskDefaults,
  describeBehaviorTrackingPlan,
  describeDraftStep,
  describePersistedStep,
  describeQuickTaskPlan,
  getFeatureStepLabel,
  getOutputModeOptions,
  getSelectorLabel,
  inferDraftGeneratedColumns,
  inferLogType,
  isFeatureTaskCategoryId,
  recommendTemplateId,
  supportsExplicitOutputColumn,
  supportsOutputModeConfig,
  toDraftFromPersistedStep,
} from './featureTabModel'
import type { FeatureFormValues, FeatureWizardState } from './featureTabModel'

export type { FeatureFormValues } from './featureTabModel'

const { Text } = Typography
const { TextArea } = Input

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

export function FeatureTab(props: Props) {
  const [form] = Form.useForm<FeatureFormValues>()
  const [featureWizardState, setFeatureWizardState] = useState<FeatureWizardState>({
    mode: 'task_home',
    selectedRecipeId: null,
    pendingRecipe: null,
  })
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
  const previewTrainingCandidateColumns = props.preview?.training_candidate_columns ?? []
  const previewBusinessContextColumns = props.preview?.business_context_columns ?? []
  const previewAnalysisRetainedColumns = props.preview?.analysis_retained_columns ?? []
  const previewFeatureLineage = props.preview?.feature_lineage ?? props.selectedPipeline?.feature_lineage ?? {}
  const recommendedTrainingColumns = useMemo(() => {
    if (previewTrainingCandidateColumns.length) return previewTrainingCandidateColumns
    if (props.selectedPipeline?.training_candidate_columns.length) return props.selectedPipeline.training_candidate_columns
    if (mode === 'quick' && quickStrategy === 'behavior_tracking') return behaviorTrackingSummary.generatedColumns
    if (mode === 'quick' && quickStrategy !== 'template') return quickTaskPreview.generatedColumns
    return inferDraftGeneratedColumns(draftSteps)
  }, [
    behaviorTrackingSummary.generatedColumns,
    draftSteps,
    mode,
    previewTrainingCandidateColumns,
    props.selectedPipeline,
    quickStrategy,
    quickTaskPreview.generatedColumns,
  ])
  const analysisRetainedColumns = useMemo(() => {
    if (previewAnalysisRetainedColumns.length) return previewAnalysisRetainedColumns
    if (props.selectedPipeline?.analysis_retained_columns.length) return props.selectedPipeline.analysis_retained_columns
    if (mode === 'quick' && quickStrategy === 'behavior_tracking') {
      return [
        behaviorTracking?.groupKey,
        behaviorTracking?.timeColumn,
        ...(behaviorTracking?.targetColumns ?? []),
      ].filter((column): column is string => Boolean(column))
    }
    if (mode === 'quick' && quickStrategy !== 'template') {
      return quickTaskConfig?.targetColumns ?? []
    }
    return []
  }, [
    behaviorTracking,
    mode,
    previewAnalysisRetainedColumns,
    props.selectedPipeline,
    quickStrategy,
    quickTaskConfig,
  ])
  const businessContextColumns = useMemo(() => {
    if (previewBusinessContextColumns.length) return previewBusinessContextColumns
    if (props.selectedPipeline?.business_context_columns.length) return props.selectedPipeline.business_context_columns
    if (mode === 'quick' && quickStrategy === 'behavior_tracking') {
      return [
        behaviorTracking?.groupKey,
        behaviorTracking?.timeColumn,
        ...(behaviorTracking?.targetColumns ?? []),
      ].filter((column): column is string => Boolean(column))
    }
    return []
  }, [
    behaviorTracking,
    mode,
    previewBusinessContextColumns,
    props.selectedPipeline,
    quickStrategy,
  ])
  const featureRecipes = useMemo(
    () => buildFeatureRecipes({
      handoff: props.featureHandoff,
      quickStrategy,
      selectedTaskCategory,
      behaviorTrackingSummary,
      behaviorTrackingSteps,
      quickTaskPreview,
      quickTaskSteps,
      recommendedTrainingColumns,
      analysisRetainedColumns,
    }),
    [
      analysisRetainedColumns,
      behaviorTrackingSteps,
      behaviorTrackingSummary,
      props.featureHandoff,
      quickStrategy,
      quickTaskPreview,
      quickTaskSteps,
      recommendedTrainingColumns,
      selectedTaskCategory,
    ],
  )
  const effectiveWizardMode = props.featureHandoff ? 'handoff_wizard' : featureWizardState.mode
  const effectiveSelectedRecipeId = featureWizardState.selectedRecipeId
    ?? props.featureHandoff?.recipe_ids[0]
    ?? featureRecipes[0]?.id
    ?? null
  const selectedRecipe = useMemo(
    () => featureRecipes.find((recipe) => recipe.id === effectiveSelectedRecipeId) ?? featureRecipes[0] ?? null,
    [effectiveSelectedRecipeId, featureRecipes],
  )
  const isHandoffWizardActive = mode === 'quick' && effectiveWizardMode === 'handoff_wizard' && Boolean(props.featureHandoff)

  function handleSelectRecipe(recipe: FeatureRecipe) {
    setFeatureWizardState((current) => ({
      ...current,
      selectedRecipeId: recipe.id,
      pendingRecipe: recipe,
    }))
  }

  function handleAcceptRecipe() {
    if (!selectedRecipe) return
    form.setFieldsValue({
      mode: 'advanced',
      steps: selectedRecipe.recommended_steps.map(toDraftFromPersistedStep),
    })
    setFeatureWizardState({
      mode: 'task_home',
      selectedRecipeId: selectedRecipe.id,
      pendingRecipe: selectedRecipe,
    })
    props.onClearFeatureHandoff()
  }

  function handleReturnToTaskHome() {
    setFeatureWizardState((current) => ({
      ...current,
      mode: 'task_home',
      pendingRecipe: selectedRecipe,
    }))
    props.onClearFeatureHandoff()
  }

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
                    {isHandoffWizardActive && props.featureHandoff ? (
                      <Card size="small" className="nested-card" title="预处理承接向导">
                        <Space direction="vertical" size={16} className="full-width">
                          <Alert
                            type="info"
                            showIcon
                            message="这一步来自预处理页的字段建议。先确认推荐方案，再生成特征草稿。"
                            description="这里不会直接把步骤链写入当前草稿。你先看来源、推荐任务和将生成的训练特征，确认后再生成草稿。"
                          />
                          <Descriptions
                            column={1}
                            items={[
                              { key: 'source-group', label: '来源问题组', children: props.featureHandoff.source_issue_group_title ?? '预处理字段建议' },
                              { key: 'reason', label: '推荐原因', children: props.featureHandoff.source_reason_text ?? '该字段原值不适合直接训练，建议转成更稳定的统计或行为特征。' },
                              { key: 'task', label: '推荐任务入口', children: selectedTaskCategory?.title ?? '行为追踪特征' },
                              { key: 'field', label: '推荐字段', children: props.featureHandoff.recommended_group_key || '待确认' },
                              { key: 'time', label: '推荐时间字段', children: props.featureHandoff.recommended_time_columns.join(', ') || '无' },
                              { key: 'targets', label: '推荐目标字段', children: props.featureHandoff.recommended_target_columns.join(', ') || '无' },
                            ]}
                          />
                          <div className="recipe-card-grid">
                            {featureRecipes.map((recipe) => (
                              <Card
                                key={recipe.id}
                                size="small"
                                hoverable
                                className={`task-category-card ${selectedRecipe?.id === recipe.id ? 'is-active' : ''}`}
                                onClick={() => handleSelectRecipe(recipe)}
                              >
                                <Space direction="vertical" size={8} className="full-width">
                                  <Text strong>{recipe.title}</Text>
                                  <Text type="secondary">{recipe.description}</Text>
                                  <div className="tag-wall">
                                    {recipe.generated_feature_descriptions.map((item) => (
                                      <Tag color="cyan" key={`${recipe.id}-${item}`}>{item}</Tag>
                                    ))}
                                  </div>
                                </Space>
                              </Card>
                            ))}
                          </div>
                          {selectedRecipe ? (
                            <Card size="small" type="inner" title="当前推荐方案">
                              <Space direction="vertical" size={12} className="full-width">
                                <Text>{selectedRecipe.description}</Text>
                                <div>
                                  <Text strong>将生成的特征</Text>
                                  <div className="tag-wall">
                                    {selectedRecipe.generated_feature_descriptions.map((item) => (
                                      <Tag color="green" key={`generated-${item}`}>{item}</Tag>
                                    ))}
                                  </div>
                                </div>
                                <div>
                                  <Text strong>推荐进入训练</Text>
                                  <div className="tag-wall">
                                    {selectedRecipe.training_candidate_descriptions.map((item) => (
                                      <Tag color="green" key={`train-${item}`}>{item}</Tag>
                                    ))}
                                  </div>
                                </div>
                                <div>
                                  <Text strong>仅保留作分析</Text>
                                  <div className="tag-wall">
                                    {selectedRecipe.analysis_retained_descriptions.map((item) => (
                                      <Tag color="gold" key={`analysis-${item}`}>{item}</Tag>
                                    ))}
                                  </div>
                                </div>
                                <Space wrap>
                                  <Button type="primary" onClick={handleAcceptRecipe}>
                                    确认生成特征草稿
                                  </Button>
                                  <Button onClick={handleReturnToTaskHome}>返回任务首页重新选择</Button>
                                </Space>
                              </Space>
                            </Card>
                          ) : null}
                        </Space>
                      </Card>
                    ) : (
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
                    )}

                    {quickStrategy === 'behavior_tracking' ? (
                      <Card size="small" className="nested-card" title="行为追踪特征">
                        <Space direction="vertical" size={16} className="full-width">
                          {props.featureHandoff && !isHandoffWizardActive ? (
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
                          {props.featureHandoff && props.featureHandoff.task_category === quickStrategy && !isHandoffWizardActive ? (
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
                <Card
                  size="small"
                  className="nested-card"
                  title={
                    isHandoffWizardActive
                      ? '承接向导摘要'
                      : quickStrategy === 'behavior_tracking'
                        ? '行为追踪摘要'
                        : quickStrategy === 'template'
                          ? '模板摘要'
                          : '任务摘要'
                  }
                >
                  {isHandoffWizardActive && props.featureHandoff ? (
                    <Space direction="vertical" size={12} className="full-width">
                      <Descriptions
                        column={1}
                        items={[
                          { key: 'handoff-task', label: '推荐任务', children: selectedTaskCategory?.title ?? '行为追踪特征' },
                          { key: 'handoff-field', label: '推荐字段', children: props.featureHandoff.recommended_group_key || '待确认' },
                          { key: 'handoff-source', label: '来源', children: props.featureHandoff.source_issue_group_title ?? '预处理字段建议' },
                        ]}
                      />
                      {selectedRecipe ? (
                        <>
                          <Text>{selectedRecipe.description}</Text>
                          <Space wrap>
                            {selectedRecipe.generated_feature_descriptions.map((item) => (
                              <Tag color="green" key={`handoff-${item}`}>{item}</Tag>
                            ))}
                          </Space>
                        </>
                      ) : (
                        <Text type="secondary">等待生成推荐方案。</Text>
                      )}
                    </Space>
                  ) : quickStrategy === 'behavior_tracking' ? (
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
              <Card size="small" className="nested-card" title="训练承接说明">
                <Space direction="vertical" size={12} className="full-width">
                  <div>
                    <Text strong>推荐进入训练</Text>
                    <div className="tag-wall">
                      {recommendedTrainingColumns.length ? (
                        recommendedTrainingColumns.map((column) => (
                          <Tag color="green" key={`candidate-${column}`}>{column}</Tag>
                        ))
                      ) : (
                        <Text type="secondary">当前还没有识别到默认训练候选列。运行后如果只有原始保留字段，建议补一个统计或行为类配方。</Text>
                      )}
                    </div>
                  </div>
                  <div>
                    <Text strong>保留作业务分析</Text>
                    <div className="tag-wall">
                      {businessContextColumns.length ? (
                        businessContextColumns.map((column) => (
                          <Tag color="blue" key={`context-${column}`}>{column}</Tag>
                        ))
                      ) : (
                        <Text type="secondary">当前还没有显式标记业务上下文字段。系统会优先保留时间、主体、目标、状态和原始消息等上下文字段。</Text>
                      )}
                    </div>
                  </div>
                  <div>
                    <Text strong>仅保留作技术分析</Text>
                    <div className="tag-wall">
                      {analysisRetainedColumns.length ? (
                        analysisRetainedColumns.map((column) => (
                          <Tag color="gold" key={`retained-${column}`}>{column}</Tag>
                        ))
                      ) : (
                        <Text type="secondary">当前没有显式标记的分析保留列；原始上下文字段默认仍会落盘，便于解释和溯源。</Text>
                      )}
                    </div>
                  </div>
                  <div>
                    <Text strong>特征来源说明</Text>
                    <Space direction="vertical" size={8} className="full-width top-gap-sm">
                      {recommendedTrainingColumns.length ? (
                        recommendedTrainingColumns.slice(0, 8).map((column) => {
                          const lineage = previewFeatureLineage[column]
                          return (
                            <Card size="small" key={`lineage-${column}`}>
                              <Space direction="vertical" size={4} className="full-width">
                                <Text strong>{column}</Text>
                                <Text type="secondary">
                                  {lineage?.business_meaning ?? '当前还没有精确的 lineage 说明，默认视为用于训练的派生特征。'}
                                </Text>
                                {lineage?.source_columns?.length ? (
                                  <div className="tag-wall">
                                    {lineage.source_columns.map((source) => (
                                      <Tag key={`${column}-${source}`}>{source}</Tag>
                                    ))}
                                    <Tag color="purple">{lineage.task_category}</Tag>
                                  </div>
                                ) : null}
                              </Space>
                            </Card>
                          )
                        })
                      ) : (
                        <Text type="secondary">运行后，这里会说明每个推荐训练特征来自哪些原始字段，以及它的业务含义。</Text>
                      )}
                    </Space>
                  </div>
                  <Alert
                    type="info"
                    showIcon
                    message="特征页会先把训练候选和分析保留分开，再交给训练页确认。"
                    description="默认情况下，新生成的统计/布尔/行为特征会优先进入训练；原始 ID、原始文本和业务上下文字段继续保留在输出和预测结果里，但不会默认混入训练。"
                  />
                </Space>
              </Card>
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
