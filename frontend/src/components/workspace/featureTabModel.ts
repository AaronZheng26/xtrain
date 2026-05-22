import type {
  FeatureHandoff,
  FeatureRecipe,
  FeatureStep,
  FeatureTaskCategory,
  FeatureTaskCategoryId,
  FeatureTemplate,
} from '../../types'

export type DraftFeatureStepType =
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

export type FeatureWizardState = {
  mode: 'task_home' | 'handoff_wizard'
  selectedRecipeId: string | null
  pendingRecipe: FeatureRecipe | null
}

export const STEP_TYPE_OPTIONS: Array<{ label: string; value: DraftFeatureStepType }> = [
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

export const SELECTOR_MODE_OPTIONS = [
  { label: '显式字段', value: 'explicit' },
  { label: '按字段类型', value: 'dtype' },
  { label: '按角色标签', value: 'role_tag' },
  { label: '按名称规则', value: 'name_pattern' },
]

export const FEATURE_DTYPE_OPTIONS = [
  { label: '字符串', value: 'string' },
  { label: '数值', value: 'numeric' },
  { label: '时间', value: 'datetime' },
]

export const ROLE_TAG_OPTIONS = [
  { label: '文本字段', value: 'text' },
  { label: '路径/URL', value: 'path' },
  { label: 'User-Agent', value: 'user_agent' },
  { label: '域名/主机', value: 'domain' },
  { label: 'IP 字段', value: 'ip' },
]

export const PATTERN_FLAG_OPTIONS = [
  { label: 'IP', value: 'ip' },
  { label: 'URL', value: 'url' },
  { label: 'Hash', value: 'hash' },
  { label: 'Hex-like', value: 'hex_like' },
  { label: 'Base64-like', value: 'base64_like' },
  { label: '邮箱', value: 'email' },
]

export const LOG_TYPE_OPTIONS = [
  { label: 'Nginx 访问日志', value: 'nginx_access' },
  { label: '程序运行日志', value: 'program_runtime' },
  { label: 'NTA 流量日志', value: 'nta_flow' },
  { label: '通用日志', value: 'generic_log' },
]

export const FEATURE_TASK_CATEGORIES: FeatureTaskCategory[] = [
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

export const BEHAVIOR_RECIPE_OPTIONS = [
  { label: '基础行为统计', value: 'base' },
  { label: '时间窗行为', value: 'window' },
  { label: '顺序特征', value: 'sequence' },
]

export function isFeatureTaskCategoryId(value: string | undefined): value is FeatureTaskCategoryId {
  return FEATURE_TASK_CATEGORIES.some((category) => category.id === value)
}

export function createBehaviorTrackingDefaults(featureHandoff: FeatureHandoff | null, availableColumns: string[]) {
  const recommendedTime = featureHandoff?.recommended_time_columns.find((column) => availableColumns.includes(column))
  const recommendedTargets = (featureHandoff?.recommended_target_columns ?? [])
    .filter((column) => availableColumns.includes(column))
    .slice(0, 2)
  const recipeKinds: Array<'base' | 'window' | 'sequence'> = featureHandoff?.tracking_type === 'entity'
    ? ['base', 'window']
    : ['base', 'sequence']

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

export function createQuickTaskDefaults(featureHandoff: FeatureHandoff | null, availableColumns: string[]) {
  return {
    targetColumns: featureHandoff?.recommended_group_key && availableColumns.includes(featureHandoff.recommended_group_key)
      ? [featureHandoff.recommended_group_key]
      : [],
    timeColumn: featureHandoff?.recommended_time_columns.find((column) => availableColumns.includes(column)),
    groupColumns: (featureHandoff?.recommended_target_columns ?? []).filter((column) => availableColumns.includes(column)).slice(0, 2),
  }
}

export function buildFeatureRecipes({
  handoff,
  quickStrategy,
  selectedTaskCategory,
  behaviorTrackingSummary,
  behaviorTrackingSteps,
  quickTaskPreview,
  quickTaskSteps,
  recommendedTrainingColumns,
  analysisRetainedColumns,
}: {
  handoff: FeatureHandoff | null
  quickStrategy: FeatureFormValues['quickStrategy'] | undefined
  selectedTaskCategory: FeatureTaskCategory | null
  behaviorTrackingSummary: ReturnType<typeof describeBehaviorTrackingPlan>
  behaviorTrackingSteps: FeatureStepDraft[]
  quickTaskPreview: ReturnType<typeof describeQuickTaskPlan>
  quickTaskSteps: FeatureStepDraft[]
  recommendedTrainingColumns: string[]
  analysisRetainedColumns: string[]
}): FeatureRecipe[] {
  if (quickStrategy === 'template') {
    return []
  }

  if (quickStrategy === 'behavior_tracking') {
    const trackingLabel = handoff?.tracking_type === 'entity' ? '主体追踪' : '流程追踪'
    return [
      {
        id: handoff?.recipe_ids[0] ?? `behavior_tracking_${handoff?.tracking_type ?? 'flow'}`,
        task_category: 'behavior_tracking',
        title: `${trackingLabel}推荐方案`,
        description: behaviorTrackingSummary.description,
        generated_feature_descriptions: behaviorTrackingSummary.generatedColumns,
        recommended_steps: behaviorTrackingSteps.map(toPersistedFromDraftStep),
        training_candidate_descriptions: recommendedTrainingColumns.length
          ? recommendedTrainingColumns.map((column) => `${column}：用于训练的行为统计/窗口/顺序特征`)
          : ['当前方案将优先生成行为追踪数值特征，再进入训练。'],
        analysis_retained_descriptions: analysisRetainedColumns.length
          ? analysisRetainedColumns.map((column) => `${column}：保留作上下文、溯源和解释`)
          : ['原始追踪键与上下文字段保留作分析，不直接混入训练。'],
      },
    ]
  }

  const taskCategory = selectedTaskCategory?.id ?? 'text_complexity'
  const taskTitle = selectedTaskCategory?.title ?? '推荐方案'
  return [
    {
      id: handoff?.recipe_ids[0] ?? `${taskCategory}_recipe`,
      task_category: taskCategory,
      title: `${taskTitle}推荐方案`,
      description: quickTaskPreview.description,
      generated_feature_descriptions: quickTaskPreview.generatedColumns,
      recommended_steps: quickTaskSteps.map(toPersistedFromDraftStep),
      training_candidate_descriptions: recommendedTrainingColumns.length
        ? recommendedTrainingColumns.map((column) => `${column}：默认进入训练的派生特征`)
        : ['当前方案会把新生成的统计特征优先作为训练候选。'],
      analysis_retained_descriptions: analysisRetainedColumns.length
        ? analysisRetainedColumns.map((column) => `${column}：保留作分析和解释，不默认进入训练`)
        : ['原始字段继续保留在输出里，便于异常解释和追踪。'],
    },
  ]
}

export function toPersistedFromDraftStep(step: FeatureStepDraft): FeatureStep {
  const normalizedParams: Record<string, unknown> = {}
  const selectorMode = step.input_selector?.mode ?? 'explicit'

  if (step.params.prefix) normalizedParams.prefix = step.params.prefix
  if (step.params.keywordsText?.trim()) {
    normalizedParams.keywords = step.params.keywordsText.split(',').map((item) => item.trim()).filter(Boolean)
  }
  if (step.params.bins) normalizedParams.bins = Number(step.params.bins)
  if (step.params.method) normalizedParams.method = step.params.method
  if (step.params.separator?.trim()) normalizedParams.separator = step.params.separator.trim()
  if (step.params.targetColumn?.trim()) normalizedParams.target_column = step.params.targetColumn.trim()
  if (step.params.timeColumn?.trim()) normalizedParams.time_column = step.params.timeColumn.trim()
  if (step.params.windowMinutes) normalizedParams.window_minutes = Number(step.params.windowMinutes)
  if (step.params.threshold) normalizedParams.threshold = Number(step.params.threshold)
  if (step.params.mappingText?.trim()) {
    try {
      normalizedParams.mapping = JSON.parse(step.params.mappingText)
    } catch {
      normalizedParams.mapping = {}
    }
  }
  if (step.params.defaultValue !== undefined && step.params.defaultValue !== '') {
    normalizedParams.default_value = step.params.defaultValue
  }
  if (step.params.operator) normalizedParams.operator = step.params.operator
  if (step.params.value !== undefined && step.params.value !== '') normalizedParams.value = step.params.value
  if (step.params.regexPattern?.trim()) normalizedParams.pattern = step.params.regexPattern.trim()
  if (step.params.patternFlags?.length) normalizedParams.patterns = step.params.patternFlags

  return {
    step_id: step.step_id,
    step_type: step.step_type,
    enabled: step.enabled,
    input_selector: {
      mode: selectorMode,
      columns: selectorMode === 'explicit' ? step.input_selector.columns : [],
      dtype: selectorMode === 'dtype' ? step.input_selector.dtype : undefined,
      role_tag: selectorMode === 'role_tag' ? step.input_selector.role_tag : undefined,
      name_pattern: selectorMode === 'name_pattern' ? step.input_selector.name_pattern : undefined,
    },
    params: normalizedParams,
    output_mode: {
      mode: step.output_mode.mode,
      output_column: step.output_mode.output_column,
      suffix: step.output_mode.suffix,
    },
  }
}

export function buildQuickTaskSteps(
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

export function describeQuickTaskPlan(
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

export function inferDraftGeneratedColumns(steps: FeatureStepDraft[]) {
  return steps
    .filter((step) => step.enabled !== false)
    .map((step) => describeGeneratedColumn(step))
    .filter((column, index, columns) => Boolean(column) && columns.indexOf(column) === index)
}

export function buildBehaviorTrackingSteps(
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

export function describeBehaviorTrackingPlan(
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

export function createFeatureStepDraft(stepType: DraftFeatureStepType = 'frequency_encode'): FeatureStepDraft {
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

export function toDraftFromPersistedStep(step: FeatureStep): FeatureStepDraft {
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

export function supportsOutputModeConfig(stepType: DraftFeatureStepType) {
  return !['select_features', 'ip_features', 'port_features', 'path_features', 'char_composition', 'unique_char_ratio', 'pattern_flags'].includes(stepType)
}

export function supportsExplicitOutputColumn(
  stepType: DraftFeatureStepType,
  selectorMode: FeatureStepDraft['input_selector']['mode'],
  selectedColumns: string[],
) {
  if (['ratio_feature', 'difference_feature', 'concat_fields', 'equality_flag', 'group_frequency', 'group_unique_count', 'group_duration', 'group_event_order', 'time_since_previous_event', 'time_until_next_event', 'group_value_change_flag', 'time_window_count', 'window_unique_count', 'window_target_unique_count', 'window_status_change_count', 'window_spike_flag'].includes(stepType)) {
    return true
  }
  return selectorMode === 'explicit' && selectedColumns.length <= 1
}

export function getOutputModeOptions(stepType: DraftFeatureStepType) {
  if (['ratio_feature', 'difference_feature', 'concat_fields', 'equality_flag', 'group_frequency', 'group_unique_count', 'group_duration', 'group_event_order', 'time_since_previous_event', 'time_until_next_event', 'group_value_change_flag', 'time_window_count', 'window_unique_count', 'window_target_unique_count', 'window_status_change_count', 'window_spike_flag'].includes(stepType)) {
    return [{ label: '追加新特征', value: 'append_new_columns' }]
  }
  return [{ label: '追加新特征', value: 'append_new_columns' }, { label: '覆盖原字段', value: 'replace_existing' }]
}

export function getSelectorLabel(stepType: DraftFeatureStepType) {
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

export function getFeatureStepLabel(stepType: string | undefined) {
  return STEP_TYPE_OPTIONS.find((option) => option.value === stepType)?.label ?? stepType ?? '未命名步骤'
}

export function inferLogType(parserProfile: string) {
  if (parserProfile === 'nginx_access') return 'nginx_access'
  if (parserProfile === 'generic_log') return 'program_runtime'
  return 'generic_log'
}

export function recommendTemplateId(parserProfile: string, columns: string[], templates: FeatureTemplate[]) {
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

export function describeDraftStep(step: Partial<FeatureStepDraft>) {
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

export function describePersistedStep(step: FeatureStep) {
  return describeDraftStep(toDraftFromPersistedStep(step))
}

function createFeatureTaskStep(
  stepType: DraftFeatureStepType,
  columns: string[],
  params: Partial<FeatureStepDraft['params']> = {},
): FeatureStepDraft {
  const draft = createFeatureStepDraft(stepType)
  return {
    ...draft,
    input_selector: {
      ...draft.input_selector,
      mode: 'explicit',
      columns,
    },
    params: {
      ...draft.params,
      ...params,
    },
    output_mode: {
      ...draft.output_mode,
      mode: (getOutputModeOptions(stepType)[0]?.value ?? 'append_new_columns') as 'append_new_columns' | 'replace_existing',
    },
  }
}

function describeGeneratedColumn(step: FeatureStepDraft) {
  return step.output_mode.output_column || `${(step.input_selector.columns ?? []).join('_')}${step.output_mode.suffix ?? ''}`
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
  const draft = createFeatureStepDraft(stepType)
  return {
    ...draft,
    input_selector: { mode: 'explicit', columns },
    params: {
      ...draft.params,
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

function describeSelector(selector: Partial<FeatureStepDraft['input_selector']> | undefined) {
  if (!selector) return '未选字段'
  if (selector.mode === 'dtype') return `按类型:${selector.dtype ?? '未选'}`
  if (selector.mode === 'role_tag') return `按角色:${selector.role_tag ?? '未选'}`
  if (selector.mode === 'name_pattern') return `按名称规则:${selector.name_pattern ?? '未填'}`
  return selector.columns?.join(', ') || '未选字段'
}
