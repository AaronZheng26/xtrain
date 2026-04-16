export type HealthComponent = {
  status: string
  detail: string
}

export type HealthRead = {
  api: HealthComponent
  sqlite: HealthComponent
  storage: HealthComponent
  ollama: HealthComponent
}

export type Project = {
  id: number
  name: string
  description: string
  status: string
  created_at: string
}

export type DashboardRoute = {
  pathname: '/'
}

export type WorkspaceTabKey = 'data' | 'preprocess' | 'feature' | 'training' | 'analysis'

export type ProjectWorkspaceRoute = {
  pathname: `/projects/${number}`
  tab?: WorkspaceTabKey
}

export type Job = {
  id: number
  name: string
  job_type: string
  status: string
  progress: number
  message: string
  created_at: string
  updated_at: string
}

export type JobSubmissionRead = {
  job: Job
  resource_id: number
  resource_type: string
}

export type SchemaField = {
  name: string
  dtype: string
  null_count: number
  non_null_count: number
  sample_values: string[]
  candidate_roles: string[]
}

export type DatasetDetectedFields = {
  timestamp_candidates: string[]
  label_candidates: string[]
  numeric_fields: string[]
  categorical_fields: string[]
  text_fields: string[]
}

export type DataSource = {
  id: number
  project_id: number
  file_name: string
  file_type: string
  parser_profile: string
  storage_path: string
  status: string
  row_count: number
  created_at: string
}

export type DatasetVersion = {
  id: number
  project_id: number
  source_id: number
  version_name: string
  parser_profile: string
  parquet_path: string
  row_count: number
  label_column: string | null
  schema_snapshot: SchemaField[]
  detected_fields: DatasetDetectedFields
  created_at: string
}

export type DatasetImportRead = {
  data_source: DataSource
  dataset_version: DatasetVersion
}

export type LogTemplate = {
  id: string
  name: string
  log_type: string
  description: string
  parser_profile: string
}

export type ImportSession = {
  id: number
  project_id: number
  file_name: string
  file_type: string
  raw_file_path: string
  status: string
  selected_template_id: string
  parser_profile: string
  parse_options: Record<string, unknown>
  cleaning_options: Record<string, unknown>
  field_mapping: Record<string, string | null>
  preview_schema: SchemaField[]
  detected_fields: DatasetDetectedFields
  preview_rows: Record<string, unknown>[]
  error_rows: Record<string, unknown>[]
  row_count: number
  confirmed_dataset_version_id: number | null
  template_suggestions: LogTemplate[]
  created_at: string
  updated_at: string
}

export type ImportSessionConfirmRead = {
  import_session: ImportSession
  import_result: DatasetImportRead
}

export type DatasetPreviewRead = {
  dataset_id: number
  columns: string[]
  rows: Record<string, unknown>[]
}

export type DatasetWorkspaceRead = {
  dataset: DatasetVersion
  preview: DatasetPreviewRead
  field_mapping: FieldMapping
  preprocess_pipelines: PreprocessPipeline[]
  feature_pipelines: FeaturePipeline[]
  models: ModelVersion[]
}

export type FieldMapping = {
  id: number
  dataset_version_id: number
  mappings: Record<string, string | null>
  confirmed: boolean
  created_at: string
  updated_at: string
}

export type PreprocessInputSelector = {
  mode?: 'explicit' | 'dtype'
  columns?: string[]
  dtype?: 'string' | 'numeric' | 'datetime'
}

export type PreprocessOutputMode = {
  mode?: 'inplace' | 'new_column'
  output_column?: string
  suffix?: string
}

export type PreprocessStep = {
  step_id?: string
  step_type?: string
  type?: string
  enabled?: boolean
  input_selector?: PreprocessInputSelector
  params: Record<string, unknown>
  output_mode?: PreprocessOutputMode
}

export type PreprocessPipeline = {
  id: number
  project_id: number
  dataset_version_id: number
  name: string
  status: string
  steps: PreprocessStep[]
  output_path: string | null
  output_row_count: number
  output_schema: SchemaField[]
  created_at: string
  updated_at: string
}

export type PreprocessPreviewRead = {
  pipeline_id: number
  columns: string[]
  rows: Record<string, unknown>[]
}

export type PreprocessStepPreviewRead = {
  preview_step_index: number
  step: PreprocessStep
  before_row_count: number
  after_row_count: number
  before_columns: string[]
  after_columns: string[]
  added_columns: string[]
  removed_columns: string[]
  before_rows: Record<string, unknown>[]
  after_rows: Record<string, unknown>[]
}

export type FieldAdvice = {
  field: string
  status: string
  reason_code: string
  reason_text: string
  recommended_action: string
  confidence: 'high' | 'medium' | 'low' | string
  feature_handoff?: FeatureHandoff | null
}

export type PreprocessFieldIssueGroup = {
  issue_type: string
  title: string
  description: string
  fields: string[]
  recommended_action: string
  handoff_target?: string | null
}

export type FeatureHandoff = {
  issue_type: 'behavior_tracking' | 'raw_text_column' | 'high_cardinality' | string
  task_category?: 'text_complexity' | 'high_cardinality' | 'time_behavior' | 'behavior_tracking' | 'numeric_statistics' | string
  tracking_type: 'flow' | 'entity' | string
  recommended_group_key: string
  recommended_time_columns: string[]
  recommended_target_columns: string[]
  recipe_ids: string[]
  reason_code?: string
}

export type RecommendedPreprocessStepDraft = {
  recommendation_id: string
  title: string
  description: string
  step: PreprocessStep
}

export type PreprocessTrainingAdvisorSummary = {
  direct_trainable_fields: number
  high_risk_fields: number
  pending_fields: number
  total_fields: number
  target_column: string | null
  suggested_training_columns: string[]
  excluded_training_columns: string[]
  analysis_basis: string
}

export type PreprocessTrainingAdvisorRead = {
  summary: PreprocessTrainingAdvisorSummary
  field_advice: FieldAdvice[]
  issue_groups: PreprocessFieldIssueGroup[]
  recommended_steps: RecommendedPreprocessStepDraft[]
  analysis_mode: 'quick' | 'sampled_trainability' | string
  sample_size: number
  generated_at: string
}

export type PreprocessTrainingAdvisorRunRead = {
  id: number
  project_id: number
  dataset_version_id: number
  job_id: number | null
  status: string
  analysis_mode: 'sampled_trainability' | string
  sample_size: number
  result: PreprocessTrainingAdvisorRead | null
  created_at: string
  updated_at: string
}

export type FeatureStep = {
  step_id?: string
  step_type?: string
  type?: string
  enabled?: boolean
  input_selector?: {
    mode?: 'explicit' | 'dtype' | 'role_tag' | 'name_pattern'
    columns?: string[]
    dtype?: 'string' | 'numeric' | 'datetime'
    role_tag?: 'text' | 'path' | 'user_agent' | 'domain' | 'ip'
    name_pattern?: string
  }
  params: Record<string, unknown>
  output_mode?: {
    mode?: 'append_new_columns' | 'replace_existing' | 'output_column_map'
    output_column?: string
    suffix?: string
    output_column_map?: Record<string, string>
  }
}

export type FeaturePipeline = {
  id: number
  project_id: number
  dataset_version_id: number
  preprocess_pipeline_id: number | null
  name: string
  status: string
  steps: FeatureStep[]
  output_path: string | null
  output_row_count: number
  output_schema: SchemaField[]
  created_at: string
  updated_at: string
}

export type FeaturePreviewRead = {
  pipeline_id: number
  columns: string[]
  rows: Record<string, unknown>[]
}

export type FeatureTemplate = {
  id: string
  project_id: number | null
  scope: 'builtin' | 'project'
  name: string
  log_type: string
  description: string
  steps: FeatureStep[]
  field_hints: {
    required_columns?: string[]
    optional_columns?: string[]
  }
  created_at?: string | null
  updated_at?: string | null
}

export type FeatureTaskCategoryId =
  | 'text_complexity'
  | 'high_cardinality'
  | 'time_behavior'
  | 'behavior_tracking'
  | 'numeric_statistics'

export type FeatureTaskCategory = {
  id: FeatureTaskCategoryId
  title: string
  description: string
  recommended_for: string[]
  default_recipe_ids: string[]
}

export type FeatureRecipe = {
  id: string
  task_category: FeatureTaskCategoryId
  title: string
  description: string
  generated_feature_descriptions: string[]
  recommended_steps: FeatureStep[]
}

export type FeatureStepPreviewRead = {
  preview_step_index: number
  step: FeatureStep
  before_row_count: number
  after_row_count: number
  before_columns: string[]
  after_columns: string[]
  added_columns: string[]
  removed_columns: string[]
  before_rows: Record<string, unknown>[]
  after_rows: Record<string, unknown>[]
}

export type ModelVersion = {
  id: number
  project_id: number
  dataset_version_id: number
  preprocess_pipeline_id: number | null
  feature_pipeline_id: number | null
  job_id: number | null
  name: string
  mode: string
  algorithm: string
  status: string
  target_column: string | null
  feature_columns: string[]
  used_feature_columns: string[]
  excluded_feature_columns: string[]
  exclusion_reasons: Record<string, string>
  training_params: Record<string, unknown>
  metrics: Record<string, unknown>
  report_json: Record<string, unknown>
  artifact_path: string | null
  prediction_path: string | null
  created_at: string
  updated_at: string
}

export type ModelPreviewRead = {
  model_id: number
  metrics: Record<string, unknown>
  columns: string[]
  rows: Record<string, unknown>[]
}

export type ModelAnalysisScorePoint = {
  sample_index: number
  anomaly_score: number
  predicted_label: string
  actual_label: string | null
}

export type ModelAnalysisHistogramBucket = {
  bucket_label: string
  range_start: number
  range_end: number
  normal_count: number
  anomaly_count: number
}

export type ModelAnalysisEmbeddingPoint = {
  x: number
  y: number
  predicted_label: string
  anomaly_score: number
  actual_label: string | null
}

export type ModelAnalysisSignalSummary = {
  column: string
  signal_type: string
  anomaly_mean: number | null
  normal_mean: number | null
  anomaly_max: number | null
  normal_max: number | null
  anomaly_active_count: number | null
  normal_active_count: number | null
  anomaly_active_rate: number | null
  normal_active_rate: number | null
}

export type ModelAnalysisRead = {
  model_id: number
  mode: string
  metrics: Record<string, unknown>
  sample_size: number
  anomaly_count: number
  score_points: ModelAnalysisScorePoint[]
  score_histogram: ModelAnalysisHistogramBucket[]
  embedding_points: ModelAnalysisEmbeddingPoint[]
  spike_signal_summaries: ModelAnalysisSignalSummary[]
  count_signal_summaries: ModelAnalysisSignalSummary[]
}

export type LlmProviderConfig = {
  id: number
  project_id: number
  provider: string
  enabled: boolean
  base_url: string
  model_name: string
  has_api_key: boolean
  api_key_hint: string | null
  created_at: string
  updated_at: string
}

export type LlmProviderConfigPayload = {
  provider: string
  enabled: boolean
  base_url: string
  model_name: string
  api_key?: string | null
  clear_api_key: boolean
}

export type ModelLlmExplanationRead = {
  model_id: number
  provider: string
  model_name: string
  analyzed_rows: number
  explanation: string
  final_content: string
  reasoning_content: string | null
  source_columns: string[]
  source_rows: Record<string, unknown>[]
  generated_at: string
}

export type LlmProviderTestRead = {
  provider: string
  model_name: string
  base_url: string
  success: boolean
  detail: string
}

export type DatasetSummary = {
  id: number
  project_id: number
  version_name: string
  parser_profile: string
  row_count: number
  label_column: string | null
  created_at: string
}

export type ModelSummary = {
  id: number
  project_id: number
  dataset_version_id: number
  name: string
  mode: string
  algorithm: string
  status: string
  created_at: string
}

export type DashboardSummaryRead = {
  project_count: number
  dataset_count: number
  model_count: number
  job_count: number
  recent_projects: Project[]
  recent_jobs: Job[]
  recent_datasets: DatasetSummary[]
  recent_models: ModelSummary[]
}

export type WorkspaceContext = {
  project: Project | null
  selectedDatasetId: number | null
  selectedPreprocessId: number | null
  selectedFeatureId: number | null
  selectedModelId: number | null
  activeTab: WorkspaceTabKey
}
