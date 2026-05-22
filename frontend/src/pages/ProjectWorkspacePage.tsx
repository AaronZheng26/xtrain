import { Suspense, lazy, useCallback, useEffect, useState } from 'react'
import { Alert, Button, Layout, Space, Spin, Tabs, message } from 'antd'
import type { UploadFile } from 'antd/es/upload/interface'
import { LineChartOutlined } from '@ant-design/icons'
import { useNavigate, useParams, useSearchParams } from 'react-router-dom'

import { WorkspaceHeader } from '../components/WorkspaceHeader'
import { AnalysisGoalCard } from '../components/workspace/AnalysisGoalCard'
import { DataReadinessCard } from '../components/workspace/DataReadinessCard'
import { QuickAnalysisPanel } from '../components/workspace/QuickAnalysisPanel'
import type { FeatureFormValues } from '../components/workspace/FeatureTab'
import type { PreprocessFormValues } from '../components/workspace/PreprocessTab'
import type { TrainingFormValues } from '../components/TrainingPanel'
import { api, getDatasetReadiness } from '../lib/api'
import { extractApiErrorMessage } from '../lib/errors'
import { useWorkspaceData } from './useWorkspaceData'
import { useWorkspaceJobs } from './useWorkspaceJobs'
import type {
  FeaturePreviewRead,
  FeatureStepPreviewRead,
  FeatureTemplate,
  FeatureHandoff,
  FieldMapping,
  ImportSession,
  ImportSessionConfirmRead,
  JobSubmissionRead,
  LlmProviderConfig,
  LlmProviderConfigPayload,
  ModelAnalysisRead,
  ModelLlmExplanationRead,
  ModelPreviewRead,
  PreprocessPreviewRead,
  PreprocessStepPreviewRead,
  PreprocessTrainingAdvisorRead,
  PreprocessTrainingAdvisorRunRead,
  AnalysisGoalState,
  DatasetReadiness,
  WorkspaceTabKey,
} from '../types'

const { Content } = Layout

const DataTab = lazy(async () => {
  const module = await import('../components/workspace/DataTab')
  return { default: module.DataTab }
})

const PreprocessTab = lazy(async () => {
  const module = await import('../components/workspace/PreprocessTab')
  return { default: module.PreprocessTab }
})

const FeatureTab = lazy(async () => {
  const module = await import('../components/workspace/FeatureTab')
  return { default: module.FeatureTab }
})

const TrainingTab = lazy(async () => {
  const module = await import('../components/workspace/TrainingTab')
  return { default: module.TrainingTab }
})

const AnalysisTab = lazy(async () => {
  const module = await import('../components/workspace/AnalysisTab')
  return { default: module.AnalysisTab }
})

const stageLabels: Record<WorkspaceTabKey, string> = {
  data: '数据',
  preprocess: '预处理',
  feature: '特征',
  training: '训练',
  analysis: '分析',
}

const LLM_REQUEST_TIMEOUT_MS = 90000
const IMPORT_REQUEST_TIMEOUT_MS = 120000

const defaultAnalysisGoal: AnalysisGoalState = {
  goal: 'web_access_anomaly',
  logType: 'nginx_access',
  mode: 'quick_unsupervised',
}

function normalizePreprocessSteps(steps: PreprocessFormValues['steps']) {
  return (steps ?? []).map((step) => {
    const rawColumns = step.input_selector?.columns
    const normalizedColumns = Array.isArray(rawColumns) ? rawColumns : rawColumns ? [rawColumns as unknown as string] : []
    const normalizedParams = { ...step.params } as Record<string, unknown>

    if (step.step_type === 'rename_columns' && typeof step.params.rename_map === 'string' && step.params.rename_map.trim()) {
      try {
        normalizedParams.rename_map = JSON.parse(step.params.rename_map)
      } catch {
        throw new Error('字段重命名映射必须是合法 JSON，例如 {"message":"raw_message"}')
      }
    }

    return {
      ...step,
      input_selector: {
        ...step.input_selector,
        columns: normalizedColumns,
      },
      params: normalizedParams,
    }
  })
}

function normalizeFeatureSteps(steps: FeatureFormValues['steps']) {
  return (steps ?? []).map((step) => {
    const normalizedParams: Record<string, unknown> = {}
    const rawColumns = step.input_selector?.columns ?? []
    const selectorMode = step.input_selector?.mode ?? 'explicit'
    const stepType = step.step_type
    const appendOnlyStepTypes = ['ratio_feature', 'difference_feature', 'concat_fields', 'equality_flag', 'group_frequency', 'group_unique_count', 'group_duration', 'group_event_order', 'time_since_previous_event', 'time_until_next_event', 'group_value_change_flag', 'time_window_count', 'window_unique_count', 'window_target_unique_count', 'window_status_change_count', 'window_spike_flag']
    const isAppendOnlyStep = appendOnlyStepTypes.includes(stepType)

    if (step.params.prefix) {
      normalizedParams.prefix = step.params.prefix
    }
    if (step.params.keywordsText?.trim()) {
      normalizedParams.keywords = step.params.keywordsText.split(',').map((keyword) => keyword.trim()).filter(Boolean)
    }
    if (step.params.bins) {
      normalizedParams.bins = Number(step.params.bins)
    }
    if (step.params.method) {
      normalizedParams.method = step.params.method
    }
    if (step.params.separator?.trim()) {
      normalizedParams.separator = step.params.separator.trim()
    }
    if (step.params.targetColumn?.trim()) {
      normalizedParams.target_column = step.params.targetColumn.trim()
    }
    if (step.params.timeColumn?.trim()) {
      normalizedParams.time_column = step.params.timeColumn.trim()
    }
    if (step.params.windowMinutes) {
      normalizedParams.window_minutes = Number(step.params.windowMinutes)
    }
    if (step.params.threshold) {
      normalizedParams.threshold = Number(step.params.threshold)
    }
    if (step.params.mappingText?.trim()) {
      try {
        normalizedParams.mapping = JSON.parse(step.params.mappingText)
      } catch {
        throw new Error('值映射 JSON 必须合法，例如 {"tcp":1,"udp":2}')
      }
    }
    if (step.params.defaultValue !== undefined && step.params.defaultValue !== '') {
      normalizedParams.default_value = step.params.defaultValue
    }
    if (step.params.operator) {
      normalizedParams.operator = step.params.operator
    }
    if (step.params.value !== undefined && step.params.value !== '') {
      normalizedParams.value = step.params.value
    }
    if (step.params.regexPattern?.trim()) {
      normalizedParams.pattern = step.params.regexPattern.trim()
    }
    if (step.params.patternFlags?.length) {
      normalizedParams.patterns = step.params.patternFlags
    }

    return {
      ...step,
      input_selector: {
        mode: selectorMode,
        columns: selectorMode === 'explicit' ? rawColumns : [],
        dtype: selectorMode === 'dtype' ? step.input_selector?.dtype : undefined,
        role_tag: selectorMode === 'role_tag' ? step.input_selector?.role_tag : undefined,
        name_pattern: selectorMode === 'name_pattern' ? step.input_selector?.name_pattern : undefined,
      },
      params: normalizedParams,
      output_mode: isAppendOnlyStep
        ? {
          ...step.output_mode,
          mode: 'append_new_columns',
        }
        : step.output_mode,
    }
  })
}

function buildFeatureFieldHints(steps: FeatureFormValues['steps']) {
  const required = new Set<string>()
  for (const step of steps ?? []) {
    for (const column of step.input_selector?.columns ?? []) {
      if (column) required.add(column)
    }
    if (step.params.targetColumn?.trim()) {
      required.add(step.params.targetColumn.trim())
    }
    if (step.params.timeColumn?.trim()) {
      required.add(step.params.timeColumn.trim())
    }
  }
  return {
    required_columns: Array.from(required),
    optional_columns: [],
  }
}

function inferTemplateLogType(parserProfile: string | undefined) {
  if (parserProfile === 'nginx_access') return 'nginx_access'
  if (parserProfile === 'generic_log') return 'program_runtime'
  return 'generic_log'
}

export function ProjectWorkspacePage() {
  const { projectId } = useParams()
  const [searchParams, setSearchParams] = useSearchParams()
  const [messageApi, contextHolder] = message.useMessage()
  const navigate = useNavigate()
  const resolvedProjectId = Number(projectId)
  const activeTab = (searchParams.get('tab') as WorkspaceTabKey | null) ?? 'data'

  const [backendStatus, setBackendStatus] = useState<'unknown' | 'online' | 'offline'>('unknown')
  const [loading, setLoading] = useState(true)
  const [errorMessage, setErrorMessage] = useState<string | null>(null)
  const [analysisGoal, setAnalysisGoal] = useState<AnalysisGoalState>(defaultAnalysisGoal)
  const [readiness, setReadiness] = useState<DatasetReadiness | null>(null)
  const [readinessLoading, setReadinessLoading] = useState(false)
  const [readinessError, setReadinessError] = useState<string | null>(null)
  const [workspaceMode, setWorkspaceMode] = useState<'guided' | 'expert'>('guided')
  const [importSession, setImportSession] = useState<ImportSession | null>(null)
  const [pipelinePreview, setPipelinePreview] = useState<PreprocessPreviewRead | null>(null)
  const [preprocessStepPreview, setPreprocessStepPreview] = useState<PreprocessStepPreviewRead | null>(null)
  const [preprocessAdvisor, setPreprocessAdvisor] = useState<PreprocessTrainingAdvisorRead | null>(null)
  const [sampledAdvisorRun, setSampledAdvisorRun] = useState<PreprocessTrainingAdvisorRunRead | null>(null)
  const [activeSampledAdvisorRunId, setActiveSampledAdvisorRunId] = useState<number | null>(null)
  const [featurePreview, setFeaturePreview] = useState<FeaturePreviewRead | null>(null)
  const [featureTemplates, setFeatureTemplates] = useState<FeatureTemplate[]>([])
  const [featureHandoff, setFeatureHandoff] = useState<FeatureHandoff | null>(null)
  const [featureStepPreview, setFeatureStepPreview] = useState<FeatureStepPreviewRead | null>(null)
  const [modelPreview, setModelPreview] = useState<ModelPreviewRead | null>(null)
  const [modelAnalysis, setModelAnalysis] = useState<ModelAnalysisRead | null>(null)
  const [llmConfig, setLlmConfig] = useState<LlmProviderConfig | null>(null)
  const [llmExplanation, setLlmExplanation] = useState<ModelLlmExplanationRead | null>(null)
  const [fileList, setFileList] = useState<UploadFile[]>([])
  const [pipelinePreviewLoading, setPipelinePreviewLoading] = useState(false)
  const [preprocessStepPreviewLoading, setPreprocessStepPreviewLoading] = useState(false)
  const [preprocessAdvisorLoading, setPreprocessAdvisorLoading] = useState(false)
  const [sampledAdvisorLoading, setSampledAdvisorLoading] = useState(false)
  const [featurePreviewLoading, setFeaturePreviewLoading] = useState(false)
  const [featureTemplatesLoading, setFeatureTemplatesLoading] = useState(false)
  const [featureStepPreviewLoading, setFeatureStepPreviewLoading] = useState(false)
  const [modelPreviewLoading, setModelPreviewLoading] = useState(false)
  const [modelAnalysisLoading, setModelAnalysisLoading] = useState(false)
  const [llmConfigLoading, setLlmConfigLoading] = useState(false)
  const [savingLlmConfig, setSavingLlmConfig] = useState(false)
  const [testingLlmConfig, setTestingLlmConfig] = useState(false)
  const [explainingWithLlm, setExplainingWithLlm] = useState(false)
  const [creatingImportSession, setCreatingImportSession] = useState(false)
  const [applyingImportCleaning, setApplyingImportCleaning] = useState(false)
  const [confirmingImportSession, setConfirmingImportSession] = useState(false)
  const [savingMapping, setSavingMapping] = useState(false)
  const [deletingProject, setDeletingProject] = useState(false)
  const [deletingDatasetId, setDeletingDatasetId] = useState<number | null>(null)
  const [runningPreprocess, setRunningPreprocess] = useState(false)
  const [runningFeaturePipeline, setRunningFeaturePipeline] = useState(false)
  const [savingFeatureTemplate, setSavingFeatureTemplate] = useState(false)
  const [runningTraining, setRunningTraining] = useState(false)
  const [startingJob, setStartingJob] = useState(false)

  const {
    project,
    datasets,
    selectedDatasetId,
    selectedDataset,
    datasetPreview,
    fieldMapping,
    pipelines,
    featurePipelines,
    models,
    selectedPipelineId,
    selectedFeaturePipelineId,
    selectedModelId,
    selectedPipeline,
    selectedFeaturePipeline,
    selectedModel,
    datasetColumns,
    datasetsLoading,
    workspaceLoading,
    loadProject,
    loadDatasets,
    loadDatasetWorkspace,
    resetWorkspaceDataState,
    clearSelectedDatasetWorkspace,
    setSelectedDatasetId,
    setFieldMapping,
    setSelectedPipelineId,
    setSelectedFeaturePipelineId,
    setSelectedModelId,
  } = useWorkspaceData({ setBackendStatus, setErrorMessage })

  const loadReadiness = useCallback(async (datasetId: number, goalState: AnalysisGoalState) => {
    setReadinessLoading(true)
    setReadinessError(null)
    try {
      const data = await getDatasetReadiness(datasetId, goalState)
      setReadiness(data)
    } catch {
      setReadiness(null)
      setReadinessError('数据就绪度评估失败，请先确认数据文件和字段解析结果。')
    } finally {
      setReadinessLoading(false)
    }
  }, [])

  const loadPipelinePreview = useCallback(async (pipelineId: number) => {
    setPipelinePreviewLoading(true)
    try {
      const response = await api.get<PreprocessPreviewRead>(`/pipelines/preprocess/${pipelineId}/preview`, { params: { limit: 10 } })
      setPipelinePreview(response.data)
    } finally {
      setPipelinePreviewLoading(false)
    }
  }, [])

  const loadPreprocessAdvisorRun = useCallback(async (advisorRunId: number) => {
    const response = await api.get<PreprocessTrainingAdvisorRunRead>(`/pipelines/preprocess/training-advisor/runs/${advisorRunId}`)
    setSampledAdvisorRun(response.data)
    if (response.data.result) {
      setPreprocessAdvisor(response.data.result)
    }
    return response.data
  }, [])

  const loadFeaturePreview = useCallback(async (pipelineId: number) => {
    setFeaturePreviewLoading(true)
    try {
      const response = await api.get<FeaturePreviewRead>(`/pipelines/features/${pipelineId}/preview`, { params: { limit: 10 } })
      setFeaturePreview(response.data)
    } finally {
      setFeaturePreviewLoading(false)
    }
  }, [])

  const loadFeatureTemplates = useCallback(async (projectIdValue: number) => {
    setFeatureTemplatesLoading(true)
    try {
      const response = await api.get<FeatureTemplate[]>('/features/templates', { params: { project_id: projectIdValue } })
      setFeatureTemplates(response.data)
    } finally {
      setFeatureTemplatesLoading(false)
    }
  }, [])

  const loadModelPreview = useCallback(async (modelId: number) => {
    setModelPreviewLoading(true)
    try {
      const response = await api.get<ModelPreviewRead>(`/training/models/${modelId}/preview`, { params: { limit: 10 } })
      setModelPreview(response.data)
    } finally {
      setModelPreviewLoading(false)
    }
  }, [])

  const loadModelAnalysis = useCallback(async (modelId: number) => {
    setModelAnalysisLoading(true)
    setModelAnalysis(null)
    try {
      const response = await api.get<ModelAnalysisRead>(`/training/models/${modelId}/analysis`, { params: { point_limit: 600, histogram_bins: 16 } })
      setModelAnalysis(response.data)
    } catch {
      setModelAnalysis(null)
    } finally {
      setModelAnalysisLoading(false)
    }
  }, [])

  const loadLlmConfig = useCallback(async (projectIdValue: number) => {
    setLlmConfigLoading(true)
    try {
      const response = await api.get<LlmProviderConfig>(`/analysis/projects/${projectIdValue}/llm-config`)
      setLlmConfig(response.data)
    } finally {
      setLlmConfigLoading(false)
    }
  }, [])

  const handleTabChange = useCallback((nextTab: string) => {
    if (!(nextTab in stageLabels)) return
    setSearchParams((current) => {
      const next = new URLSearchParams(current)
      next.set('tab', nextTab)
      return next
    })
  }, [setSearchParams])

  const {
    latestJob,
    loadJobs,
    addPendingWorkspaceJob,
    clearWorkspaceJobs,
  } = useWorkspaceJobs({
    backendStatus,
    selectedDatasetId,
    activeSampledAdvisorRunId,
    sampledAdvisorLoading,
    messageApi,
    loadDatasetWorkspace,
    loadPreprocessAdvisorRun,
    setSelectedPipelineId,
    setSelectedFeaturePipelineId,
    setSelectedModelId,
    setSampledAdvisorLoading,
    setActiveSampledAdvisorRunId,
    setErrorMessage,
    handleTabChange,
  })

  const resetWorkspaceState = useCallback(() => {
    resetWorkspaceDataState()
    setImportSession(null)
    setPipelinePreview(null)
    setPreprocessStepPreview(null)
    setPreprocessAdvisor(null)
    setSampledAdvisorRun(null)
    setActiveSampledAdvisorRunId(null)
    setPreprocessAdvisorLoading(false)
    setSampledAdvisorLoading(false)
    setFeaturePreview(null)
    setFeatureTemplates([])
    setFeatureHandoff(null)
    setFeatureStepPreview(null)
    setModelPreview(null)
    setModelAnalysis(null)
    setLlmConfig(null)
    setLlmExplanation(null)
    setReadiness(null)
    setReadinessError(null)
    setWorkspaceMode('guided')
    setFileList([])
    clearWorkspaceJobs()
  }, [clearWorkspaceJobs, resetWorkspaceDataState])

  const loadWorkspaceShell = useCallback(async (projectIdValue: number) => {
    try {
      await Promise.all([loadProject(projectIdValue), loadJobs(), loadDatasets(projectIdValue)])
      setBackendStatus('online')
      setErrorMessage(null)
    } catch {
      setBackendStatus('offline')
      setErrorMessage('无法连接后端，请先启动 FastAPI 服务。')
    } finally {
      setLoading(false)
    }
  }, [loadDatasets, loadJobs, loadProject])

  useEffect(() => {
    if (!Number.isFinite(resolvedProjectId)) {
      navigate('/')
      return
    }
    resetWorkspaceState()
    setLoading(true)
    void loadWorkspaceShell(resolvedProjectId)
  }, [loadWorkspaceShell, navigate, projectId, resetWorkspaceState, resolvedProjectId])

  useEffect(() => {
    if (backendStatus === 'online' && selectedDatasetId) void loadDatasetWorkspace(selectedDatasetId)
  }, [backendStatus, loadDatasetWorkspace, selectedDatasetId])

  useEffect(() => {
    if (backendStatus !== 'online' || !selectedDatasetId) {
      setReadiness(null)
      setReadinessError(null)
      return
    }
    void loadReadiness(selectedDatasetId, analysisGoal)
  }, [analysisGoal, backendStatus, loadReadiness, selectedDatasetId])

  useEffect(() => {
    if (backendStatus === 'online' && activeTab === 'preprocess' && selectedPipelineId && selectedPipeline?.status === 'completed' && selectedPipeline.output_path) {
      void loadPipelinePreview(selectedPipelineId)
      return
    }
    setPipelinePreview(null)
  }, [backendStatus, activeTab, loadPipelinePreview, selectedPipelineId, selectedPipeline])

  useEffect(() => {
    setPreprocessStepPreview(null)
    setPreprocessAdvisor(null)
    setSampledAdvisorRun(null)
    setActiveSampledAdvisorRunId(null)
    setPreprocessAdvisorLoading(false)
    setSampledAdvisorLoading(false)
  }, [selectedDatasetId])

  useEffect(() => {
    if (backendStatus === 'online' && activeTab === 'feature' && selectedFeaturePipelineId && selectedFeaturePipeline?.status === 'completed' && selectedFeaturePipeline.output_path) {
      void loadFeaturePreview(selectedFeaturePipelineId)
      return
    }
    setFeaturePreview(null)
  }, [backendStatus, activeTab, loadFeaturePreview, selectedFeaturePipelineId, selectedFeaturePipeline])

  useEffect(() => {
    if (backendStatus === 'online' && activeTab === 'feature' && project) void loadFeatureTemplates(project.id)
  }, [backendStatus, activeTab, loadFeatureTemplates, project])

  useEffect(() => {
    if (
      backendStatus === 'online'
      && selectedModelId
      && (activeTab === 'training' || activeTab === 'analysis')
      && selectedModel?.status === 'completed'
      && selectedModel.prediction_path
    ) {
      void loadModelPreview(selectedModelId)
      return
    }
    setModelPreview(null)
  }, [backendStatus, activeTab, loadModelPreview, selectedModelId, selectedModel])

  useEffect(() => {
    if (
      !selectedModel
      || selectedModel.status !== 'completed'
      || !selectedModel.prediction_path
      || backendStatus !== 'online'
      || (activeTab !== 'training' && activeTab !== 'analysis')
    ) {
      setModelAnalysis(null)
      return
    }
    if (selectedModel.mode !== 'unsupervised') {
      setModelAnalysis(null)
      return
    }
    void loadModelAnalysis(selectedModel.id)
  }, [backendStatus, activeTab, loadModelAnalysis, selectedModel])

  useEffect(() => {
    setLlmExplanation(null)
  }, [selectedModelId])

  useEffect(() => {
    setFeatureStepPreview(null)
  }, [selectedDatasetId])

  useEffect(() => {
    if (backendStatus === 'online' && project && activeTab === 'analysis') {
      void loadLlmConfig(project.id)
    }
  }, [backendStatus, project, activeTab, loadLlmConfig])

  const handleFeatureHandoff = useCallback((handoff: FeatureHandoff) => {
    setFeatureHandoff(handoff)
    handleTabChange('feature')
    const label = handoff.task_category === 'text_complexity'
      ? '文本特征'
      : handoff.task_category === 'high_cardinality'
        ? '高基数特征'
        : '行为追踪特征'
    const sourceLabel = handoff.source_issue_group_title ? `来自「${handoff.source_issue_group_title}」的` : ''
    messageApi.success(`已带入${sourceLabel}${label}推荐方案，可在特征页确认后生成草稿。`)
  }, [handleTabChange, messageApi])

  async function handleCreateImportSession() {
    if (!project) return
    const currentFile = fileList[0]?.originFileObj
    if (!currentFile) {
      messageApi.warning('请先选择待导入的日志文件。')
      return
    }
    const formData = new FormData()
    formData.append('project_id', String(project.id))
    formData.append('file', currentFile)
    setCreatingImportSession(true)
    try {
      const response = await api.post<ImportSession>(
        '/import-sessions',
        formData,
        {
          headers: { 'Content-Type': 'multipart/form-data' },
          timeout: IMPORT_REQUEST_TIMEOUT_MS,
        },
      )
      setFileList([])
      setImportSession(response.data)
      messageApi.success('导入会话已创建，请确认预览后生成数据版本。')
    } catch (error) {
      setErrorMessage(extractApiErrorMessage(error, '创建导入会话失败，请确认格式、编码或文件大小是否合理。'))
    } finally {
      setCreatingImportSession(false)
    }
  }

  async function handleSelectImportTemplate(templateId: string) {
    if (!importSession) return
    try {
      const response = await api.put<ImportSession>(
        `/import-sessions/${importSession.id}/template`,
        { template_id: templateId },
        { timeout: IMPORT_REQUEST_TIMEOUT_MS },
      )
      setImportSession(response.data)
      messageApi.success('解析模板已更新。')
    } catch (error) {
      setErrorMessage(extractApiErrorMessage(error, '更新解析模板失败，请检查后端日志。'))
    }
  }

  async function handleApplyImportCleaning(options: { include_columns?: string[]; exclude_columns?: string[]; rename_columns?: Record<string, string> }) {
    if (!importSession) return
    setApplyingImportCleaning(true)
    try {
      const response = await api.put<ImportSession>(
        `/import-sessions/${importSession.id}/cleaning-options`,
        { cleaning_options: options },
        { timeout: IMPORT_REQUEST_TIMEOUT_MS },
      )
      setImportSession(response.data)
      messageApi.success('导入清洗已应用，预览已刷新。')
    } catch (error) {
      setErrorMessage(extractApiErrorMessage(error, '应用导入清洗失败，请检查字段配置。'))
    } finally {
      setApplyingImportCleaning(false)
    }
  }

  async function handleConfirmImportSession() {
    if (!project || !importSession) return
    setConfirmingImportSession(true)
    try {
      const response = await api.post<ImportSessionConfirmRead>(
        `/import-sessions/${importSession.id}/confirm`,
        undefined,
        { timeout: IMPORT_REQUEST_TIMEOUT_MS },
      )
      setImportSession(null)
      await loadDatasets(project.id, response.data.import_result.dataset_version.id)
      messageApi.success(`导入成功，已生成 ${response.data.import_result.dataset_version.version_name}。`)
    } catch (error) {
      setErrorMessage(extractApiErrorMessage(error, '确认导入失败，请检查模板、字段或后端日志。'))
    } finally {
      setConfirmingImportSession(false)
    }
  }

  async function handleSaveFieldMapping(values: Record<string, string | undefined>) {
    if (!selectedDatasetId) return
    setSavingMapping(true)
    try {
      const payload = { mappings: { event_time: values.event_time || null, source_ip: values.source_ip || null, dest_ip: values.dest_ip || null, status_code: values.status_code || null, label: values.label || null, raw_message: values.raw_message || null } }
      const response = await api.put<FieldMapping>(`/datasets/${selectedDatasetId}/field-mapping`, payload)
      setFieldMapping(response.data)
      messageApi.success('字段映射已保存。')
    } catch {
      setErrorMessage('保存字段映射失败，请检查字段选择是否有效。')
    } finally {
      setSavingMapping(false)
    }
  }

  async function handleDeleteProject() {
    if (!project) return
    setDeletingProject(true)
    try {
      await api.delete(`/projects/${project.id}`)
      messageApi.success('项目已删除。')
      navigate('/')
    } catch {
      setErrorMessage('删除项目失败，请检查后端日志。')
    } finally {
      setDeletingProject(false)
    }
  }

  async function handleDeleteDataset(datasetId: number) {
    if (!project) return
    setDeletingDatasetId(datasetId)
    try {
      await api.delete(`/datasets/${datasetId}`)
      if (selectedDatasetId === datasetId) {
        setFeatureHandoff(null)
        clearSelectedDatasetWorkspace()
        setPipelinePreview(null)
        setFeaturePreview(null)
        setModelPreview(null)
      }
      await loadDatasets(project.id)
      handleTabChange('data')
      messageApi.success('数据集已删除。')
    } catch {
      setErrorMessage('删除数据集失败，请检查后端日志。')
    } finally {
      setDeletingDatasetId(null)
    }
  }

  async function handleRunPreprocess(values: PreprocessFormValues) {
    if (!project || !selectedDatasetId) return
    setRunningPreprocess(true)
    try {
      const steps = normalizePreprocessSteps(values.steps ?? [])

      const response = await api.post<JobSubmissionRead>('/pipelines/preprocess', { project_id: project.id, dataset_version_id: selectedDatasetId, name: values.name, steps })
      await Promise.all([loadDatasetWorkspace(selectedDatasetId), loadJobs()])
      setSelectedPipelineId(response.data.resource_id)
      addPendingWorkspaceJob({ jobId: response.data.job.id, kind: 'preprocess', resourceId: response.data.resource_id, tab: 'preprocess' })
      handleTabChange('preprocess')
      messageApi.success('预处理任务已提交，完成后会自动刷新结果。')
    } catch (error) {
      setErrorMessage(extractApiErrorMessage(error, '执行预处理失败，请检查步骤参数。'))
    } finally {
      setRunningPreprocess(false)
    }
  }

  async function handlePreviewPreprocessStep(previewStepIndex: number, values: PreprocessFormValues) {
    if (!project || !selectedDatasetId) return
    setPreprocessStepPreviewLoading(true)
    try {
      const steps = normalizePreprocessSteps(values.steps ?? [])

      const response = await api.post<PreprocessStepPreviewRead>('/pipelines/preprocess/step-preview', {
        project_id: project.id,
        dataset_version_id: selectedDatasetId,
        steps,
        preview_step_index: previewStepIndex,
        limit: 6,
      })
      setPreprocessStepPreview(response.data)
      messageApi.success(`已生成第 ${previewStepIndex + 1} 步预览。`)
    } catch (error) {
      setErrorMessage(extractApiErrorMessage(error, '生成步骤预览失败，请检查步骤参数。'))
    } finally {
      setPreprocessStepPreviewLoading(false)
    }
  }

  const handleAnalyzePreprocessAdvisor = useCallback(async (values: PreprocessFormValues) => {
    if (!project || !selectedDatasetId) return
    setPreprocessAdvisorLoading(true)
    try {
      const response = await api.post<PreprocessTrainingAdvisorRead>('/pipelines/preprocess/training-advisor', {
        project_id: project.id,
        dataset_version_id: selectedDatasetId,
        steps: normalizePreprocessSteps(values.steps ?? []),
        target_column: selectedDataset?.label_column ?? null,
      })
      setPreprocessAdvisor(response.data)
      setErrorMessage(null)
    } catch (error) {
      setErrorMessage(extractApiErrorMessage(error, '生成训练影响建议失败，请检查当前步骤链。'))
    } finally {
      setPreprocessAdvisorLoading(false)
    }
  }, [project, selectedDatasetId, selectedDataset?.label_column])

  const handleRunSampledPreprocessAdvisor = useCallback(async (values: PreprocessFormValues) => {
    if (!project || !selectedDatasetId) return
    setSampledAdvisorLoading(true)
    setSampledAdvisorRun(null)
    try {
      const response = await api.post<JobSubmissionRead>('/pipelines/preprocess/training-advisor/sample', {
        project_id: project.id,
        dataset_version_id: selectedDatasetId,
        steps: normalizePreprocessSteps(values.steps ?? []),
        target_column: selectedDataset?.label_column ?? null,
        sample_limit: 2000,
      })
      setActiveSampledAdvisorRunId(response.data.resource_id)
      addPendingWorkspaceJob({
        jobId: response.data.job.id,
        kind: 'advisor',
        resourceId: response.data.resource_id,
        tab: 'preprocess',
      })
      handleTabChange('preprocess')
      messageApi.success('采样训练适配分析已提交，完成后会自动刷新结果。')
    } catch (error) {
      setSampledAdvisorLoading(false)
      setActiveSampledAdvisorRunId(null)
      setErrorMessage(extractApiErrorMessage(error, '提交采样训练适配分析失败，请检查当前步骤链。'))
    }
  }, [handleTabChange, messageApi, project, selectedDataset?.label_column, selectedDatasetId])

  async function handleRunFeaturePipeline(values: FeatureFormValues) {
    if (!project || !selectedDatasetId) return
    setRunningFeaturePipeline(true)
    try {
      const response = await api.post<JobSubmissionRead>('/pipelines/features', {
        project_id: project.id,
        dataset_version_id: selectedDatasetId,
        preprocess_pipeline_id: values.preprocessPipelineId ?? null,
        name: values.name,
        mode: values.mode,
        template_id: values.templateId ?? null,
        steps: normalizeFeatureSteps(values.steps ?? []),
      })
      await loadDatasetWorkspace(selectedDatasetId)
      setSelectedFeaturePipelineId(response.data.resource_id)
      setFeatureHandoff(null)
      addPendingWorkspaceJob({ jobId: response.data.job.id, kind: 'feature', resourceId: response.data.resource_id, tab: 'feature' })
      handleTabChange('feature')
      messageApi.success('特征工程任务已提交，完成后会自动刷新结果。')
    } catch (error) {
      setErrorMessage(extractApiErrorMessage(error, '执行特征工程失败，请检查步骤参数。'))
    } finally {
      setRunningFeaturePipeline(false)
    }
  }

  async function handlePreviewFeatureStep(previewStepIndex: number, values: FeatureFormValues) {
    if (!project || !selectedDatasetId) return
    setFeatureStepPreviewLoading(true)
    try {
      const response = await api.post<FeatureStepPreviewRead>('/pipelines/features/step-preview', {
        project_id: project.id,
        dataset_version_id: selectedDatasetId,
        preprocess_pipeline_id: values.preprocessPipelineId ?? null,
        steps: normalizeFeatureSteps(values.steps ?? []),
        preview_step_index: previewStepIndex,
        limit: 6,
      })
      setFeatureStepPreview(response.data)
      messageApi.success(`已生成第 ${previewStepIndex + 1} 步特征预览。`)
    } catch (error) {
      setErrorMessage(extractApiErrorMessage(error, '生成特征步骤预览失败，请检查步骤参数。'))
    } finally {
      setFeatureStepPreviewLoading(false)
    }
  }

  async function handleSaveFeatureTemplate(values: FeatureFormValues) {
    if (!project) return
    setSavingFeatureTemplate(true)
    try {
      await api.post<FeatureTemplate>('/features/templates', {
        project_id: project.id,
        name: values.templateSaveName || `${values.name}-template`,
        log_type: values.templateSaveLogType || inferTemplateLogType(selectedDataset?.parser_profile),
        description: values.templateSaveDescription || '',
        steps: normalizeFeatureSteps(values.steps ?? []),
        field_hints: buildFeatureFieldHints(values.steps ?? []),
      })
      await loadFeatureTemplates(project.id)
      messageApi.success('项目内特征模板已保存。')
    } catch (error) {
      setErrorMessage(extractApiErrorMessage(error, '保存特征模板失败，请检查模板名称或步骤配置。'))
    } finally {
      setSavingFeatureTemplate(false)
    }
  }

  async function handleRunTraining(values: TrainingFormValues) {
    if (!project || !selectedDatasetId) return
    setRunningTraining(true)
    try {
      const advisorSuggestedColumns =
        sampledAdvisorRun?.result?.summary.suggested_training_columns
        ?? preprocessAdvisor?.summary.suggested_training_columns
        ?? []
      const selectedTrainingFeaturePipeline =
        values.featurePipelineId
          ? featurePipelines.find((pipeline) => pipeline.id === values.featurePipelineId) ?? null
          : null
      const requestedFeatureColumns = values.featureColumns?.length
        ? values.featureColumns
        : selectedTrainingFeaturePipeline?.training_candidate_columns?.length
          ? selectedTrainingFeaturePipeline.training_candidate_columns
          : !values.featurePipelineId && advisorSuggestedColumns.length
          ? advisorSuggestedColumns
          : []

      const response = await api.post<JobSubmissionRead>('/training/models', {
        project_id: project.id,
        dataset_version_id: selectedDatasetId,
        preprocess_pipeline_id: values.preprocessPipelineId ?? null,
        feature_pipeline_id: values.featurePipelineId ?? null,
        name: values.name,
        mode: values.mode,
        algorithm: values.algorithm,
        target_column: values.targetColumn ?? null,
        feature_columns: requestedFeatureColumns,
        training_params: {},
      })
      await Promise.all([loadDatasetWorkspace(selectedDatasetId), loadJobs()])
      setSelectedModelId(response.data.resource_id)
      addPendingWorkspaceJob({ jobId: response.data.job.id, kind: 'training', resourceId: response.data.resource_id, tab: 'training' })
      handleTabChange('training')
      messageApi.success(
        values.featurePipelineId && !values.featureColumns?.length
          ? '训练任务已提交，默认会复用特征页推荐训练字段。'
          : requestedFeatureColumns.length && !values.featureColumns?.length && !values.featurePipelineId
            ? '训练任务已提交，已优先复用预处理阶段的训练影响建议。'
            : '训练任务已提交，完成后会自动刷新结果。',
      )
    } catch {
      setErrorMessage('执行训练失败，请检查标签列、训练字段或算法选择。')
    } finally {
      setRunningTraining(false)
    }
  }

  async function handleSaveLlmConfig(values: LlmProviderConfigPayload) {
    if (!project) return
    setSavingLlmConfig(true)
    try {
      const response = await api.put<LlmProviderConfig>(`/analysis/projects/${project.id}/llm-config`, values)
      setLlmConfig(response.data)
      messageApi.success('大模型配置已保存。')
    } catch (error) {
      setErrorMessage(extractApiErrorMessage(error, '保存大模型配置失败，请检查接口地址、模型名或 Key。'))
    } finally {
      setSavingLlmConfig(false)
    }
  }

  async function handleRunLlmExplanation(topK: number) {
    if (!selectedModelId) return
    setExplainingWithLlm(true)
    setLlmExplanation(null)
    try {
      const response = await api.post<ModelLlmExplanationRead>(
        `/analysis/models/${selectedModelId}/llm-explanation`,
        { top_k: topK },
        { timeout: LLM_REQUEST_TIMEOUT_MS },
      )
      setLlmExplanation(response.data)
      messageApi.success('AI 异常分析已生成。')
    } catch (error: unknown) {
      setErrorMessage(extractApiErrorMessage(error, '生成 AI 异常分析失败，请检查模型配置或后端日志。'))
    } finally {
      setExplainingWithLlm(false)
    }
  }

  async function handleTestLlmConfig(values: LlmProviderConfigPayload) {
    if (!project) return
    setTestingLlmConfig(true)
    try {
      const response = await api.post(
        `/analysis/projects/${project.id}/llm-config/test`,
        values,
        { timeout: LLM_REQUEST_TIMEOUT_MS },
      )
      const detail = response.data?.detail ?? '连接测试成功。'
      messageApi.success(detail)
    } catch (error: unknown) {
      setErrorMessage(extractApiErrorMessage(error, '连接测试失败，请检查接口地址、模型名或 API Key。'))
    } finally {
      setTestingLlmConfig(false)
    }
  }

  async function handleStartDemoTraining() {
    setStartingJob(true)
    try {
      await api.post('/jobs/demo', { name: `demo-training-${new Date().toLocaleTimeString('zh-CN', { hour12: false })}`, duration_seconds: 10 })
      await loadJobs()
      messageApi.success('演示训练任务已启动。')
    } catch {
      setErrorMessage('演示训练任务启动失败，请检查后端日志。')
    } finally {
      setStartingJob(false)
    }
  }

  function handleQuickAnalysisEntry() {
    setWorkspaceMode('guided')
    if (!selectedDatasetId) {
      messageApi.info('请先导入或选择一个可分析数据版本。')
      handleTabChange('data')
      return
    }
    if (readiness && readiness.score < 40) {
      messageApi.warning('当前数据就绪度偏低，建议先做字段映射或数据整理。')
      handleTabChange('preprocess')
      return
    }
    messageApi.success('已进入推荐路径：先确认特征，再建立检测模型。')
    handleTabChange(readiness?.recommended_templates.length ? 'feature' : 'preprocess')
  }

  function handleExpertModeEntry() {
    setWorkspaceMode('expert')
    messageApi.info('已切换到专家模式，保留完整字段映射、数据整理、特征工程和训练流程。')
    handleTabChange('data')
  }

  if (loading) return <div className="loading-state"><Spin size="large" /></div>

  return (
    <Layout className="app-shell workspace-shell">
      {contextHolder}
      <Content className="workspace-content">
        {errorMessage ? <Alert banner type="warning" message={errorMessage} /> : null}
        <WorkspaceHeader
          project={project}
          datasetLabel={selectedDataset?.version_name}
          latestJob={latestJob}
          activeTab={activeTab}
          onRefresh={() => void loadWorkspaceShell(resolvedProjectId)}
          onDeleteProject={() => void handleDeleteProject()}
          deletingProject={deletingProject}
        />
        <Space direction="vertical" size={16} className="full-width workspace-guided-stack">
          <AnalysisGoalCard value={analysisGoal} onChange={setAnalysisGoal} />
          <DataReadinessCard
            readiness={readiness}
            loading={readinessLoading}
            errorMessage={readinessError}
            onQuickAnalysis={handleQuickAnalysisEntry}
            onExpertMode={handleExpertModeEntry}
          />
          {selectedDatasetId && readiness && workspaceMode === 'guided' ? (
            <QuickAnalysisPanel
              goalState={analysisGoal}
              readiness={readiness}
              onOpenData={() => handleTabChange('data')}
              onOpenFeature={() => handleTabChange('feature')}
              onOpenTraining={() => handleTabChange('training')}
              onOpenAnalysis={() => handleTabChange('analysis')}
            />
          ) : null}
          {workspaceMode === 'expert' ? (
            <Alert
              type="info"
              showIcon
              message="专家模式已开启"
              description="下面仍保留原有字段映射、数据整理、异常分析特征、建立检测模型和异常研判页签。"
            />
          ) : null}
        </Space>
        <Tabs
          className="workspace-tabs"
          activeKey={activeTab}
          onChange={handleTabChange}
          destroyOnHidden
          items={[
            {
              key: 'data',
              label: stageLabels.data,
              children: activeTab === 'data' ? (
                <Suspense fallback={<div className="loading-state"><Spin /></div>}>
                  <DataTab project={project} datasets={datasets} selectedDatasetId={selectedDatasetId} selectedDataset={selectedDataset} datasetPreview={datasetPreview} fileList={fileList} datasetsLoading={datasetsLoading} previewLoading={workspaceLoading} fieldMapping={fieldMapping} importSession={importSession} mappingLoading={workspaceLoading} savingMapping={savingMapping} creatingImportSession={creatingImportSession} applyingImportCleaning={applyingImportCleaning} confirmingImportSession={confirmingImportSession} deletingDatasetId={deletingDatasetId} onSelectDataset={setSelectedDatasetId} onFileListChange={setFileList} onCreateImportSession={() => void handleCreateImportSession()} onConfirmImportSession={() => void handleConfirmImportSession()} onSelectImportTemplate={(templateId) => void handleSelectImportTemplate(templateId)} onApplyImportCleaning={(options) => void handleApplyImportCleaning(options)} onSaveFieldMapping={(values) => void handleSaveFieldMapping(values)} onDeleteDataset={(datasetId) => void handleDeleteDataset(datasetId)} />
                </Suspense>
              ) : null,
            },
            {
              key: 'preprocess',
              label: stageLabels.preprocess,
              children: activeTab === 'preprocess' ? (
                <Suspense fallback={<div className="loading-state"><Spin /></div>}>
                  <PreprocessTab dataset={selectedDataset} columns={datasetColumns} pipelines={pipelines} selectedPipelineId={selectedPipelineId} selectedPipeline={selectedPipeline} preview={pipelinePreview} stepPreview={preprocessStepPreview} stepPreviewLoading={preprocessStepPreviewLoading} listLoading={workspaceLoading} previewLoading={pipelinePreviewLoading} running={runningPreprocess} advisor={preprocessAdvisor} advisorLoading={preprocessAdvisorLoading} sampledAdvisorRun={sampledAdvisorRun} sampledAdvisorLoading={sampledAdvisorLoading} onRun={(values) => void handleRunPreprocess(values)} onPreviewStep={(index, values) => void handlePreviewPreprocessStep(index, values)} onAnalyzeAdvisor={handleAnalyzePreprocessAdvisor} onRunSampledAdvisor={handleRunSampledPreprocessAdvisor} onFeatureHandoff={handleFeatureHandoff} onSelectPipeline={setSelectedPipelineId} />
                </Suspense>
              ) : null,
            },
            {
              key: 'feature',
              label: stageLabels.feature,
              children: activeTab === 'feature' ? (
                <Suspense fallback={<div className="loading-state"><Spin /></div>}>
                  <FeatureTab projectId={project?.id ?? null} dataset={selectedDataset} preprocessPipelines={pipelines} pipelines={featurePipelines} templates={featureTemplates} templatesLoading={featureTemplatesLoading} selectedPipelineId={selectedFeaturePipelineId} selectedPipeline={selectedFeaturePipeline} preview={featurePreview} stepPreview={featureStepPreview} listLoading={workspaceLoading} previewLoading={featurePreviewLoading} stepPreviewLoading={featureStepPreviewLoading} running={runningFeaturePipeline} savingTemplate={savingFeatureTemplate} featureHandoff={featureHandoff} onClearFeatureHandoff={() => setFeatureHandoff(null)} onRun={(values) => void handleRunFeaturePipeline(values)} onPreviewStep={(index, values) => void handlePreviewFeatureStep(index, values)} onSaveTemplate={(values) => void handleSaveFeatureTemplate(values)} onSelectPipeline={setSelectedFeaturePipelineId} />
                </Suspense>
              ) : null,
            },
            {
              key: 'training',
              label: stageLabels.training,
              children: activeTab === 'training' ? (
                <Suspense fallback={<div className="loading-state"><Spin /></div>}>
                  <TrainingTab dataset={selectedDataset} columns={datasetColumns} featurePipelines={featurePipelines} preprocessPipelines={pipelines} models={models} selectedModelId={selectedModelId} selectedModel={selectedModel} preview={modelPreview} analysis={modelAnalysis} listLoading={workspaceLoading} previewLoading={modelPreviewLoading} analysisLoading={modelAnalysisLoading} running={runningTraining} onRun={(values) => void handleRunTraining(values)} onSelectModel={setSelectedModelId} />
                </Suspense>
              ) : null,
            },
            {
              key: 'analysis',
              label: stageLabels.analysis,
              children: activeTab === 'analysis' ? (
                <Suspense fallback={<div className="loading-state"><Spin /></div>}>
                  <AnalysisTab project={project} models={models} selectedModelId={selectedModelId} selectedModel={selectedModel} preview={modelPreview} analysis={modelAnalysis} llmConfig={llmConfig} llmExplanation={llmExplanation} listLoading={workspaceLoading} previewLoading={modelPreviewLoading} analysisLoading={modelAnalysisLoading} llmConfigLoading={llmConfigLoading} savingLlmConfig={savingLlmConfig} testingLlmConfig={testingLlmConfig} explainingWithLlm={explainingWithLlm} onSelectModel={setSelectedModelId} onSaveLlmConfig={(values) => void handleSaveLlmConfig(values)} onTestLlmConfig={(values) => void handleTestLlmConfig(values)} onRunLlmExplanation={(topK) => void handleRunLlmExplanation(topK)} />
                </Suspense>
              ) : null,
            },
          ]}
        />
        <div className="floating-action">
          <Button type="primary" icon={<LineChartOutlined />} loading={startingJob} onClick={() => void handleStartDemoTraining()}>
            启动演示训练
          </Button>
        </div>
      </Content>
    </Layout>
  )
}
