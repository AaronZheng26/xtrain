import { useCallback, useEffect, useMemo, useState } from 'react'
import { Alert, Button, Form, Layout, Spin, Tabs, message } from 'antd'
import type { UploadFile } from 'antd/es/upload/interface'
import { LineChartOutlined } from '@ant-design/icons'
import { useNavigate, useParams, useSearchParams } from 'react-router-dom'

import { AnalysisTab } from '../components/workspace/AnalysisTab'
import { DataTab } from '../components/workspace/DataTab'
import { FeatureTab } from '../components/workspace/FeatureTab'
import { PreprocessTab } from '../components/workspace/PreprocessTab'
import { TrainingTab } from '../components/workspace/TrainingTab'
import { WorkspaceHeader } from '../components/WorkspaceHeader'
import type { FeatureFormValues } from '../components/workspace/FeatureTab'
import type { PreprocessFormValues } from '../components/workspace/PreprocessTab'
import type { TrainingFormValues } from '../components/TrainingPanel'
import { api } from '../lib/api'
import { extractApiErrorMessage } from '../lib/errors'
import type {
  DatasetPreviewRead,
  DatasetVersion,
  DatasetWorkspaceRead,
  FeaturePipeline,
  FeaturePreviewRead,
  FeatureStepPreviewRead,
  FeatureTemplate,
  FieldMapping,
  ImportSession,
  ImportSessionConfirmRead,
  Job,
  JobSubmissionRead,
  LlmProviderConfig,
  LlmProviderConfigPayload,
  ModelAnalysisRead,
  ModelLlmExplanationRead,
  ModelPreviewRead,
  ModelVersion,
  PreprocessPipeline,
  PreprocessPreviewRead,
  PreprocessStepPreviewRead,
  PreprocessTrainingAdvisorRead,
  PreprocessTrainingAdvisorRunRead,
  Project,
  WorkspaceTabKey,
} from '../types'

const { Content } = Layout

const stageLabels: Record<WorkspaceTabKey, string> = {
  data: '数据',
  preprocess: '预处理',
  feature: '特征',
  training: '训练',
  analysis: '分析',
}

const LLM_REQUEST_TIMEOUT_MS = 90000
const IMPORT_REQUEST_TIMEOUT_MS = 120000

type PendingWorkspaceJob = {
  jobId: number
  kind: 'preprocess' | 'feature' | 'training' | 'advisor'
  resourceId: number
  tab: WorkspaceTabKey
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
    const appendOnlyStepTypes = ['ratio_feature', 'difference_feature', 'concat_fields', 'equality_flag', 'group_frequency', 'group_unique_count', 'time_window_count', 'window_unique_count', 'window_spike_flag']
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
  const [mappingForm] = Form.useForm<Record<string, string | undefined>>()
  const [messageApi, contextHolder] = message.useMessage()
  const navigate = useNavigate()
  const resolvedProjectId = Number(projectId)
  const activeTab = (searchParams.get('tab') as WorkspaceTabKey | null) ?? 'data'

  const [project, setProject] = useState<Project | null>(null)
  const [jobs, setJobs] = useState<Job[]>([])
  const [datasets, setDatasets] = useState<DatasetVersion[]>([])
  const [selectedDatasetId, setSelectedDatasetId] = useState<number | null>(null)
  const [selectedDataset, setSelectedDataset] = useState<DatasetVersion | null>(null)
  const [datasetPreview, setDatasetPreview] = useState<DatasetPreviewRead | null>(null)
  const [fieldMapping, setFieldMapping] = useState<FieldMapping | null>(null)
  const [importSession, setImportSession] = useState<ImportSession | null>(null)
  const [pipelines, setPipelines] = useState<PreprocessPipeline[]>([])
  const [pipelinePreview, setPipelinePreview] = useState<PreprocessPreviewRead | null>(null)
  const [preprocessStepPreview, setPreprocessStepPreview] = useState<PreprocessStepPreviewRead | null>(null)
  const [preprocessAdvisor, setPreprocessAdvisor] = useState<PreprocessTrainingAdvisorRead | null>(null)
  const [sampledAdvisorRun, setSampledAdvisorRun] = useState<PreprocessTrainingAdvisorRunRead | null>(null)
  const [featurePipelines, setFeaturePipelines] = useState<FeaturePipeline[]>([])
  const [featurePreview, setFeaturePreview] = useState<FeaturePreviewRead | null>(null)
  const [featureTemplates, setFeatureTemplates] = useState<FeatureTemplate[]>([])
  const [featureStepPreview, setFeatureStepPreview] = useState<FeatureStepPreviewRead | null>(null)
  const [models, setModels] = useState<ModelVersion[]>([])
  const [modelPreview, setModelPreview] = useState<ModelPreviewRead | null>(null)
  const [modelAnalysis, setModelAnalysis] = useState<ModelAnalysisRead | null>(null)
  const [llmConfig, setLlmConfig] = useState<LlmProviderConfig | null>(null)
  const [llmExplanation, setLlmExplanation] = useState<ModelLlmExplanationRead | null>(null)
  const [selectedPipelineId, setSelectedPipelineId] = useState<number | null>(null)
  const [selectedFeaturePipelineId, setSelectedFeaturePipelineId] = useState<number | null>(null)
  const [selectedModelId, setSelectedModelId] = useState<number | null>(null)
  const [fileList, setFileList] = useState<UploadFile[]>([])
  const [backendStatus, setBackendStatus] = useState<'unknown' | 'online' | 'offline'>('unknown')
  const [loading, setLoading] = useState(true)
  const [datasetsLoading, setDatasetsLoading] = useState(false)
  const [workspaceLoading, setWorkspaceLoading] = useState(false)
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
  const [errorMessage, setErrorMessage] = useState<string | null>(null)
  const [pendingWorkspaceJobs, setPendingWorkspaceJobs] = useState<PendingWorkspaceJob[]>([])

  const selectedPipeline = useMemo(() => pipelines.find((pipeline) => pipeline.id === selectedPipelineId) ?? null, [pipelines, selectedPipelineId])
  const selectedFeaturePipeline = useMemo(() => featurePipelines.find((pipeline) => pipeline.id === selectedFeaturePipelineId) ?? null, [featurePipelines, selectedFeaturePipelineId])
  const selectedModel = useMemo(() => models.find((model) => model.id === selectedModelId) ?? null, [models, selectedModelId])
  const latestJob = jobs[0] ?? null
  const hasPendingWorkspaceJobs = pendingWorkspaceJobs.length > 0
  const datasetColumns = selectedDataset?.schema_snapshot.map((field) => field.name) ?? []
  const featureColumns = selectedFeaturePipeline?.output_schema.map((field) => field.name) ?? selectedPipeline?.output_schema.map((field) => field.name) ?? datasetColumns

  const resetWorkspaceState = useCallback(() => {
    setDatasets([])
    setSelectedDatasetId(null)
    setSelectedDataset(null)
    setDatasetPreview(null)
    setFieldMapping(null)
    setImportSession(null)
    setPipelines([])
    setPipelinePreview(null)
    setPreprocessStepPreview(null)
    setPreprocessAdvisor(null)
    setSampledAdvisorRun(null)
    setPreprocessAdvisorLoading(false)
    setSampledAdvisorLoading(false)
    setFeaturePipelines([])
    setFeaturePreview(null)
    setFeatureTemplates([])
    setFeatureStepPreview(null)
    setModels([])
    setModelPreview(null)
    setModelAnalysis(null)
    setLlmConfig(null)
    setLlmExplanation(null)
    setSelectedPipelineId(null)
    setSelectedFeaturePipelineId(null)
    setSelectedModelId(null)
    setFileList([])
    mappingForm.resetFields()
  }, [mappingForm])

  const loadProject = useCallback(async (projectIdValue: number) => {
    const response = await api.get<Project>(`/projects/${projectIdValue}`)
    setProject(response.data)
  }, [])

  const loadJobs = useCallback(async () => {
    const response = await api.get<Job[]>('/jobs')
    setJobs(response.data)
  }, [])

  const loadDatasets = useCallback(async (projectIdValue: number, preferredDatasetId: number | null = null) => {
    setDatasetsLoading(true)
    try {
      const response = await api.get<DatasetVersion[]>('/datasets', { params: { project_id: projectIdValue } })
      setDatasets(response.data)
      setSelectedDatasetId((current) => {
        const desiredDatasetId = preferredDatasetId ?? current
        return response.data.find((dataset) => dataset.id === desiredDatasetId)?.id ?? response.data[0]?.id ?? null
      })
      setErrorMessage(null)
    } catch {
      setBackendStatus('offline')
      setErrorMessage('加载项目数据集失败。')
    } finally {
      setDatasetsLoading(false)
    }
  }, [])

  const loadDatasetWorkspace = useCallback(async (datasetId: number) => {
    setWorkspaceLoading(true)
    try {
      const response = await api.get<DatasetWorkspaceRead>(`/datasets/${datasetId}/workspace`, { params: { preview_limit: 12 } })
      const workspace = response.data
      setSelectedDataset(workspace.dataset)
      setDatasetPreview(workspace.preview)
      setFieldMapping(workspace.field_mapping)
      setPipelines(workspace.preprocess_pipelines)
      setFeaturePipelines(workspace.feature_pipelines)
      setModels(workspace.models)
      mappingForm.setFieldsValue(Object.fromEntries(Object.entries(workspace.field_mapping.mappings).map(([key, value]) => [key, value ?? undefined])))
      setSelectedPipelineId((current) => workspace.preprocess_pipelines.find((item) => item.id === current)?.id ?? workspace.preprocess_pipelines[0]?.id ?? null)
      setSelectedFeaturePipelineId((current) => workspace.feature_pipelines.find((item) => item.id === current)?.id ?? workspace.feature_pipelines[0]?.id ?? null)
      setSelectedModelId((current) => workspace.models.find((item) => item.id === current)?.id ?? workspace.models[0]?.id ?? null)
      setErrorMessage(null)
    } catch {
      setBackendStatus('offline')
      setErrorMessage('加载数据集工作区失败。')
    } finally {
      setWorkspaceLoading(false)
    }
  }, [mappingForm])

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
    setProject(null)
    setLoading(true)
    void loadWorkspaceShell(resolvedProjectId)
  }, [loadWorkspaceShell, navigate, projectId, resetWorkspaceState, resolvedProjectId])

  useEffect(() => {
    if (backendStatus !== 'online') return
    const timer = window.setInterval(() => {
      if (!document.hidden) void loadJobs()
    }, hasPendingWorkspaceJobs ? 3000 : 10000)
    return () => window.clearInterval(timer)
  }, [backendStatus, hasPendingWorkspaceJobs, loadJobs])

  useEffect(() => {
    if (backendStatus === 'online' && selectedDatasetId) void loadDatasetWorkspace(selectedDatasetId)
  }, [backendStatus, loadDatasetWorkspace, selectedDatasetId])

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

  const handleTabChange = useCallback((nextTab: string) => {
    setSearchParams((current) => {
      const next = new URLSearchParams(current)
      next.set('tab', nextTab)
      return next
    })
  }, [setSearchParams])

  useEffect(() => {
    if (!pendingWorkspaceJobs.length || !selectedDatasetId) {
      return
    }

    const datasetId = selectedDatasetId
    const settledJobs = pendingWorkspaceJobs
      .map((pendingJob) => ({
        ...pendingJob,
        job: jobs.find((job) => job.id === pendingJob.jobId) ?? null,
      }))
      .filter((pendingJob) => pendingJob.job && (pendingJob.job.status === 'completed' || pendingJob.job.status === 'failed'))

    if (!settledJobs.length) {
      return
    }

    let cancelled = false

    async function syncWorkspaceAfterJobs() {
      setPendingWorkspaceJobs((current) => current.filter((pendingJob) => !settledJobs.some((settled) => settled.jobId === pendingJob.jobId)))
      const needsWorkspaceRefresh = settledJobs.some((settled) => settled.kind !== 'advisor')
      if (needsWorkspaceRefresh) {
        await Promise.all([loadDatasetWorkspace(datasetId), loadJobs()])
      } else {
        await loadJobs()
      }
      if (cancelled) return

      for (const settled of settledJobs) {
        if (!settled.job) continue
        if (settled.kind === 'preprocess') {
          setSelectedPipelineId(settled.resourceId)
        } else if (settled.kind === 'feature') {
          setSelectedFeaturePipelineId(settled.resourceId)
        } else if (settled.kind === 'training') {
          setSelectedModelId(settled.resourceId)
        } else if (settled.kind === 'advisor') {
          setSampledAdvisorLoading(false)
          if (settled.job.status === 'completed') {
            try {
              await loadPreprocessAdvisorRun(settled.resourceId)
            } catch (error) {
              const failureMessage = extractApiErrorMessage(error, '加载采样训练适配分析结果失败。')
              setErrorMessage(failureMessage)
              messageApi.error(failureMessage)
              continue
            }
          }
        }

        handleTabChange(settled.tab)

        if (settled.job.status === 'completed') {
          messageApi.success(settled.job.message || '后台任务已完成。')
        } else {
          const failureMessage = settled.job.message || '后台任务执行失败，请检查后端日志。'
          setErrorMessage(failureMessage)
          messageApi.error(failureMessage)
        }
      }
    }

    void syncWorkspaceAfterJobs()

    return () => {
      cancelled = true
    }
  }, [handleTabChange, jobs, loadDatasetWorkspace, loadJobs, loadPreprocessAdvisorRun, messageApi, pendingWorkspaceJobs, selectedDatasetId])

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

  async function handleSaveFieldMapping() {
    if (!selectedDatasetId) return
    setSavingMapping(true)
    try {
      const values = mappingForm.getFieldsValue()
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
        setSelectedDatasetId(null)
        setSelectedDataset(null)
        setDatasetPreview(null)
        setFieldMapping(null)
        setPipelines([])
        setFeaturePipelines([])
        setModels([])
        setSelectedPipelineId(null)
        setSelectedFeaturePipelineId(null)
        setSelectedModelId(null)
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
      setPendingWorkspaceJobs((current) => [...current, { jobId: response.data.job.id, kind: 'preprocess', resourceId: response.data.resource_id, tab: 'preprocess' }])
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
    try {
      const response = await api.post<JobSubmissionRead>('/pipelines/preprocess/training-advisor/sample', {
        project_id: project.id,
        dataset_version_id: selectedDatasetId,
        steps: normalizePreprocessSteps(values.steps ?? []),
        target_column: selectedDataset?.label_column ?? null,
        sample_limit: 2000,
      })
      setPendingWorkspaceJobs((current) => [...current, {
        jobId: response.data.job.id,
        kind: 'advisor',
        resourceId: response.data.resource_id,
        tab: 'preprocess',
      }])
      handleTabChange('preprocess')
      messageApi.success('采样训练适配分析已提交，完成后会自动刷新结果。')
    } catch (error) {
      setSampledAdvisorLoading(false)
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
      setPendingWorkspaceJobs((current) => [...current, { jobId: response.data.job.id, kind: 'feature', resourceId: response.data.resource_id, tab: 'feature' }])
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
      const requestedFeatureColumns = values.featureColumns?.length
        ? values.featureColumns
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
      setPendingWorkspaceJobs((current) => [...current, { jobId: response.data.job.id, kind: 'training', resourceId: response.data.resource_id, tab: 'training' }])
      handleTabChange('training')
      messageApi.success(requestedFeatureColumns.length && !values.featureColumns?.length && !values.featurePipelineId
        ? '训练任务已提交，已优先复用预处理阶段的训练影响建议。'
        : '训练任务已提交，完成后会自动刷新结果。')
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
        <Tabs
          className="workspace-tabs"
          activeKey={activeTab}
          onChange={handleTabChange}
          items={[
            { key: 'data', label: stageLabels.data, children: <DataTab project={project} datasets={datasets} selectedDatasetId={selectedDatasetId} selectedDataset={selectedDataset} datasetPreview={datasetPreview} fileList={fileList} datasetsLoading={datasetsLoading} previewLoading={workspaceLoading} fieldMapping={fieldMapping} importSession={importSession} mappingLoading={workspaceLoading} savingMapping={savingMapping} creatingImportSession={creatingImportSession} applyingImportCleaning={applyingImportCleaning} confirmingImportSession={confirmingImportSession} deletingDatasetId={deletingDatasetId} mappingForm={mappingForm} onSelectDataset={setSelectedDatasetId} onFileListChange={setFileList} onCreateImportSession={() => void handleCreateImportSession()} onConfirmImportSession={() => void handleConfirmImportSession()} onSelectImportTemplate={(templateId) => void handleSelectImportTemplate(templateId)} onApplyImportCleaning={(options) => void handleApplyImportCleaning(options)} onSaveFieldMapping={() => void handleSaveFieldMapping()} onDeleteDataset={(datasetId) => void handleDeleteDataset(datasetId)} /> },
            { key: 'preprocess', label: stageLabels.preprocess, children: <PreprocessTab dataset={selectedDataset} columns={datasetColumns} pipelines={pipelines} selectedPipelineId={selectedPipelineId} selectedPipeline={selectedPipeline} preview={pipelinePreview} stepPreview={preprocessStepPreview} stepPreviewLoading={preprocessStepPreviewLoading} listLoading={workspaceLoading} previewLoading={pipelinePreviewLoading} running={runningPreprocess} advisor={preprocessAdvisor} advisorLoading={preprocessAdvisorLoading} sampledAdvisorRun={sampledAdvisorRun} sampledAdvisorLoading={sampledAdvisorLoading} onRun={(values) => void handleRunPreprocess(values)} onPreviewStep={(index, values) => void handlePreviewPreprocessStep(index, values)} onAnalyzeAdvisor={handleAnalyzePreprocessAdvisor} onRunSampledAdvisor={handleRunSampledPreprocessAdvisor} onSelectPipeline={setSelectedPipelineId} /> },
            { key: 'feature', label: stageLabels.feature, children: <FeatureTab projectId={project?.id ?? null} dataset={selectedDataset} preprocessPipelines={pipelines} pipelines={featurePipelines} templates={featureTemplates} templatesLoading={featureTemplatesLoading} selectedPipelineId={selectedFeaturePipelineId} selectedPipeline={selectedFeaturePipeline} preview={featurePreview} stepPreview={featureStepPreview} listLoading={workspaceLoading} previewLoading={featurePreviewLoading} stepPreviewLoading={featureStepPreviewLoading} running={runningFeaturePipeline} savingTemplate={savingFeatureTemplate} onRun={(values) => void handleRunFeaturePipeline(values)} onPreviewStep={(index, values) => void handlePreviewFeatureStep(index, values)} onSaveTemplate={(values) => void handleSaveFeatureTemplate(values)} onSelectPipeline={setSelectedFeaturePipelineId} /> },
            { key: 'training', label: stageLabels.training, children: <TrainingTab dataset={selectedDataset} columns={featureColumns} featurePipelines={featurePipelines} preprocessPipelines={pipelines} models={models} selectedModelId={selectedModelId} selectedModel={selectedModel} preview={modelPreview} analysis={modelAnalysis} listLoading={workspaceLoading} previewLoading={modelPreviewLoading} analysisLoading={modelAnalysisLoading} running={runningTraining} onRun={(values) => void handleRunTraining(values)} onSelectModel={setSelectedModelId} /> },
            { key: 'analysis', label: stageLabels.analysis, children: <AnalysisTab project={project} models={models} selectedModelId={selectedModelId} selectedModel={selectedModel} preview={modelPreview} analysis={modelAnalysis} llmConfig={llmConfig} llmExplanation={llmExplanation} listLoading={workspaceLoading} previewLoading={modelPreviewLoading} analysisLoading={modelAnalysisLoading} llmConfigLoading={llmConfigLoading} savingLlmConfig={savingLlmConfig} testingLlmConfig={testingLlmConfig} explainingWithLlm={explainingWithLlm} onSelectModel={setSelectedModelId} onSaveLlmConfig={(values) => void handleSaveLlmConfig(values)} onTestLlmConfig={(values) => void handleTestLlmConfig(values)} onRunLlmExplanation={(topK) => void handleRunLlmExplanation(topK)} /> },
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
