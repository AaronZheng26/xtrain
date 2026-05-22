import { useCallback, useMemo, useState } from 'react'

import { api } from '../lib/api'
import type {
  DatasetPreviewRead,
  DatasetVersion,
  DatasetWorkspaceRead,
  FeaturePipeline,
  FieldMapping,
  ModelVersion,
  PreprocessPipeline,
  Project,
} from '../types'

type BackendStatus = 'unknown' | 'online' | 'offline'

type UseWorkspaceDataParams = {
  setBackendStatus: (status: BackendStatus) => void
  setErrorMessage: (message: string | null) => void
}

export function useWorkspaceData({ setBackendStatus, setErrorMessage }: UseWorkspaceDataParams) {
  const [project, setProject] = useState<Project | null>(null)
  const [datasets, setDatasets] = useState<DatasetVersion[]>([])
  const [selectedDatasetId, setSelectedDatasetId] = useState<number | null>(null)
  const [selectedDataset, setSelectedDataset] = useState<DatasetVersion | null>(null)
  const [datasetPreview, setDatasetPreview] = useState<DatasetPreviewRead | null>(null)
  const [fieldMapping, setFieldMapping] = useState<FieldMapping | null>(null)
  const [pipelines, setPipelines] = useState<PreprocessPipeline[]>([])
  const [featurePipelines, setFeaturePipelines] = useState<FeaturePipeline[]>([])
  const [models, setModels] = useState<ModelVersion[]>([])
  const [selectedPipelineId, setSelectedPipelineId] = useState<number | null>(null)
  const [selectedFeaturePipelineId, setSelectedFeaturePipelineId] = useState<number | null>(null)
  const [selectedModelId, setSelectedModelId] = useState<number | null>(null)
  const [datasetsLoading, setDatasetsLoading] = useState(false)
  const [workspaceLoading, setWorkspaceLoading] = useState(false)

  const selectedPipeline = useMemo(
    () => pipelines.find((pipeline) => pipeline.id === selectedPipelineId) ?? null,
    [pipelines, selectedPipelineId],
  )
  const selectedFeaturePipeline = useMemo(
    () => featurePipelines.find((pipeline) => pipeline.id === selectedFeaturePipelineId) ?? null,
    [featurePipelines, selectedFeaturePipelineId],
  )
  const selectedModel = useMemo(
    () => models.find((model) => model.id === selectedModelId) ?? null,
    [models, selectedModelId],
  )
  const datasetColumns = selectedDataset?.schema_snapshot.map((field) => field.name) ?? []

  const loadProject = useCallback(async (projectIdValue: number) => {
    const response = await api.get<Project>(`/projects/${projectIdValue}`)
    setProject(response.data)
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
  }, [setBackendStatus, setErrorMessage])

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
  }, [setBackendStatus, setErrorMessage])

  const clearSelectedDatasetWorkspace = useCallback(() => {
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
  }, [])

  const resetWorkspaceDataState = useCallback(() => {
    setProject(null)
    setDatasets([])
    clearSelectedDatasetWorkspace()
    setDatasetsLoading(false)
    setWorkspaceLoading(false)
  }, [clearSelectedDatasetWorkspace])

  return {
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
  }
}
