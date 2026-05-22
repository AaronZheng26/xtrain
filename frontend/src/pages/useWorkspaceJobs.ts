import { useCallback, useEffect, useMemo, useState } from 'react'

import { api } from '../lib/api'
import { extractApiErrorMessage } from '../lib/errors'
import type { Job, PreprocessTrainingAdvisorRunRead, WorkspaceTabKey } from '../types'

export type PendingWorkspaceJob = {
  jobId: number
  kind: 'preprocess' | 'feature' | 'training' | 'advisor'
  resourceId: number
  tab: WorkspaceTabKey
}

type WorkspaceMessageApi = {
  success: (content: string) => unknown
  error: (content: string) => unknown
}

type UseWorkspaceJobsParams = {
  backendStatus: 'unknown' | 'online' | 'offline'
  selectedDatasetId: number | null
  activeSampledAdvisorRunId: number | null
  sampledAdvisorLoading: boolean
  messageApi: WorkspaceMessageApi
  loadDatasetWorkspace: (datasetId: number) => Promise<void>
  loadPreprocessAdvisorRun: (advisorRunId: number) => Promise<PreprocessTrainingAdvisorRunRead>
  setSelectedPipelineId: (pipelineId: number) => void
  setSelectedFeaturePipelineId: (pipelineId: number) => void
  setSelectedModelId: (modelId: number) => void
  setSampledAdvisorLoading: (loading: boolean) => void
  setActiveSampledAdvisorRunId: (advisorRunId: number | null) => void
  setErrorMessage: (message: string | null) => void
  handleTabChange: (tab: WorkspaceTabKey) => void
}

export function useWorkspaceJobs({
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
}: UseWorkspaceJobsParams) {
  const [jobs, setJobs] = useState<Job[]>([])
  const [pendingWorkspaceJobs, setPendingWorkspaceJobs] = useState<PendingWorkspaceJob[]>([])

  const latestJob = jobs[0] ?? null
  const hasPendingWorkspaceJobs = pendingWorkspaceJobs.length > 0

  const loadJobs = useCallback(async () => {
    const response = await api.get<Job[]>('/jobs')
    setJobs(response.data)
  }, [])

  const addPendingWorkspaceJob = useCallback((pendingJob: PendingWorkspaceJob) => {
    setPendingWorkspaceJobs((current) => [...current, pendingJob])
  }, [])

  const clearWorkspaceJobs = useCallback(() => {
    setJobs([])
    setPendingWorkspaceJobs([])
  }, [])

  useEffect(() => {
    if (backendStatus !== 'online') return
    const timer = window.setInterval(() => {
      if (!document.hidden) void loadJobs()
    }, hasPendingWorkspaceJobs ? 3000 : 10000)
    return () => window.clearInterval(timer)
  }, [backendStatus, hasPendingWorkspaceJobs, loadJobs])

  useEffect(() => {
    if (!activeSampledAdvisorRunId || !sampledAdvisorLoading || backendStatus !== 'online') {
      return
    }
    const advisorRunId = activeSampledAdvisorRunId
    let cancelled = false

    async function pollAdvisorRun() {
      try {
        const run = await loadPreprocessAdvisorRun(advisorRunId)
        if (cancelled) return

        if (run.status === 'completed') {
          setSampledAdvisorLoading(false)
          setActiveSampledAdvisorRunId(null)
          setPendingWorkspaceJobs((current) => current.filter((item) => item.resourceId !== advisorRunId))
          await loadJobs()
          messageApi.success('采样训练适配分析已完成。')
          return
        }

        if (run.status === 'failed') {
          setSampledAdvisorLoading(false)
          setActiveSampledAdvisorRunId(null)
          setPendingWorkspaceJobs((current) => current.filter((item) => item.resourceId !== advisorRunId))
          await loadJobs()
          const failureMessage = '采样训练适配分析执行失败，请检查后端日志。'
          setErrorMessage(failureMessage)
          messageApi.error(failureMessage)
          return
        }
      } catch (error) {
        if (cancelled) return
        const failureMessage = extractApiErrorMessage(error, '加载采样训练适配分析状态失败。')
        setSampledAdvisorLoading(false)
        setActiveSampledAdvisorRunId(null)
        setErrorMessage(failureMessage)
        messageApi.error(failureMessage)
        return
      }

      if (!cancelled) {
        window.setTimeout(() => {
          if (!cancelled) void pollAdvisorRun()
        }, 2000)
      }
    }

    void pollAdvisorRun()

    return () => {
      cancelled = true
    }
  }, [
    activeSampledAdvisorRunId,
    backendStatus,
    loadJobs,
    loadPreprocessAdvisorRun,
    messageApi,
    sampledAdvisorLoading,
    setActiveSampledAdvisorRunId,
    setErrorMessage,
    setSampledAdvisorLoading,
  ])

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
          setActiveSampledAdvisorRunId(null)
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
  }, [
    handleTabChange,
    jobs,
    loadDatasetWorkspace,
    loadJobs,
    loadPreprocessAdvisorRun,
    messageApi,
    pendingWorkspaceJobs,
    selectedDatasetId,
    setActiveSampledAdvisorRunId,
    setErrorMessage,
    setSampledAdvisorLoading,
    setSelectedFeaturePipelineId,
    setSelectedModelId,
    setSelectedPipelineId,
  ])

  return useMemo(
    () => ({
      jobs,
      latestJob,
      pendingWorkspaceJobs,
      hasPendingWorkspaceJobs,
      loadJobs,
      addPendingWorkspaceJob,
      setPendingWorkspaceJobs,
      clearWorkspaceJobs,
    }),
    [
      addPendingWorkspaceJob,
      clearWorkspaceJobs,
      hasPendingWorkspaceJobs,
      jobs,
      latestJob,
      loadJobs,
      pendingWorkspaceJobs,
    ],
  )
}
