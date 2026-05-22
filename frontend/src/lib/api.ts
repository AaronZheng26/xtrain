import axios from 'axios'
import type { AnalysisGoalState, DatasetReadiness } from '../types'

const baseURL = import.meta.env.VITE_API_BASE_URL || '/api/v1'

export const api = axios.create({
  baseURL,
  timeout: 10000,
})

export async function getDatasetReadiness(datasetId: number, goalState?: AnalysisGoalState) {
  const response = await api.get<DatasetReadiness>(`/datasets/${datasetId}/readiness`, {
    params: {
      goal: goalState ? analysisGoalLabel(goalState.goal) : undefined,
      log_type: goalState?.logType,
    },
  })
  return response.data
}

function analysisGoalLabel(goal: AnalysisGoalState['goal']) {
  const labels: Record<AnalysisGoalState['goal'], string> = {
    web_access_anomaly: 'Web 访问异常',
    login_anomaly: '登录异常',
    network_traffic_anomaly: '网络流量异常',
    program_runtime_anomaly: '程序运行异常',
    api_abuse: 'API 滥用',
    custom_log_anomaly: '自定义日志异常',
  }
  return labels[goal]
}
