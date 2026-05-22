import type { AnalysisGoalId, AnalysisLogTypeId, AnalysisModeId, DatasetReadinessLevel } from '../types'

export const analysisGoalOptions: Array<{ label: string; value: AnalysisGoalId; description: string }> = [
  { label: 'Web 访问异常', value: 'web_access_anomaly', description: '发现异常路径、状态码、访问频次和扫描行为。' },
  { label: '登录异常', value: 'login_anomaly', description: '关注账号、来源 IP、登录结果和短时间爆发。' },
  { label: '网络流量异常', value: 'network_traffic_anomaly', description: '识别源/目的 IP、端口、协议和流量突增。' },
  { label: '程序运行异常', value: 'program_runtime_anomaly', description: '分析主机、进程、级别、错误消息和异常堆栈。' },
  { label: 'API 滥用', value: 'api_abuse', description: '关注接口路径、调用方、状态码和高频访问模式。' },
  { label: '自定义日志异常', value: 'custom_log_anomaly', description: '先做字段探测，再按识别结果推荐分析路径。' },
]

export const analysisLogTypeOptions: Array<{ label: string; value: AnalysisLogTypeId }> = [
  { label: 'Nginx access log', value: 'nginx_access' },
  { label: '程序运行日志', value: 'program_runtime' },
  { label: 'NTA / NetFlow', value: 'nta_flow' },
  { label: 'CSV 表格日志', value: 'csv_table' },
  { label: '自定义文本日志', value: 'custom_text' },
]

export const analysisModeOptions: Array<{ label: string; value: AnalysisModeId; description: string }> = [
  { label: '快速无监督异常发现', value: 'quick_unsupervised', description: '没有标签也能先跑异常分数，适合快速排查。' },
  { label: '有标签监督训练', value: 'supervised', description: '已有可信标签时使用，结果更可控。' },
  { label: '规则 + 模型混合分析', value: 'hybrid', description: '先保留规则判断，再用模型发现未知异常。' },
  { label: '只做数据清洗与字段探测', value: 'cleaning_only', description: '数据还不稳定时，先整理字段和格式。' },
]

export function getReadinessLevelMeta(level: DatasetReadinessLevel) {
  const meta: Record<DatasetReadinessLevel, { label: string; color: string; summary: string }> = {
    excellent: { label: '适合直接快速分析', color: 'green', summary: '关键字段和数据规模都比较完整。' },
    good: { label: '可以分析，建议确认字段', color: 'blue', summary: '可以先跑快速分析，但建议补充映射或清洗。' },
    limited: { label: '只能做有限分析', color: 'orange', summary: '建议先做字段映射、清洗或补充上下文。' },
    poor: { label: '不建议直接训练', color: 'red', summary: '请先检查格式、字段和解析模板。' },
  }
  return meta[level]
}

export function analysisGoalLabel(goal: AnalysisGoalId) {
  return analysisGoalOptions.find((option) => option.value === goal)?.label ?? '自定义日志异常'
}

export function analysisLogTypeLabel(logType: AnalysisLogTypeId) {
  return analysisLogTypeOptions.find((option) => option.value === logType)?.label ?? '自定义文本日志'
}

export function analysisModeLabel(mode: AnalysisModeId) {
  return analysisModeOptions.find((option) => option.value === mode)?.label ?? '快速无监督异常发现'
}
