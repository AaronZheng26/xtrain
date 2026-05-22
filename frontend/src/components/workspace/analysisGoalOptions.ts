import type { AnalysisGoalId, AnalysisLogTypeId, AnalysisModeId } from '../../types'

export const analysisGoalOptions: Array<{ value: AnalysisGoalId; label: string; description: string }> = [
  { value: 'web_access_anomaly', label: 'Web 访问异常', description: '发现异常路径、状态码激增、可疑 UA 或访问爆发。' },
  { value: 'login_anomaly', label: '登录异常', description: '关注账号、来源 IP、失败登录和短时高频行为。' },
  { value: 'network_traffic_anomaly', label: '网络流量异常', description: '适合 NetFlow/NTA，关注源目 IP、端口、协议和流量突增。' },
  { value: 'program_runtime_anomaly', label: '程序运行异常', description: '适合运行日志，关注主机、进程、级别、错误关键字。' },
  { value: 'api_abuse', label: 'API 滥用', description: '识别高频调用、异常 endpoint、异常账号或来源。' },
  { value: 'custom_log_anomaly', label: '自定义日志异常', description: '字段不固定时，先做字段探测与通用异常发现。' },
]

export const analysisLogTypeOptions: Array<{ value: AnalysisLogTypeId; label: string }> = [
  { value: 'nginx_access', label: 'Nginx access log' },
  { value: 'program_runtime', label: '程序运行日志' },
  { value: 'nta_flow', label: 'NTA / NetFlow' },
  { value: 'csv_table', label: 'CSV 表格日志' },
  { value: 'custom_text', label: '自定义文本日志' },
]

export const analysisModeOptions: Array<{ value: AnalysisModeId; label: string; description: string }> = [
  { value: 'quick_unsupervised', label: '快速无监督异常发现', description: '没有标签时优先推荐，先快速找出可疑样本。' },
  { value: 'supervised', label: '有标签监督训练', description: '已有可信 label 时使用。' },
  { value: 'hybrid', label: '规则 + 模型混合分析', description: '先保留规则判断，再用模型补充未知异常。' },
  { value: 'cleaning_only', label: '只做数据清洗与字段探测', description: '字段质量不足时先整理数据。' },
]
