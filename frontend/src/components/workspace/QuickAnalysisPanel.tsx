import { Alert, Button, Card, Space, Steps, Tag, Typography } from 'antd'
import type { AnalysisGoalState, DatasetReadiness } from '../../types'
import { analysisGoalOptions, analysisModeOptions } from './analysisGoalOptions'

const { Text } = Typography

type Props = {
  goalState: AnalysisGoalState
  readiness: DatasetReadiness
  onOpenData: () => void
  onOpenFeature: () => void
  onOpenTraining: () => void
  onOpenAnalysis: () => void
}

export function QuickAnalysisPanel({ goalState, readiness, onOpenData, onOpenFeature, onOpenTraining, onOpenAnalysis }: Props) {
  const goal = analysisGoalOptions.find((option) => option.value === goalState.goal)?.label ?? '自定义日志异常'
  const mode = analysisModeOptions.find((option) => option.value === goalState.mode)?.label ?? readiness.recommended_mode
  const template = readiness.recommended_templates[0] ?? '通用安全日志模板'

  return (
    <Card className="quick-analysis-card" title="推荐快速分析路径" extra={<Tag color="processing">{goal}</Tag>}>
      <Space direction="vertical" size={16} className="full-width">
        <Alert
          type={readiness.score >= 60 ? 'success' : 'warning'}
          showIcon
          message={readiness.score >= 60 ? '当前数据可以先走快速分析。' : '当前数据建议先补字段映射或清洗，再运行分析。'}
          description={`推荐模式：${readiness.recommended_mode || mode}；推荐模板：${template}。`}
        />
        <Steps
          direction="vertical"
          size="small"
          current={0}
          items={[
            {
              title: '确认字段映射',
              description: '检查时间、主体、行为字段是否识别正确，必要时在数据页调整。',
            },
            {
              title: '生成异常分析特征',
              description: `优先使用 ${template}，把原始字段转成可训练的异常信号。`,
            },
            {
              title: '建立检测模型',
              description: '默认使用无监督异常发现；如果发现可信 label，可切换有标签训练。',
            },
            {
              title: '进入异常研判',
              description: '查看异常样本、业务上下文、相似样本，并记录真实异常/误报/待确认。',
            },
          ]}
        />
        <Space wrap>
          <Button onClick={onOpenData}>检查字段</Button>
          <Button onClick={onOpenFeature}>生成特征</Button>
          <Button onClick={onOpenTraining}>建立模型</Button>
          <Button type="primary" onClick={onOpenAnalysis}>查看异常结果</Button>
        </Space>
        <Text type="secondary">MVP 阶段先采用推荐与半自动引导，不会自动跳过专家确认步骤。</Text>
      </Space>
    </Card>
  )
}
