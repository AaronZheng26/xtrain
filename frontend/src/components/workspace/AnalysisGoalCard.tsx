import { Card, Radio, Select, Space, Typography } from 'antd'
import type { AnalysisGoalState, AnalysisGoalId } from '../../types'
import { analysisGoalOptions, analysisLogTypeOptions, analysisModeOptions } from './analysisGoalOptions'

const { Text, Title } = Typography

type Props = {
  value: AnalysisGoalState
  onChange: (value: AnalysisGoalState) => void
}

export function AnalysisGoalCard({ value, onChange }: Props) {
  const selectedGoal = analysisGoalOptions.find((option) => option.value === value.goal)
  const selectedMode = analysisModeOptions.find((option) => option.value === value.mode)

  return (
    <Card className="analysis-goal-card" title="先选择这次要解决的安全分析问题">
      <Space direction="vertical" size={16} className="full-width">
        <div>
          <Title level={5}>分析目标</Title>
          <Radio.Group
            className="analysis-goal-radio"
            optionType="button"
            buttonStyle="solid"
            value={value.goal}
            options={analysisGoalOptions.map((option) => ({ value: option.value, label: option.label }))}
            onChange={(event) => onChange({ ...value, goal: event.target.value as AnalysisGoalId })}
          />
          {selectedGoal ? <Text type="secondary">{selectedGoal.description}</Text> : null}
        </div>
        <div className="analysis-goal-controls">
          <div>
            <Text strong>日志类型</Text>
            <Select
              value={value.logType}
              className="full-width top-gap"
              options={analysisLogTypeOptions}
              onChange={(nextValue) => onChange({ ...value, logType: nextValue })}
            />
          </div>
          <div>
            <Text strong>分析模式</Text>
            <Select
              value={value.mode}
              className="full-width top-gap"
              options={analysisModeOptions.map((option) => ({ value: option.value, label: option.label }))}
              onChange={(nextValue) => onChange({ ...value, mode: nextValue })}
            />
            {selectedMode ? <Text type="secondary">{selectedMode.description}</Text> : null}
          </div>
        </div>
      </Space>
    </Card>
  )
}
