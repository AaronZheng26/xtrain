import { Alert, Button, Card, Collapse, Space, Tag, Typography } from 'antd'
import { RobotOutlined, SearchOutlined } from '@ant-design/icons'
import { useMemo, useState } from 'react'
import type { InvestigationVerdict } from '../../types'

const { Text, Paragraph } = Typography

const verdictOptions: Array<{ value: InvestigationVerdict; label: string; color: string }> = [
  { value: 'true_positive', label: '真实异常', color: 'red' },
  { value: 'false_positive', label: '误报', color: 'green' },
  { value: 'pending', label: '待确认', color: 'orange' },
]

type Props = {
  sampleKey: string
  sample: Record<string, unknown>
  similarMatches: Array<{ label: string; count: number }>
  llmConfigured: boolean
  llmLoading: boolean
  onGenerateAdvice: () => void
}

export function InvestigationActions({ sampleKey, sample, similarMatches, llmConfigured, llmLoading, onGenerateAdvice }: Props) {
  const [verdicts, setVerdicts] = useState<Record<string, InvestigationVerdict>>({})
  const [whitelistSamples, setWhitelistSamples] = useState<Record<string, boolean>>({})
  const verdict = verdicts[sampleKey]
  const rawLog = useMemo(() => pickRawLog(sample), [sample])

  return (
    <Card size="small" title="研判动作">
      <Space direction="vertical" size={12} className="full-width">
        <Space wrap>
          {verdictOptions.map((option) => (
            <Button
              key={option.value}
              type={verdict === option.value ? 'primary' : 'default'}
              danger={option.value === 'true_positive' && verdict === option.value}
              onClick={() => setVerdicts((current) => ({ ...current, [sampleKey]: option.value }))}
            >
              {option.label}
            </Button>
          ))}
          {verdict ? <Tag color={verdictOptions.find((option) => option.value === verdict)?.color}>已标记：{verdictOptions.find((option) => option.value === verdict)?.label}</Tag> : null}
        </Space>
        <Space wrap>
          <Button onClick={() => setWhitelistSamples((current) => ({ ...current, [sampleKey]: true }))}>
            加入白名单
          </Button>
          <Button icon={<SearchOutlined />} disabled={!similarMatches.length}>
            查找相似样本
          </Button>
          <Button icon={<RobotOutlined />} loading={llmLoading} disabled={!llmConfigured} onClick={onGenerateAdvice}>
            生成处置建议
          </Button>
        </Space>
        {whitelistSamples[sampleKey] ? (
          <Alert type="info" showIcon message="已记录为白名单候选。MVP 阶段仅保存在当前页面，后续可落库为规则。" />
        ) : null}
        {!llmConfigured ? (
          <Alert type="warning" showIcon message="未配置可用 LLM。可以先跳过 AI 解释，或在上方配置本地/在线模型后再生成处置建议。" />
        ) : null}
        {similarMatches.length ? (
          <div>
            <Text strong>相似样本线索</Text>
            <div className="tag-wall">
              {similarMatches.map((match) => (
                <Tag color="cyan" key={match.label}>{match.label}: {match.count} 条</Tag>
              ))}
            </div>
          </div>
        ) : null}
        {rawLog ? (
          <Collapse
            size="small"
            items={[
              {
                key: 'raw-log',
                label: '查看原始日志 / 业务上下文',
                children: <Paragraph copyable style={{ whiteSpace: 'pre-wrap', marginBottom: 0 }}>{rawLog}</Paragraph>,
              },
            ]}
          />
        ) : null}
      </Space>
    </Card>
  )
}

function pickRawLog(sample: Record<string, unknown>) {
  const candidates = ['raw_message', 'message', 'log', 'event', 'request', 'path']
  for (const column of candidates) {
    const value = sample[column]
    if (value !== null && value !== undefined && String(value).trim()) {
      return String(value)
    }
  }
  return ''
}
