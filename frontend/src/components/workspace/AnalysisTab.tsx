import { useEffect, useMemo, useState } from 'react'
import { Alert, AutoComplete, Button, Card, Collapse, Descriptions, Empty, Form, Input, InputNumber, List, Select, Space, Switch, Table, Tag, Typography } from 'antd'
import { RobotOutlined } from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'

import { DetailPanel } from '../DetailPanel'
import { StageLayout } from '../StageLayout'
import { UnsupervisedResultCharts } from '../UnsupervisedResultCharts'
import { buildPreviewColumns } from '../../lib/ui'
import type { LlmProviderConfig, LlmProviderConfigPayload, ModelAnalysisRead, ModelLlmExplanationRead, ModelPreviewRead, ModelVersion, Project } from '../../types'

const { Text } = Typography

type Props = {
  project: Project | null
  models: ModelVersion[]
  selectedModelId: number | null
  selectedModel: ModelVersion | null
  preview: ModelPreviewRead | null
  analysis: ModelAnalysisRead | null
  llmConfig: LlmProviderConfig | null
  llmExplanation: ModelLlmExplanationRead | null
  listLoading: boolean
  previewLoading: boolean
  analysisLoading: boolean
  llmConfigLoading: boolean
  savingLlmConfig: boolean
  testingLlmConfig: boolean
  explainingWithLlm: boolean
  onSelectModel: (modelId: number) => void
  onSaveLlmConfig: (values: LlmProviderConfigPayload) => void
  onTestLlmConfig: (values: LlmProviderConfigPayload) => void
  onRunLlmExplanation: (topK: number) => void
}

type SignalSampleRow = Record<string, unknown> & {
  __rowKey: string
}

export function AnalysisTab(props: Props) {
  const [configForm] = Form.useForm<LlmProviderConfigPayload & { topK: number; apiKey: string }>()
  const [selectedSignal, setSelectedSignal] = useState<{ modelId: number; column: string; type: 'spike' | 'count' } | null>(null)
  const [selectedSignalSampleKey, setSelectedSignalSampleKey] = useState<string | null>(null)
  const signalSummaryColumns = [
    { title: '特征列', dataIndex: 'column', key: 'column', width: 220 },
    { title: '异常均值', dataIndex: 'anomaly_mean', key: 'anomaly_mean', render: renderMetricValue },
    { title: '正常均值', dataIndex: 'normal_mean', key: 'normal_mean', render: renderMetricValue },
    { title: '异常最大值', dataIndex: 'anomaly_max', key: 'anomaly_max', render: renderMetricValue },
    { title: '异常激活率', dataIndex: 'anomaly_active_rate', key: 'anomaly_active_rate', render: renderRateValue },
    { title: '正常激活率', dataIndex: 'normal_active_rate', key: 'normal_active_rate', render: renderRateValue },
  ]

  useEffect(() => {
    configForm.setFieldsValue({
      provider: props.llmConfig?.provider ?? 'ollama',
      enabled: props.llmConfig?.enabled ?? true,
      base_url: props.llmConfig?.base_url ?? '',
      model_name: props.llmConfig?.model_name ?? '',
      clear_api_key: false,
      apiKey: '',
      topK: 5,
    })
  }, [props.llmConfig, configForm])

  const provider = Form.useWatch('provider', configForm) ?? 'ollama'
  const selectedSignalColumn = selectedSignal?.modelId === props.selectedModelId ? selectedSignal.column : null
  const selectedSignalType = selectedSignal?.modelId === props.selectedModelId ? selectedSignal.type : null

  const signalSampleRows = useMemo<SignalSampleRow[]>(() => {
    if (!selectedSignalColumn || !props.preview?.rows?.length || !props.preview.columns.includes(selectedSignalColumn)) {
      return []
    }
    return props.preview.rows
      .filter((row) => {
        const value = toNumericValue(row[selectedSignalColumn])
        if (value === null) return false
        return value > 0
      })
      .sort((left, right) => (toNumericValue(right[selectedSignalColumn]) ?? 0) - (toNumericValue(left[selectedSignalColumn]) ?? 0))
      .slice(0, 8)
      .map((row, index) => ({
        ...row,
        __rowKey: buildSignalSampleKey(row, index),
      }))
  }, [props.preview, selectedSignalColumn])

  const selectedSignalSample = useMemo<SignalSampleRow | null>(() => {
    if (!selectedSignalSampleKey) return null
    return signalSampleRows.find((row) => String(row.__rowKey) === selectedSignalSampleKey) ?? null
  }, [selectedSignalSampleKey, signalSampleRows])

  const selectedSignalSampleContext = useMemo(() => {
    if (!selectedSignalSample || !selectedSignalColumn) return []
    return buildSignalSampleContext(selectedSignalSample, selectedSignalColumn)
  }, [selectedSignalSample, selectedSignalColumn])

  const selectedSignalSampleMatches = useMemo(() => {
    if (!selectedSignalSample || !props.preview?.rows?.length) return []
    return buildSignalSampleMatches(props.preview.rows, selectedSignalSample)
  }, [props.preview, selectedSignalSample])

  const signalWindowDetail = useMemo(() => {
    if (!selectedSignalSample || !props.preview?.rows?.length || !selectedSignalColumn) {
      return null
    }
    return buildSignalWindowDetail(props.preview.rows, selectedSignalSample, selectedSignalColumn)
  }, [props.preview, selectedSignalSample, selectedSignalColumn])

  useEffect(() => {
    const currentUrl = configForm.getFieldValue('base_url')
    const currentModel = configForm.getFieldValue('model_name')
    if (provider === 'minimax') {
      if (!currentUrl || currentUrl === 'https://api.example.com/v1') {
        configForm.setFieldValue('base_url', 'https://api.minimaxi.com/v1')
      }
      if (!currentModel) {
        configForm.setFieldValue('model_name', 'MiniMax-M2.5')
      }
    }
    if (provider === 'ollama' && !currentUrl) {
      configForm.setFieldValue('base_url', 'http://127.0.0.1:11434')
    }
  }, [provider, configForm])

  function handleSaveConfig() {
    const values = configForm.getFieldsValue()
    props.onSaveLlmConfig({
      provider: values.provider ?? 'ollama',
      enabled: values.enabled ?? true,
      base_url: values.base_url ?? '',
      model_name: values.model_name ?? '',
      api_key: values.apiKey?.trim() ? values.apiKey : null,
      clear_api_key: values.clear_api_key ?? false,
    })
    configForm.setFieldValue('apiKey', '')
    configForm.setFieldValue('clear_api_key', false)
  }

  function handleExplain() {
    const topK = configForm.getFieldValue('topK') ?? 5
    props.onRunLlmExplanation(Number(topK))
  }

  function handleTestConfig() {
    const values = configForm.getFieldsValue()
    props.onTestLlmConfig({
      provider: values.provider ?? 'ollama',
      enabled: values.enabled ?? true,
      base_url: values.base_url ?? '',
      model_name: values.model_name ?? '',
      api_key: values.apiKey?.trim() ? values.apiKey : props.llmConfig?.has_api_key ? '' : null,
      clear_api_key: false,
    })
  }

  const modelOptions =
    provider === 'minimax'
      ? [
          { value: 'MiniMax-M2.5', label: 'MiniMax-M2.5' },
          { value: 'MiniMax-M2.5-highspeed', label: 'MiniMax-M2.5-highspeed' },
          { value: 'MiniMax-M2.1', label: 'MiniMax-M2.1' },
          { value: 'MiniMax-M2.1-highspeed', label: 'MiniMax-M2.1-highspeed' },
          { value: 'MiniMax-M2', label: 'MiniMax-M2' },
          { value: 'M2-her', label: 'M2-her' },
        ]
      : []

  return (
    <StageLayout
      main={
        <Space direction="vertical" size={20} className="full-width">
          <Card title="异常结果中心">
            <Space direction="vertical" size={16} className="full-width">
              <Alert type="info" showIcon message="支持本地 Ollama、MiniMax 和在线 OpenAI 兼容接口。保存配置后，可直接对当前模型的 Top 异常样本发起 AI 解读。" />
              <Select
                value={props.selectedModelId ?? undefined}
                placeholder="选择一个模型版本"
                options={props.models.map((model) => ({ label: `${model.name} (${model.algorithm})`, value: model.id }))}
                onChange={props.onSelectModel}
              />
            </Space>
          </Card>
          <Card title="大模型分析配置" loading={props.llmConfigLoading} extra={props.project ? <Tag color="processing">{props.project.name}</Tag> : null}>
            <Form form={configForm} layout="vertical" initialValues={{ provider: 'ollama', enabled: true, topK: 5 }}>
              <Form.Item name="provider" label="模型提供方">
                <Select
                  options={[
                    { label: '本地 Ollama', value: 'ollama' },
                    { label: 'MiniMax', value: 'minimax' },
                    { label: '在线 OpenAI 兼容接口', value: 'openai_compatible' },
                  ]}
                />
              </Form.Item>
              {provider === 'minimax' ? (
                <Alert
                  type="info"
                  showIcon
                  message="MiniMax 官方文档给出的 OpenAI 兼容地址是 https://api.minimaxi.com/v1。文本生成常用模型可先尝试 MiniMax-M2.5、MiniMax-M2.5-highspeed；对话场景可用 M2-her。"
                />
              ) : null}
              <Form.Item name="base_url" label="接口地址" rules={[{ required: true, message: '请输入接口地址' }]}>
                <Input placeholder="例如 http://127.0.0.1:11434 或 https://api.example.com/v1" />
              </Form.Item>
              <Form.Item name="model_name" label="模型名称" rules={[{ required: true, message: '请输入模型名称' }]}>
                <AutoComplete
                  options={modelOptions}
                  filterOption={(inputValue, option) => String(option?.value ?? '').toLowerCase().includes(inputValue.toLowerCase())}
                >
                  <Input placeholder="例如 qwen2.5:7b-instruct、MiniMax-M2.7 或 gpt-4o-mini" />
                </AutoComplete>
              </Form.Item>
              <Form.Item name="enabled" label="启用 AI 分析" valuePropName="checked">
                <Switch checkedChildren="启用" unCheckedChildren="禁用" />
              </Form.Item>
              <Form.Item name="apiKey" label="API Key">
                <Input.Password placeholder={props.llmConfig?.api_key_hint ? `已保存 Key: ${props.llmConfig.api_key_hint}` : '留空表示不更新'} />
              </Form.Item>
              <Form.Item name="clear_api_key" label="清空已保存 Key" valuePropName="checked">
                <Switch checkedChildren="清空" unCheckedChildren="保留" />
              </Form.Item>
              <Form.Item name="topK" label="分析 Top 异常数">
                <InputNumber min={1} max={15} style={{ width: '100%' }} />
              </Form.Item>
              <Space>
                <Button type="primary" loading={props.savingLlmConfig} onClick={handleSaveConfig}>
                  保存配置
                </Button>
                <Button loading={props.testingLlmConfig} onClick={handleTestConfig}>
                  测试连接
                </Button>
                <Button
                  icon={<RobotOutlined />}
                  loading={props.explainingWithLlm}
                  disabled={!props.selectedModelId}
                  onClick={handleExplain}
                >
                  分析当前模型异常
                </Button>
              </Space>
            </Form>
          </Card>
          <Card title="模型结果列表">
            <List
              loading={props.listLoading}
              locale={{ emptyText: '当前项目还没有模型结果可供分析。' }}
              dataSource={props.models}
              renderItem={(model) => (
                <List.Item
                  className={model.id === props.selectedModelId ? 'selectable-row is-selected' : 'selectable-row'}
                  onClick={() => props.onSelectModel(model.id)}
                >
                  <List.Item.Meta
                    title={<Space><Text strong>{model.name}</Text><Tag color={model.mode === 'unsupervised' ? 'magenta' : 'green'}>{model.mode}</Tag></Space>}
                    description={`${model.algorithm} / ${model.status}`}
                  />
                </List.Item>
              )}
            />
          </Card>
        </Space>
      }
      detail={
        <DetailPanel title="异常详情与 AI 分析" extra={props.selectedModel ? <Tag color="magenta">{props.selectedModel.name}</Tag> : null}>
          {props.selectedModel ? (
            <Space direction="vertical" size={16} className="full-width">
              <Descriptions
                column={1}
                items={[
                  { key: 'mode', label: '模型模式', children: props.selectedModel.mode },
                  { key: 'algorithm', label: '算法', children: props.selectedModel.algorithm },
                  { key: 'status', label: '状态', children: props.selectedModel.status },
                  { key: 'predictions', label: '结果文件', children: props.selectedModel.prediction_path ?? '未生成' },
                ]}
              />
              <Card size="small" className="nested-card" title="评估与异常摘要">
                <Space wrap>
                  {Object.entries(props.preview?.metrics ?? props.selectedModel.metrics).map(([key, value]) => (
                    <Tag key={key} color="purple">{key}: {String(value)}</Tag>
                  ))}
                </Space>
              </Card>
              {props.selectedModel.mode === 'unsupervised' ? (
                props.analysis ? (
                  <Space direction="vertical" size={16} className="full-width">
                    <Card size="small" className="nested-card" title="异常可视化">
                      <Space direction="vertical" size={12} className="full-width">
                        <Space wrap>
                          <Tag color="red">异常样本: {props.analysis.anomaly_count}</Tag>
                          <Tag color="geekblue">投影点数: {props.analysis.sample_size}</Tag>
                        </Space>
                        <UnsupervisedResultCharts analysis={props.analysis} />
                      </Space>
                    </Card>
                    <Card size="small" className="nested-card" title="时间窗与突增信号摘要">
                      <Space direction="vertical" size={16} className="full-width">
                        <Card size="small" title="突增标记摘要">
                          {props.analysis.spike_signal_summaries.length ? (
                            <Table
                              rowKey="column"
                              columns={signalSummaryColumns}
                              dataSource={props.analysis.spike_signal_summaries}
                              pagination={false}
                              scroll={{ x: 960 }}
                              size="small"
                              onRow={(record) => ({
                                onClick: () => {
                                  if (props.selectedModelId) {
                                    setSelectedSignal({ modelId: props.selectedModelId, column: record.column, type: 'spike' })
                                    setSelectedSignalSampleKey(null)
                                  }
                                },
                              })}
                              rowClassName={(record) => record.column === selectedSignalColumn ? 'selectable-row is-selected' : 'selectable-row'}
                            />
                          ) : (
                            <Text type="secondary">当前结果里还没有检测到突增标记类特征。</Text>
                          )}
                        </Card>
                        <Card size="small" title="计数类信号摘要">
                          {props.analysis.count_signal_summaries.length ? (
                            <Table
                              rowKey="column"
                              columns={signalSummaryColumns}
                              dataSource={props.analysis.count_signal_summaries}
                              pagination={false}
                              scroll={{ x: 960 }}
                              size="small"
                              onRow={(record) => ({
                                onClick: () => {
                                  if (props.selectedModelId) {
                                    setSelectedSignal({ modelId: props.selectedModelId, column: record.column, type: 'count' })
                                    setSelectedSignalSampleKey(null)
                                  }
                                },
                              })}
                              rowClassName={(record) => record.column === selectedSignalColumn ? 'selectable-row is-selected' : 'selectable-row'}
                            />
                          ) : (
                            <Text type="secondary">当前结果里还没有窗口计数或聚合计数类特征。</Text>
                          )}
                        </Card>
                      </Space>
                    </Card>
                    <Card
                      size="small"
                      className="nested-card"
                      title="信号联动样本"
                      extra={selectedSignalColumn ? <Tag color="blue">{selectedSignalColumn}</Tag> : null}
                    >
                      {selectedSignalColumn ? (
                        props.preview?.columns.includes(selectedSignalColumn) ? (
                          signalSampleRows.length ? (
                            <Space direction="vertical" size={12} className="full-width">
                              <Alert
                                type="info"
                                showIcon
                                message={`当前正在查看 ${selectedSignalType === 'spike' ? '突增标记' : '计数'} 特征 ${selectedSignalColumn} 对应的高分异常样本。`}
                              />
                              <Table<Record<string, unknown>>
                                rowKey={(record) => String(record.__rowKey)}
                                columns={buildPreviewColumns(props.preview?.columns ?? [])}
                                dataSource={signalSampleRows}
                                pagination={{ pageSize: 5, hideOnSinglePage: true }}
                                scroll={{ x: 1000 }}
                                size="small"
                                onRow={(record) => ({
                                  onClick: () => setSelectedSignalSampleKey(String(record.__rowKey)),
                                })}
                                rowClassName={(record) => String(record.__rowKey) === selectedSignalSampleKey ? 'selectable-row is-selected' : 'selectable-row'}
                              />
                            </Space>
                          ) : (
                            <Text type="secondary">当前预览范围内还没有命中该信号的异常样本。</Text>
                          )
                        ) : (
                          <Text type="secondary">当前模型预览里没有包含 {selectedSignalColumn} 这一列，暂时无法联动样本。</Text>
                        )
                      ) : (
                        <Text type="secondary">点击上面的任意摘要行，这里会联动展示对应的高分异常样本。</Text>
                      )}
                    </Card>
                    <Card
                      size="small"
                      className="nested-card"
                      title="样本反查摘要"
                      extra={selectedSignalSample ? <Tag color="purple">已选样本</Tag> : null}
                    >
                      {selectedSignalSample ? (
                        <Space direction="vertical" size={12} className="full-width">
                          <Descriptions
                            column={1}
                            items={[
                              { key: 'signal', label: '联动信号', children: selectedSignalColumn ?? '--' },
                              { key: 'signalValue', label: '信号值', children: formatPreviewValue(selectedSignalSample[selectedSignalColumn ?? '']) },
                              { key: 'score', label: '异常分数', children: formatPreviewValue(selectedSignalSample.anomaly_score) },
                              { key: 'predicted', label: '预测结果', children: formatPreviewValue(selectedSignalSample.predicted_label) },
                              { key: 'actual', label: '真实标签', children: formatPreviewValue(selectedSignalSample.actual_label) },
                            ]}
                          />
                          <Card size="small" title="关键上下文字段">
                            {selectedSignalSampleContext.length ? (
                              <div className="tag-wall">
                                {selectedSignalSampleContext.map((entry) => (
                                  <Tag color="blue" key={entry.label}>{entry.label}: {entry.value}</Tag>
                                ))}
                              </div>
                            ) : (
                              <Text type="secondary">当前样本里没有识别到明显的上下文字段。</Text>
                            )}
                          </Card>
                          <Card size="small" title="当前预览内相同字段值命中数">
                            {selectedSignalSampleMatches.length ? (
                              <div className="tag-wall">
                                {selectedSignalSampleMatches.map((entry) => (
                                  <Tag color="cyan" key={entry.label}>{entry.label}: {entry.count} 条</Tag>
                                ))}
                              </div>
                            ) : (
                              <Text type="secondary">当前预览里暂时没有更多可用于对比的相同字段值。</Text>
                            )}
                          </Card>
                        </Space>
                      ) : (
                        <Text type="secondary">点击上方联动样本表中的某一行，这里会显示该样本的关键字段和在当前预览中的相似命中情况。</Text>
                      )}
                    </Card>
                    <Card
                      size="small"
                      className="nested-card"
                      title="同组时间窗明细"
                      extra={signalWindowDetail?.windowMinutes ? <Tag color="geekblue">{signalWindowDetail.windowMinutes} 分钟窗</Tag> : null}
                    >
                      {selectedSignalSample ? (
                        signalWindowDetail ? (
                          signalWindowDetail.sequenceRows.length ? (
                            <Space direction="vertical" size={12} className="full-width">
                              <Descriptions
                                column={1}
                                items={[
                                  { key: 'timeColumn', label: '时间字段', children: signalWindowDetail.timeColumn },
                                  { key: 'anchors', label: '同组锚点', children: signalWindowDetail.anchorColumns.length ? signalWindowDetail.anchorColumns.join(', ') : '无' },
                                  { key: 'windowStart', label: '窗口起点', children: signalWindowDetail.windowStart ?? '--' },
                                  { key: 'windowEnd', label: '窗口终点', children: signalWindowDetail.windowEnd ?? '--' },
                                  { key: 'totalRows', label: '窗口内事件数', children: signalWindowDetail.rows.length },
                                  { key: 'focusIndex', label: '当前样本位置', children: signalWindowDetail.focusLabel },
                                ]}
                              />
                              <Alert
                                type="info"
                                showIcon
                                message="下面展示当前样本前后各 2 条同组事件，帮助快速判断异常发生前后的上下文。"
                              />
                              <Table<Record<string, unknown>>
                                rowKey={(record) => String(record.__rowKey)}
                                columns={buildPreviewColumns(signalWindowDetail.sequenceColumns)}
                                dataSource={signalWindowDetail.sequenceRows}
                                pagination={{ pageSize: 6, hideOnSinglePage: true }}
                                scroll={{ x: 1000 }}
                                size="small"
                                rowClassName={(record) => String(record.__rowKey) === selectedSignalSampleKey ? 'selectable-row is-selected' : 'selectable-row'}
                              />
                            </Space>
                          ) : (
                            <Text type="secondary">当前预览范围内没有找到同时间窗、同分组的更多事件。</Text>
                          )
                        ) : (
                          <Text type="secondary">当前样本无法可靠推断时间窗明细，通常是因为缺少时间字段或窗口特征列没有携带时间窗信息。</Text>
                        )
                      ) : (
                        <Text type="secondary">先在上面的联动样本里选中一条异常记录，这里会展示它所在时间窗内的同组事件序列。</Text>
                      )}
                    </Card>
                  </Space>
                ) : (
                  <Alert type="info" showIcon message={props.analysisLoading ? '正在生成无监督结果图…' : '当前模型为无监督时，这里会展示异常分数和二维投影视图。'} />
                )
              ) : (
                <Alert type="info" showIcon message="当前模型是有监督模型，异常可视化主要在无监督场景下展示。" />
              )}
              <Card size="small" className="nested-card" title="Ollama 解释占位">
                {props.llmExplanation ? (
                  <Space direction="vertical" size={12} className="full-width">
                    <Space wrap>
                      <Tag color="cyan">{props.llmExplanation.provider}</Tag>
                      <Tag color="blue">{props.llmExplanation.model_name}</Tag>
                      <Tag color="purple">分析样本: {props.llmExplanation.analyzed_rows}</Tag>
                    </Space>
                    {props.llmExplanation.reasoning_content ? (
                      <Collapse
                        size="small"
                        items={[
                          {
                            key: 'reasoning',
                            label: '模型思考过程',
                            children: (
                              <Typography.Paragraph style={{ whiteSpace: 'pre-wrap', marginBottom: 0 }}>
                                {props.llmExplanation.reasoning_content}
                              </Typography.Paragraph>
                            ),
                          },
                        ]}
                      />
                    ) : null}
                    <div className="markdown-output">
                      <ReactMarkdown remarkPlugins={[remarkGfm]}>
                        {props.llmExplanation.final_content || props.llmExplanation.explanation}
                      </ReactMarkdown>
                    </div>
                    <Table<Record<string, unknown>>
                      rowKey={(_, index) => `llm-source-${index}`}
                      columns={buildPreviewColumns(props.llmExplanation.source_columns)}
                      dataSource={props.llmExplanation.source_rows}
                      pagination={{ pageSize: 5, hideOnSinglePage: true }}
                      scroll={{ x: 1000 }}
                      size="small"
                    />
                  </Space>
                ) : (
                  <Space><RobotOutlined /><Text type="secondary">保存配置后，可对当前模型的 Top 异常样本生成风险解释和排查建议。</Text></Space>
                )}
              </Card>
              <Table<Record<string, unknown>>
                rowKey={(_, index) => String(index)}
                loading={props.previewLoading}
                columns={buildPreviewColumns(props.preview?.columns ?? [])}
                dataSource={props.preview?.rows ?? []}
                pagination={{ pageSize: 5, hideOnSinglePage: true }}
                scroll={{ x: 1000 }}
                size="small"
              />
            </Space>
          ) : (
            <Empty description="选择一个模型后，这里会显示异常结果和 Ollama 分析入口。" />
          )}
        </DetailPanel>
      }
    />
  )
}

function renderMetricValue(value: number | null) {
  if (value === null || value === undefined) {
    return '--'
  }
  return value.toFixed(4)
}

function renderRateValue(value: number | null) {
  if (value === null || value === undefined) {
    return '--'
  }
  return `${(value * 100).toFixed(1)}%`
}

function toNumericValue(value: unknown) {
  if (value === null || value === undefined || value === '') {
    return null
  }
  const numeric = Number(value)
  return Number.isFinite(numeric) ? numeric : null
}

function buildSignalSampleKey(row: Record<string, unknown>, index: number) {
  const sampleIndex = row.sample_index
  if (sampleIndex !== null && sampleIndex !== undefined && sampleIndex !== '') {
    return `sample-${String(sampleIndex)}`
  }
  return `preview-${index}`
}

function buildSignalSampleContext(row: Record<string, unknown>, signalColumn: string) {
  const preferredColumns = [
    'event_time',
    'source_ip',
    'dest_ip',
    'host',
    'source_host',
    'dest_host',
    'process_name',
    'protocol',
    'method',
    'path',
    'status_code',
  ]
  const signalBase = inferSignalBase(signalColumn)
  const preferredSet = new Set(preferredColumns)
  const entries: Array<{ label: string; value: string }> = []

  for (const column of preferredColumns) {
    const value = row[column]
    if (hasDisplayValue(value)) {
      entries.push({ label: column, value: formatPreviewValue(value) })
    }
  }

  for (const [column, value] of Object.entries(row)) {
    if (entries.length >= 8) break
    if (preferredSet.has(column) || column.startsWith('__')) continue
    if (!hasDisplayValue(value)) continue
    if (signalBase && column.includes(signalBase)) {
      entries.push({ label: column, value: formatPreviewValue(value) })
    }
  }

  return entries.slice(0, 8)
}

function buildSignalSampleMatches(rows: Record<string, unknown>[], selectedRow: Record<string, unknown>) {
  const comparableColumns = ['source_ip', 'dest_ip', 'host', 'process_name', 'protocol', 'method', 'path', 'status_code']
  const summaries: Array<{ label: string; count: number }> = []

  for (const column of comparableColumns) {
    const selectedValue = selectedRow[column]
    if (!hasDisplayValue(selectedValue)) continue
    const count = rows.filter((row) => formatComparableValue(row[column]) === formatComparableValue(selectedValue)).length
    if (count > 1) {
      summaries.push({ label: `${column}=${formatPreviewValue(selectedValue)}`, count })
    }
  }

  return summaries.slice(0, 6)
}

function inferSignalBase(signalColumn: string) {
  return signalColumn
    .replace(/_(\d+)m_(unique_count|count|spike)$/i, '')
    .replace(/_(group_count|unique_count|count|spike)$/i, '')
}

function hasDisplayValue(value: unknown) {
  return value !== null && value !== undefined && String(value).trim() !== ''
}

function formatComparableValue(value: unknown) {
  if (!hasDisplayValue(value)) return ''
  return String(value).trim()
}

function formatPreviewValue(value: unknown) {
  if (!hasDisplayValue(value)) return '--'
  return String(value)
}

function buildSignalWindowDetail(
  rows: Record<string, unknown>[],
  selectedRow: SignalSampleRow,
  signalColumn: string,
) {
  const timeColumn = inferTimeColumn(selectedRow)
  const windowMinutes = inferWindowMinutes(signalColumn)
  if (!timeColumn || !windowMinutes) {
    return null
  }

  const selectedTime = parseDateValue(selectedRow[timeColumn])
  if (!selectedTime) {
    return null
  }

  const anchorColumns = inferAnchorColumns(rows, selectedRow, signalColumn)
  const windowStart = floorToWindow(selectedTime, windowMinutes)
  const windowEnd = new Date(windowStart.getTime() + windowMinutes * 60 * 1000)

  const filteredRows = rows
    .map<SignalSampleRow>((row, index) => ({ ...row, __rowKey: buildSignalSampleKey(row, index) }))
    .filter((row) => {
      const rowTime = parseDateValue(row[timeColumn])
      if (!rowTime) return false
      if (rowTime < windowStart || rowTime >= windowEnd) return false
      return anchorColumns.every((column) => formatComparableValue(row[column]) === formatComparableValue(selectedRow[column]))
    })
    .sort((left, right) => {
      const leftTime = parseDateValue(left[timeColumn])?.getTime() ?? 0
      const rightTime = parseDateValue(right[timeColumn])?.getTime() ?? 0
      return leftTime - rightTime
    })

  const columns = [
    timeColumn,
    ...anchorColumns,
    signalColumn,
    'anomaly_score',
    'predicted_label',
    'actual_label',
  ].filter((column, index, array) => array.indexOf(column) === index && rows.some((row) => Object.prototype.hasOwnProperty.call(row, column)))

  const selectedIndex = filteredRows.findIndex((row) => String(row.__rowKey) === String(selectedRow.__rowKey))
  const windowStartIndex = selectedIndex >= 0 ? Math.max(selectedIndex - 2, 0) : 0
  const windowEndIndex = selectedIndex >= 0 ? Math.min(selectedIndex + 3, filteredRows.length) : Math.min(filteredRows.length, 5)
  const sequenceRows = filteredRows.slice(windowStartIndex, windowEndIndex).map((row, index) => ({
    ...row,
    __eventRelation: buildEventRelation(windowStartIndex + index, selectedIndex),
  }))

  return {
    timeColumn,
    windowMinutes,
    anchorColumns,
    windowStart: formatWindowTime(windowStart),
    windowEnd: formatWindowTime(windowEnd),
    columns,
    sequenceColumns: ['__eventRelation', ...columns],
    sequenceRows,
    focusLabel: selectedIndex >= 0 ? `第 ${selectedIndex + 1} 条 / 共 ${filteredRows.length} 条` : '未定位',
    rows: filteredRows,
  }
}

function inferTimeColumn(row: Record<string, unknown>) {
  const preferred = ['event_time', 'timestamp', 'time', 'log_time']
  for (const column of preferred) {
    if (Object.prototype.hasOwnProperty.call(row, column) && parseDateValue(row[column])) {
      return column
    }
  }
  return Object.keys(row).find((column) => column.toLowerCase().includes('time') && parseDateValue(row[column])) ?? null
}

function inferWindowMinutes(signalColumn: string) {
  const match = signalColumn.match(/_(\d+)m_(?:unique_count|count|spike)$/i)
  if (!match) return null
  const minutes = Number(match[1])
  return Number.isFinite(minutes) && minutes > 0 ? minutes : null
}

function inferAnchorColumns(rows: Record<string, unknown>[], selectedRow: Record<string, unknown>, signalColumn: string) {
  const preferredColumns = ['source_ip', 'dest_ip', 'host', 'source_host', 'dest_host', 'process_name', 'protocol', 'method', 'path']
  const signalBase = inferSignalBase(signalColumn)
  const candidates = new Set<string>()

  preferredColumns.forEach((column) => {
    if (hasDisplayValue(selectedRow[column])) candidates.add(column)
  })

  Object.keys(selectedRow).forEach((column) => {
    if (signalBase && column.includes(signalBase) && hasDisplayValue(selectedRow[column])) {
      candidates.add(column)
    }
  })

  return Array.from(candidates)
    .map((column) => ({
      column,
      count: rows.filter((row) => formatComparableValue(row[column]) === formatComparableValue(selectedRow[column])).length,
    }))
    .filter((entry) => entry.count > 1)
    .sort((left, right) => right.count - left.count)
    .slice(0, 2)
    .map((entry) => entry.column)
}

function parseDateValue(value: unknown) {
  if (!hasDisplayValue(value)) return null
  const date = new Date(String(value))
  return Number.isNaN(date.getTime()) ? null : date
}

function floorToWindow(date: Date, windowMinutes: number) {
  const windowMs = windowMinutes * 60 * 1000
  return new Date(Math.floor(date.getTime() / windowMs) * windowMs)
}

function formatWindowTime(date: Date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}:${String(date.getSeconds()).padStart(2, '0')}`
}

function buildEventRelation(index: number, selectedIndex: number) {
  if (selectedIndex < 0) return `窗口事件 ${index + 1}`
  if (index === selectedIndex) return '当前样本'
  if (index < selectedIndex) return `前 ${selectedIndex - index} 条`
  return `后 ${index - selectedIndex} 条`
}
