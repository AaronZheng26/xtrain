import { useEffect } from 'react'
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

export function AnalysisTab(props: Props) {
  const [configForm] = Form.useForm<LlmProviderConfigPayload & { topK: number; apiKey: string }>()

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
                  <Card size="small" className="nested-card" title="异常可视化">
                    <Space direction="vertical" size={12} className="full-width">
                      <Space wrap>
                        <Tag color="red">异常样本: {props.analysis.anomaly_count}</Tag>
                        <Tag color="geekblue">投影点数: {props.analysis.sample_size}</Tag>
                      </Space>
                      <UnsupervisedResultCharts analysis={props.analysis} />
                    </Space>
                  </Card>
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
