import { useEffect, useState } from 'react'
import { Alert, Button, Card, Col, Form, Input, Layout, List, Popconfirm, Row, Space, Spin, Statistic, Tag, Timeline, Typography, message, Tabs } from 'antd'
import { ArrowRightOutlined, DeleteOutlined, FolderOpenOutlined, PlusOutlined } from '@ant-design/icons'
import { useNavigate } from 'react-router-dom'

import { StatusTrendChart } from '../components/StatusTrendChart'
import { WorkflowMap } from '../components/WorkflowMap'
import { api } from '../lib/api'
import type { DashboardSummaryRead, HealthRead, Project } from '../types'

const { Header, Content } = Layout
const { Title, Paragraph, Text } = Typography

type ProjectFormValues = {
  name: string
  description?: string
}

export function DashboardPage() {
  const [form] = Form.useForm<ProjectFormValues>()
  const [messageApi, contextHolder] = message.useMessage()
  const [summary, setSummary] = useState<DashboardSummaryRead | null>(null)
  const [health, setHealth] = useState<HealthRead | null>(null)
  const [loading, setLoading] = useState(true)
  const [submittingProject, setSubmittingProject] = useState(false)
  const [deletingProjectId, setDeletingProjectId] = useState<number | null>(null)
  const [errorMessage, setErrorMessage] = useState<string | null>(null)
  const navigate = useNavigate()

  async function loadDashboard() {
    try {
      const [summaryResponse, healthResponse] = await Promise.all([
        api.get<DashboardSummaryRead>('/dashboard/summary'),
        api.get<HealthRead>('/system/health'),
      ])
      setSummary(summaryResponse.data)
      setHealth(healthResponse.data)
      setErrorMessage(null)
    } catch {
      setErrorMessage('无法连接后端，请先启动 FastAPI 服务。')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void loadDashboard()
  }, [])

  async function handleCreateProject(values: ProjectFormValues) {
    setSubmittingProject(true)
    try {
      const response = await api.post<Project>('/projects', values)
      form.resetFields()
      await loadDashboard()
      navigate(`/projects/${response.data.id}`)
      messageApi.success('项目已创建。')
    } catch {
      setErrorMessage('创建项目失败，可能是名称重复或后端不可用。')
    } finally {
      setSubmittingProject(false)
    }
  }

  async function handleDeleteProject(projectId: number) {
    setDeletingProjectId(projectId)
    try {
      await api.delete(`/projects/${projectId}`)
      await loadDashboard()
      messageApi.success('项目已删除。')
    } catch {
      setErrorMessage('删除项目失败，请检查后端日志。')
    } finally {
      setDeletingProjectId(null)
    }
  }

  const latestJob = summary?.recent_jobs?.[0] ?? null
  const healthItems = health ? [
    { key: 'api', label: 'API', status: health.api.status, detail: health.api.detail },
    { key: 'sqlite', label: 'SQLite', status: health.sqlite.status, detail: health.sqlite.detail },
    { key: 'storage', label: 'Storage', status: health.storage.status, detail: health.storage.detail },
    { key: 'ollama', label: 'Ollama', status: health.ollama.status, detail: health.ollama.detail },
  ] : []

  return (
    <Layout className="app-shell dashboard-shell">
      {contextHolder}
      <Header className="app-header">
        <div>
          <Text className="header-label">首页</Text>
          <Title level={2}>项目总览与系统状态</Title>
        </div>
        <Button onClick={() => void loadDashboard()}>刷新总览</Button>
      </Header>
      <Content className="app-content">
        {errorMessage ? <Alert banner type="warning" message={errorMessage} /> : null}
        {loading ? (
          <div className="loading-state"><Spin size="large" /></div>
        ) : (
          <Space direction="vertical" size={20} className="content-stack">
            <Row gutter={[20, 20]} className="dashboard-hero-grid">
              <Col xs={24} xl={15}>
                <Card className="hero-card dashboard-hero-main">
                  <Text className="brand-eyebrow">xtrain</Text>
                  <Title level={3}>网络安全日志分析工作台</Title>
                  <Paragraph>
                    首页聚焦项目入口、系统状态和最近产物，真正的导入、处理、训练与异常分析都收敛在项目工作区里。
                  </Paragraph>
                  <div className="dashboard-chip-row">
                    <Tag color="blue">项目总览</Tag>
                    <Tag color="gold">分阶段工作区</Tag>
                    <Tag color="green">本地部署</Tag>
                    <Tag color="magenta">AI 异常分析</Tag>
                  </div>
                  <div className="dashboard-hero-actions">
                    <Button type="primary" icon={<PlusOutlined />} onClick={() => form.scrollToField('name')}>
                      创建项目
                    </Button>
                    <Button
                      icon={<ArrowRightOutlined />}
                      disabled={!summary?.recent_projects.length}
                      onClick={() => summary?.recent_projects[0] && navigate(`/projects/${summary.recent_projects[0].id}`)}
                    >
                      进入最近项目
                    </Button>
                  </div>
                  <div className="dashboard-metric-strip">
                    <div className="dashboard-metric-pill">
                      <Text className="dashboard-metric-label">项目</Text>
                      <Text strong>{summary?.project_count ?? 0}</Text>
                    </div>
                    <div className="dashboard-metric-pill">
                      <Text className="dashboard-metric-label">数据集</Text>
                      <Text strong>{summary?.dataset_count ?? 0}</Text>
                    </div>
                    <div className="dashboard-metric-pill">
                      <Text className="dashboard-metric-label">模型</Text>
                      <Text strong>{summary?.model_count ?? 0}</Text>
                    </div>
                    <div className="dashboard-metric-pill">
                      <Text className="dashboard-metric-label">任务</Text>
                      <Text strong>{summary?.job_count ?? 0}</Text>
                    </div>
                  </div>
                </Card>
              </Col>
              <Col xs={24} xl={9}>
                <Card title="系统快照" className="dashboard-snapshot-card">
                  <div className="dashboard-snapshot-stack">
                    <div className="dashboard-health-grid">
                      {healthItems.map((item) => (
                        <div key={item.key} className="dashboard-health-chip">
                          <Space size={8}>
                            <Tag color={item.status === 'up' ? 'green' : item.status === 'down' ? 'red' : 'gold'}>
                              {item.label}
                            </Tag>
                            <Text>{item.detail}</Text>
                          </Space>
                        </div>
                      ))}
                    </div>
                    <div className="dashboard-latest-job">
                      <Text className="dashboard-latest-job-label">最新任务</Text>
                      {latestJob ? (
                        <div>
                          <Space wrap>
                            <Text strong>{latestJob.name}</Text>
                            <Tag color={latestJob.status === 'completed' ? 'green' : latestJob.status === 'failed' ? 'red' : 'blue'}>
                              {latestJob.status}
                            </Tag>
                          </Space>
                          <Paragraph className="dashboard-latest-job-copy">
                            {latestJob.message || `当前进度 ${latestJob.progress}%`}
                          </Paragraph>
                        </div>
                      ) : (
                        <Paragraph className="dashboard-latest-job-copy">当前还没有任务记录。</Paragraph>
                      )}
                    </div>
                  </div>
                </Card>
              </Col>
            </Row>

            <Row gutter={[20, 20]}>
              <Col xs={24} md={12} xl={6}><Card className="metric-card"><Statistic title="项目数量" value={summary?.project_count ?? 0} /><Paragraph>按项目组织数据和模型版本。</Paragraph></Card></Col>
              <Col xs={24} md={12} xl={6}><Card className="metric-card"><Statistic title="数据集数量" value={summary?.dataset_count ?? 0} /><Paragraph>已导入数据版本数量。</Paragraph></Card></Col>
              <Col xs={24} md={12} xl={6}><Card className="metric-card"><Statistic title="模型数量" value={summary?.model_count ?? 0} /><Paragraph>历史训练输出的模型版本。</Paragraph></Card></Col>
              <Col xs={24} md={12} xl={6}><Card className="metric-card"><Statistic title="任务数量" value={summary?.job_count ?? 0} /><Paragraph>最近任务和训练状态。</Paragraph></Card></Col>
            </Row>

            <Row gutter={[20, 20]}>
              <Col xs={24} xl={15}>
                <Card title="项目入口" extra={<Text type="secondary">从这里进入各项目工作区</Text>}>
                  <div className="dashboard-create-strip">
                    <Form form={form} layout="vertical" onFinish={(values) => void handleCreateProject(values)}>
                      <Row gutter={16}>
                        <Col xs={24} md={10}><Form.Item label="项目名称" name="name" rules={[{ required: true, message: '请输入项目名称' }]}><Input placeholder="例如：NTA 异常识别" /></Form.Item></Col>
                        <Col xs={24} md={10}><Form.Item label="项目描述" name="description"><Input placeholder="写下数据来源、目标或当前分析主题" /></Form.Item></Col>
                        <Col xs={24} md={4} className="form-action"><Button type="primary" htmlType="submit" block loading={submittingProject} icon={<PlusOutlined />}>创建</Button></Col>
                      </Row>
                    </Form>
                  </div>
                  <List
                    dataSource={summary?.recent_projects ?? []}
                    locale={{ emptyText: '暂无项目，请先创建一个。' }}
                    renderItem={(project) => (
                      <List.Item
                        className="dashboard-project-row"
                        actions={[
                          <Button key="open" type="link" icon={<ArrowRightOutlined />} onClick={() => navigate(`/projects/${project.id}`)}>进入工作区</Button>,
                          <Popconfirm
                            key="delete"
                            title="删除该项目"
                            description="会同步删除该项目的数据集、流程版本和模型产物。"
                            okText="确认删除"
                            cancelText="取消"
                            onConfirm={() => void handleDeleteProject(project.id)}
                          >
                            <Button type="link" danger icon={<DeleteOutlined />} loading={deletingProjectId === project.id}>
                              删除
                            </Button>
                          </Popconfirm>,
                        ]}
                      >
                        <List.Item.Meta
                          avatar={<FolderOpenOutlined className="dashboard-list-icon" />}
                          title={<Space wrap><Text strong>{project.name}</Text><Tag color="green">{project.status}</Tag></Space>}
                          description={
                            <div className="dashboard-project-meta">
                              <Text>{project.description || '未填写描述'}</Text>
                              <Text type="secondary">创建于 {formatDateTime(project.created_at)}</Text>
                            </div>
                          }
                        />
                      </List.Item>
                    )}
                  />
                </Card>
              </Col>
              <Col xs={24} xl={9}>
                <Card title="最近任务" className="dashboard-side-card">
                  <StatusTrendChart jobs={summary?.recent_jobs ?? []} />
                </Card>
                <Card title="工作流概览" className="workflow-top-card dashboard-side-card top-gap">
                  <WorkflowMap />
                </Card>
              </Col>
            </Row>

            <Row gutter={[20, 20]}>
              <Col xs={24}>
                <Card title="最近情报" extra={<Text type="secondary">快速回到最新数据与模型</Text>}>
                  <Tabs
                    items={[
                      {
                        key: 'datasets',
                        label: '最近数据集',
                        children: (
                          <List
                            className="dashboard-compact-list"
                            dataSource={summary?.recent_datasets ?? []}
                            locale={{ emptyText: '暂无数据集。' }}
                            renderItem={(dataset) => (
                              <List.Item actions={[<Button key="open" type="link" onClick={() => navigate(`/projects/${dataset.project_id}?tab=data`)}>查看项目</Button>]}>
                                <List.Item.Meta
                                  title={<Space wrap><Text strong>{dataset.version_name}</Text><Tag color="blue">{dataset.parser_profile}</Tag></Space>}
                                  description={`项目 ${dataset.project_id} · ${dataset.row_count} 行 · ${formatDateTime(dataset.created_at)}`}
                                />
                              </List.Item>
                            )}
                          />
                        ),
                      },
                      {
                        key: 'models',
                        label: '最近模型',
                        children: (
                          <List
                            className="dashboard-compact-list"
                            dataSource={summary?.recent_models ?? []}
                            locale={{ emptyText: '暂无模型版本。' }}
                            renderItem={(model) => (
                              <List.Item actions={[<Button key="open" type="link" onClick={() => navigate(`/projects/${model.project_id}?tab=training`)}>查看项目</Button>]}>
                                <List.Item.Meta
                                  title={<Space wrap><Text strong>{model.name}</Text><Tag color={model.mode === 'supervised' ? 'green' : 'magenta'}>{model.mode}</Tag></Space>}
                                  description={`${model.algorithm} · ${model.status} · ${formatDateTime(model.created_at)}`}
                                />
                              </List.Item>
                            )}
                          />
                        ),
                      },
                      {
                        key: 'health',
                        label: '系统检查',
                        children: (
                          <Timeline
                            items={healthItems.map((item) => ({
                              color: item.status === 'up' ? 'green' : item.status === 'down' ? 'red' : 'gold',
                              children: `${item.label}: ${item.detail}`,
                            }))}
                          />
                        ),
                      },
                    ]}
                  />
                </Card>
              </Col>
            </Row>
          </Space>
        )}
      </Content>
    </Layout>
  )
}

function formatDateTime(value: string) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }

  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date)
}
