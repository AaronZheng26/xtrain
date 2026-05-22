import { Breadcrumb, Button, Popconfirm, Progress, Space, Tag, Typography } from 'antd'
import { ArrowLeftOutlined, DeleteOutlined, LineChartOutlined } from '@ant-design/icons'
import { Link } from 'react-router-dom'

import type { Job, Project, WorkspaceTabKey } from '../types'

const { Title, Text } = Typography

type Props = {
  project: Project | null
  datasetLabel?: string
  latestJob: Job | null
  activeTab: WorkspaceTabKey
  onRefresh: () => void
  onDeleteProject: () => void
  deletingProject: boolean
}

const tabLabels: Record<WorkspaceTabKey, string> = {
  data: '数据',
  preprocess: '预处理',
  feature: '特征',
  training: '训练',
  analysis: '分析',
}

export function WorkspaceHeader({ project, datasetLabel, latestJob, activeTab, onRefresh, onDeleteProject, deletingProject }: Props) {
  const latestJobLabel = latestJob ? humanizeJobStatus(latestJob) : null

  return (
    <div className="workspace-header">
      <div>
        <Breadcrumb
          items={[
            { title: <Link to="/">首页</Link> },
            { title: project?.name ?? '项目工作区' },
            { title: tabLabels[activeTab] },
          ]}
        />
        <Space className="top-gap" size={12} wrap>
          <Title level={2} className="workspace-title">
            {project?.name ?? '项目工作区'}
          </Title>
          {datasetLabel ? <Tag color="processing">{datasetLabel}</Tag> : null}
        </Space>
        <Text type="secondary">{project?.description || '在这个工作区里按阶段完成数据处理、训练和结果分析。'}</Text>
      </div>
      <div className="workspace-actions">
        {latestJob ? (
          <div className="workspace-job-card">
            <Space direction="vertical" size={4} style={{ width: '100%' }}>
              <Space>
                <LineChartOutlined />
                <Text strong>{latestJob.name}</Text>
                <Tag color={jobStatusColor(latestJob.status)}>{latestJobLabel}</Tag>
              </Space>
              <Progress percent={latestJob.progress} size="small" />
              <Text type="secondary">{latestJob.message}</Text>
            </Space>
          </div>
        ) : null}
        <Space>
          <Button icon={<ArrowLeftOutlined />}>
            <Link to="/">返回首页</Link>
          </Button>
          <Button onClick={onRefresh}>刷新工作区</Button>
          <Popconfirm
            title="删除当前项目"
            description="会一并删除该项目下的数据集、处理中间产物和模型文件。"
            okText="确认删除"
            cancelText="取消"
            onConfirm={onDeleteProject}
          >
            <Button danger icon={<DeleteOutlined />} loading={deletingProject}>
              删除项目
            </Button>
          </Popconfirm>
        </Space>
      </div>
    </div>
  )
}

function humanizeJobStatus(job: Job) {
  if (job.status === 'queued') return '等待执行'
  if (job.status === 'completed' || job.status === 'success') return '分析完成'
  if (job.status === 'failed') return '分析失败'
  if (job.status === 'running') {
    const runningLabels: Record<string, string> = {
      import: '正在读取日志',
      mapping: '正在识别字段',
      preprocess: '正在整理数据',
      preprocess_training_advisor: '正在分析训练影响',
      feature: '正在生成异常分析特征',
      training: '正在建立检测模型',
      analysis: '正在生成异常分数',
    }
    return runningLabels[job.job_type] ?? '正在执行分析任务'
  }
  return job.status
}

function jobStatusColor(status: string) {
  if (status === 'completed' || status === 'success') return 'green'
  if (status === 'failed') return 'red'
  if (status === 'queued') return 'gold'
  return 'processing'
}
