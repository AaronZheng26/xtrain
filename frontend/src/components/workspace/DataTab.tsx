import { useState } from 'react'
import { Alert, Button, Card, Col, Descriptions, Empty, Input, List, Popconfirm, Row, Select, Space, Table, Tag, Typography, Upload } from 'antd'
import type { FormInstance } from 'antd'
import type { UploadFile } from 'antd/es/upload/interface'
import { CloudUploadOutlined, DatabaseOutlined, FileSearchOutlined } from '@ant-design/icons'

import { DetailPanel } from '../DetailPanel'
import { FieldMappingCard } from '../FieldMappingCard'
import { StageLayout } from '../StageLayout'
import { buildPreviewColumns, renderTagGroup } from '../../lib/ui'
import type { DatasetPreviewRead, DatasetVersion, FieldMapping, ImportSession, Project, SchemaField } from '../../types'

const { Text } = Typography

type Props = {
  project: Project | null
  datasets: DatasetVersion[]
  selectedDatasetId: number | null
  selectedDataset: DatasetVersion | null
  datasetPreview: DatasetPreviewRead | null
  fileList: UploadFile[]
  datasetsLoading: boolean
  previewLoading: boolean
  fieldMapping: FieldMapping | null
  mappingLoading: boolean
  savingMapping: boolean
  importSession: ImportSession | null
  creatingImportSession: boolean
  applyingImportCleaning: boolean
  confirmingImportSession: boolean
  deletingDatasetId: number | null
  mappingForm: FormInstance<Record<string, string | undefined>>
  onSelectDataset: (datasetId: number) => void
  onFileListChange: (files: UploadFile[]) => void
  onCreateImportSession: () => void
  onConfirmImportSession: () => void
  onSelectImportTemplate: (templateId: string) => void
  onApplyImportCleaning: (options: { include_columns?: string[]; exclude_columns?: string[]; rename_columns?: Record<string, string> }) => void
  onSaveFieldMapping: () => void
  onDeleteDataset: (datasetId: number) => void
}

const schemaColumns = [
  { title: '字段名', dataIndex: 'name', key: 'name', width: 160 },
  { title: '类型', dataIndex: 'dtype', key: 'dtype', width: 120 },
  { title: '空值', dataIndex: 'null_count', key: 'null_count', width: 90 },
  {
    title: '候选角色',
    dataIndex: 'candidate_roles',
    key: 'candidate_roles',
    render: (roles: string[]) =>
      roles.length ? roles.map((role) => <Tag key={role}>{role}</Tag>) : <Text type="secondary">无</Text>,
  },
]

export function DataTab(props: Props) {
  const datasetColumns = props.selectedDataset?.schema_snapshot.map((field) => field.name) ?? []
  const importColumns = props.importSession?.preview_schema.map((field) => field.name) ?? []
  const cleaningOptions = props.importSession?.cleaning_options ?? {}
  const includeColumns = Array.isArray(cleaningOptions.include_columns) ? cleaningOptions.include_columns as string[] : []
  const excludeColumns = Array.isArray(cleaningOptions.exclude_columns) ? cleaningOptions.exclude_columns as string[] : []
  const renameColumns = isRecord(cleaningOptions.rename_columns) ? cleaningOptions.rename_columns : {}
  const cleaningDraftKey = `${props.importSession?.id ?? 'none'}:${JSON.stringify(cleaningOptions)}`

  return (
    <StageLayout
      main={
        <Space direction="vertical" size={20} className="full-width">
          <Card title="数据集管理" extra={props.project ? <Tag color="processing">{props.project.name}</Tag> : null}>
            <Space direction="vertical" size={16} className="full-width">
              <Upload
                beforeUpload={() => false}
                maxCount={1}
                fileList={props.fileList}
                accept=".log,.csv,.xlsx"
                onChange={({ fileList: nextFileList }) => props.onFileListChange(nextFileList)}
              >
                <Button icon={<CloudUploadOutlined />}>选择日志文件</Button>
              </Upload>
              <Button
                type="primary"
                icon={<DatabaseOutlined />}
                loading={props.creatingImportSession}
                disabled={!props.project || !props.fileList.length}
                onClick={props.onCreateImportSession}
              >
                创建导入会话并预览
              </Button>
              {props.importSession ? (
                <Card size="small" className="nested-card" title="导入会话预览">
                  <Space direction="vertical" size={12} className="full-width">
                    <Alert
                      type="info"
                      showIcon
                      message="当前文件尚未生成正式数据版本。请确认模板和字段预览后，再点击确认生成。"
                    />
                    <Descriptions
                      column={1}
                      items={[
                        { key: 'file', label: '文件', children: props.importSession.file_name },
                        { key: 'rows', label: '预览行数', children: props.importSession.row_count },
                        { key: 'parser', label: '解析模板', children: props.importSession.parser_profile },
                      ]}
                    />
                    <Select
                      value={props.importSession.selected_template_id}
                      options={props.importSession.template_suggestions.map((template) => ({
                        label: `${template.name} (${template.parser_profile})`,
                        value: template.id,
                      }))}
                      onChange={props.onSelectImportTemplate}
                    />
                    <Space wrap>
                      {Object.entries(props.importSession.field_mapping).filter(([, value]) => value).slice(0, 8).map(([key, value]) => (
                        <Tag key={key} color="blue">{key}: {value}</Tag>
                      ))}
                    </Space>
                    <Card size="small" title="导入清洗" className="nested-card">
                      <ImportCleaningEditor
                        key={cleaningDraftKey}
                        includeColumns={includeColumns}
                        excludeColumns={excludeColumns}
                        renameColumns={renameColumns}
                        importColumns={importColumns}
                        applying={props.applyingImportCleaning}
                        onApply={props.onApplyImportCleaning}
                      />
                    </Card>
                    <Table<Record<string, unknown>>
                      rowKey={(_, index) => `import-preview-${index}`}
                      columns={buildPreviewColumns(Object.keys(props.importSession.preview_rows[0] ?? {}))}
                      dataSource={props.importSession.preview_rows}
                      pagination={{ pageSize: 5, hideOnSinglePage: true }}
                      scroll={{ x: 900 }}
                      size="small"
                    />
                    <Button type="primary" loading={props.confirmingImportSession} onClick={props.onConfirmImportSession}>
                      确认生成数据版本
                    </Button>
                  </Space>
                </Card>
              ) : null}
              <List
                loading={props.datasetsLoading}
                locale={{ emptyText: '当前项目还没有数据集。' }}
                dataSource={props.datasets}
                renderItem={(dataset) => (
                  <List.Item
                    className={dataset.id === props.selectedDatasetId ? 'selectable-row is-selected' : 'selectable-row'}
                    onClick={() => props.onSelectDataset(dataset.id)}
                  >
                    <List.Item.Meta
                      title={
                        <Space wrap>
                          <Text strong>{dataset.version_name}</Text>
                          <Tag color="blue">{dataset.parser_profile}</Tag>
                        </Space>
                      }
                      description={`共 ${dataset.row_count} 行，标签列：${dataset.label_column ?? '未识别'}`}
                    />
                    <Popconfirm
                      title="删除该数据集"
                      description="会同步删除该数据集的预处理、特征和模型产物。"
                      okText="确认删除"
                      cancelText="取消"
                      onConfirm={() => props.onDeleteDataset(dataset.id)}
                    >
                      <Button danger size="small" loading={props.deletingDatasetId === dataset.id}>
                        删除
                      </Button>
                    </Popconfirm>
                  </List.Item>
                )}
              />
            </Space>
          </Card>
          <FieldMappingCard
            dataset={props.selectedDataset}
            fieldMapping={props.fieldMapping}
            loading={props.mappingLoading}
            saving={props.savingMapping}
            columns={datasetColumns}
            form={props.mappingForm}
            onSave={props.onSaveFieldMapping}
          />
        </Space>
      }
      detail={
        <DetailPanel
          title="数据集详情"
          extra={props.selectedDataset ? <Tag color="purple">{props.selectedDataset.version_name}</Tag> : null}
          emptyDescription="选择或导入一个数据集后，这里会显示字段探测、样本预览和元信息。"
        >
          {props.selectedDataset ? (
            <Space direction="vertical" size={16} className="full-width">
              <Descriptions
                column={1}
                items={[
                  { key: 'parser', label: '解析模板', children: props.selectedDataset.parser_profile },
                  { key: 'rows', label: '数据行数', children: props.selectedDataset.row_count },
                  { key: 'label', label: '标签列', children: props.selectedDataset.label_column ?? '未识别' },
                  { key: 'path', label: '数据文件', children: props.selectedDataset.parquet_path },
                ]}
              />
              <Row gutter={[12, 12]}>
                <Col span={24}>
                  <Card size="small" title="时间字段候选" className="nested-card">
                    {renderTagGroup(props.selectedDataset.detected_fields.timestamp_candidates)}
                  </Card>
                </Col>
                <Col span={24}>
                  <Card size="small" title="标签字段候选" className="nested-card">
                    {renderTagGroup(props.selectedDataset.detected_fields.label_candidates)}
                  </Card>
                </Col>
              </Row>
              <div>
                <Space size={8} className="section-title">
                  <FileSearchOutlined />
                  <Text strong>字段探测</Text>
                </Space>
                <Table<SchemaField>
                  className="top-gap"
                  rowKey="name"
                  loading={props.previewLoading}
                  columns={schemaColumns}
                  dataSource={props.selectedDataset.schema_snapshot}
                  pagination={{ pageSize: 5, hideOnSinglePage: true }}
                  scroll={{ x: 720 }}
                  size="small"
                />
              </div>
              <Table<Record<string, unknown>>
                rowKey={(_, index) => String(index)}
                loading={props.previewLoading}
                columns={buildPreviewColumns(props.datasetPreview?.columns ?? [])}
                dataSource={props.datasetPreview?.rows ?? []}
                pagination={{ pageSize: 5, hideOnSinglePage: true }}
                scroll={{ x: 900 }}
                size="small"
              />
            </Space>
          ) : (
            <Empty description="选择或导入一个数据集后，这里会显示字段探测、样本预览和元信息。" />
          )}
        </DetailPanel>
      }
    />
  )
}

function toOption(column: string) {
  return { label: column, value: column }
}

type ImportCleaningEditorProps = {
  includeColumns: string[]
  excludeColumns: string[]
  renameColumns: Record<string, string>
  importColumns: string[]
  applying: boolean
  onApply: (options: { include_columns?: string[]; exclude_columns?: string[]; rename_columns?: Record<string, string> }) => void
}

function ImportCleaningEditor(props: ImportCleaningEditorProps) {
  const [draftIncludeColumns, setDraftIncludeColumns] = useState<string[]>(props.includeColumns)
  const [draftExcludeColumns, setDraftExcludeColumns] = useState<string[]>(props.excludeColumns)
  const [renameText, setRenameText] = useState(JSON.stringify(props.renameColumns, null, 2))
  const [renameError, setRenameError] = useState<string | null>(null)

  function handleApplyCleaningDraft() {
    const parsed = parseRenameJson(renameText)
    if (parsed === null) {
      setRenameError('字段重命名 JSON 不合法，请检查格式。')
      return
    }
    setRenameError(null)
    props.onApply({
      include_columns: draftIncludeColumns,
      exclude_columns: draftExcludeColumns,
      rename_columns: parsed,
    })
  }

  function handleResetCleaningDraft() {
    setDraftIncludeColumns(props.includeColumns)
    setDraftExcludeColumns(props.excludeColumns)
    setRenameText(JSON.stringify(props.renameColumns, null, 2))
    setRenameError(null)
  }

  return (
    <Space direction="vertical" size={12} className="full-width">
      <Alert
        type="info"
        showIcon
        message="大数据集建议先完成字段选择和重命名草稿，再统一点击“应用清洗”。这样可以减少频繁的后端重算。"
      />
      <div>
        <Text type="secondary">保留字段</Text>
        <Select
          mode="multiple"
          allowClear
          className="full-width top-gap"
          value={draftIncludeColumns}
          options={props.importColumns.map(toOption)}
          placeholder="为空表示保留所有字段"
          onChange={(columns) => setDraftIncludeColumns(columns)}
        />
      </div>
      <div>
        <Text type="secondary">剔除字段</Text>
        <Select
          mode="multiple"
          allowClear
          className="full-width top-gap"
          value={draftExcludeColumns}
          options={props.importColumns.map(toOption)}
          placeholder="选择不需要导入的字段"
          onChange={(columns) => setDraftExcludeColumns(columns)}
        />
      </div>
      <div>
        <Text type="secondary">重命名字段 JSON</Text>
        <Input.TextArea
          rows={3}
          className="top-gap"
          value={renameText}
          placeholder='例如 {"remote_addr":"source_ip","message":"raw_message"}'
          onChange={(event) => setRenameText(event.target.value)}
        />
        {renameError ? <Text type="danger">{renameError}</Text> : null}
      </div>
      <Space>
        <Button type="primary" loading={props.applying} onClick={handleApplyCleaningDraft}>
          应用清洗
        </Button>
        <Button onClick={handleResetCleaningDraft} disabled={props.applying}>
          重置草稿
        </Button>
      </Space>
    </Space>
  )
}

function isRecord(value: unknown): value is Record<string, string> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

function parseRenameJson(value: string): Record<string, string> | null {
  if (!value.trim()) return {}
  try {
    const parsed = JSON.parse(value)
    if (!isRecord(parsed)) return null
    return Object.fromEntries(Object.entries(parsed).filter(([, nextValue]) => typeof nextValue === 'string' && nextValue.trim()))
  } catch {
    return null
  }
}
