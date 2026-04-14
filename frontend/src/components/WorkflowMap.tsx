import { Background, Controls, MiniMap, ReactFlow, type Edge, type Node } from '@xyflow/react'
import '@xyflow/react/dist/style.css'

const nodes: Node[] = [
  {
    id: 'import',
    position: { x: 0, y: 40 },
    data: { label: '数据导入' },
    style: nodeStyle('#1677ff'),
  },
  {
    id: 'preprocess',
    position: { x: 200, y: 40 },
    data: { label: '预处理' },
    style: nodeStyle('#faad14'),
  },
  {
    id: 'feature',
    position: { x: 400, y: 40 },
    data: { label: '特征提取' },
    style: nodeStyle('#13c2c2'),
  },
  {
    id: 'training',
    position: { x: 600, y: 40 },
    data: { label: '训练/重训练' },
    style: nodeStyle('#52c41a'),
  },
  {
    id: 'evaluation',
    position: { x: 820, y: 10 },
    data: { label: '效果评估' },
    style: nodeStyle('#722ed1'),
  },
  {
    id: 'analysis',
    position: { x: 820, y: 110 },
    data: { label: '异常分析 + Ollama' },
    style: nodeStyle('#eb2f96'),
  },
]

const edges: Edge[] = [
  { id: 'e1', source: 'import', target: 'preprocess', animated: true },
  { id: 'e2', source: 'preprocess', target: 'feature', animated: true },
  { id: 'e3', source: 'feature', target: 'training', animated: true },
  { id: 'e4', source: 'training', target: 'evaluation', animated: true },
  { id: 'e5', source: 'training', target: 'analysis', animated: true },
]

function nodeStyle(color: string) {
  return {
    borderRadius: 18,
    padding: 12,
    border: `1px solid ${color}`,
    background: '#ffffff',
    boxShadow: `0 12px 32px ${color}22`,
    minWidth: 132,
    textAlign: 'center' as const,
    fontWeight: 600,
  }
}

export function WorkflowMap() {
  return (
    <div style={{ width: '100%', height: 320 }}>
      <ReactFlow nodes={nodes} edges={edges} fitView nodesDraggable={false} zoomOnScroll={false}>
        <MiniMap pannable zoomable />
        <Controls showInteractive={false} />
        <Background />
      </ReactFlow>
    </div>
  )
}
