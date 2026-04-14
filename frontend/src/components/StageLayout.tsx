import { Col, Row } from 'antd'
import type { ReactNode } from 'react'

type Props = {
  main: ReactNode
  detail: ReactNode
}

export function StageLayout({ main, detail }: Props) {
  return (
    <Row gutter={[20, 20]} className="stage-layout">
      <Col xs={24} xxl={15}>
        {main}
      </Col>
      <Col xs={24} xxl={9}>
        {detail}
      </Col>
    </Row>
  )
}
