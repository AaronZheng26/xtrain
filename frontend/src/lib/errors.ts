import axios from 'axios'

export function extractApiErrorMessage(error: unknown, fallback: string) {
  if (axios.isAxiosError(error)) {
    const detail = error.response?.data?.detail
    if (typeof detail === 'string' && detail.trim()) {
      return detail
    }
    if (Array.isArray(detail) && detail.length > 0) {
      const firstItem = detail[0]
      if (typeof firstItem === 'string') {
        return firstItem
      }
      if (typeof firstItem === 'object' && firstItem !== null && 'msg' in firstItem && typeof firstItem.msg === 'string') {
        return firstItem.msg
      }
    }
    if (typeof error.message === 'string' && error.message.trim()) {
      return error.message
    }
  }

  if (error instanceof Error && error.message.trim()) {
    return error.message
  }

  return fallback
}
