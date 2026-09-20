// Lightweight global toast/notification store used by action buttons
// (batch retry, discard, drain, submit).
export interface Toast {
  id: number
  kind: 'success' | 'error' | 'info'
  message: string
}

const toasts = useState<Toast[]>('app-toasts', () => [])
let seq = 0

export function useToast() {
  function push(kind: Toast['kind'], message: string) {
    const id = ++seq
    toasts.value = [...toasts.value, { id, kind, message }]
    setTimeout(() => {
      toasts.value = toasts.value.filter((t) => t.id !== id)
    }, 4000)
  }
  return {
    toasts,
    success: (m: string) => push('success', m),
    error: (m: string) => push('error', m),
    info: (m: string) => push('info', m),
    run: async (msg: string, fn: () => Promise<unknown>) => {
      try {
        await fn()
        push('success', msg)
      } catch (e) {
        push('error', e instanceof Error ? e.message : String(e))
      }
    },
  }
}
