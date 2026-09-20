// Live metrics store backed by the server-sent-events stream. Falls back to
// one-second polling if EventSource is unavailable.
import type { MetricsSnapshot } from '~/lib/api/types'
import { endpoints } from '~/lib/api/endpoints'

const empty: MetricsSnapshot = {
  now: '',
  queue_depth: {},
  delay_depth: 0,
  dead_backlog: 0,
  workers: { online: 0, draining: 0, offline: 0, active_slots: 0, total_slots: 0 },
  utilization: 0,
  throughput_per_sec: 0,
  success_rate: {},
  failure_rate: {},
  avg_exec_latency_ms: 0,
  avg_sched_latency_ms: 0,
  trend: [],
  dead_trend: [],
}

export function useMetricsStore() {
  const snapshot = useState<MetricsSnapshot>('metrics-snapshot', () => structuredClone(empty))
  const connected = useState<boolean>('metrics-connected', () => false)
  const lastUpdated = useState<number>('metrics-updated', () => 0)
  let es: EventSource | null = null
  let pollTimer: ReturnType<typeof setInterval> | null = null

  function apply(s: MetricsSnapshot) {
    snapshot.value = s
    lastUpdated.value = Date.now()
  }

  async function pollOnce() {
    try {
      apply(await endpoints.metrics())
      connected.value = true
    } catch {
      connected.value = false
    }
  }

  function start() {
    if (es || pollTimer) return
    if (typeof EventSource !== 'undefined') {
      es = new EventSource(endpoints.eventsUrl())
      es.onmessage = (ev) => {
        try {
          apply(JSON.parse(ev.data))
          connected.value = true
        } catch {
          /* ignore malformed frame */
        }
      }
      es.onerror = () => {
        connected.value = false
      }
    } else {
      pollOnce()
      pollTimer = setInterval(pollOnce, 1000)
    }
  }

  function stop() {
    es?.close()
    es = null
    if (pollTimer) clearInterval(pollTimer)
    pollTimer = null
  }

  return { snapshot, connected, lastUpdated, start, stop, refresh: pollOnce }
}
