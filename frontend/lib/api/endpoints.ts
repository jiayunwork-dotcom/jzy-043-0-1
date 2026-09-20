import { api } from './client'
import type {
  MetricsSnapshot, SubmitTaskInput, Task, TaskDetailResp, TaskListResp,
  WorkerDetail, DeadStat, DAG,
} from './types'

export interface TaskQuery {
  state?: string
  priority?: string
  type?: string
  from?: string
  to?: string
  dag_id?: string
  limit?: number
  offset?: number
}

export const endpoints = {
  // metrics
  metrics: () => api.get<MetricsSnapshot>('/api/metrics'),
  eventsUrl: () => '/api/events',

  // tasks
  submitTask: (input: SubmitTaskInput) =>
    api.post<{ task: Task; idempotent_hit: boolean }>('/api/tasks', input),
  listTasks: (q: TaskQuery) => api.get<TaskListResp>('/api/tasks', q as Record<string, string | number>),
  getTask: (id: string) => api.get<TaskDetailResp>(`/api/tasks/${id}`),
  cancelTask: (id: string) => api.post(`/api/tasks/${id}/cancel`),
  taskTypes: () => api.get<{ types: string[] }>('/api/tasks/types'),

  // dead letter
  listDead: () =>
    api.get<{ tasks: Task[]; total: number; error_stats: DeadStat[] }>('/api/dead'),
  deadStats: () => api.get<{ stats: DeadStat[] }>('/api/dead/stats'),
  retryDead: (ids: string[]) => api.post<{ requeued: number }>('/api/dead/retry', { ids }),
  discardDead: (ids: string[]) => api.post<{ discarded: number }>('/api/dead/discard', { ids }),

  // workers
  listWorkers: () => api.get<{ workers: WorkerDetail[] }>('/api/workers'),
  drainWorker: (id: string) => api.post(`/api/workers/${id}/drain`),
  simulateLoss: (id: string) => api.post(`/api/workers/${id}/simulate-loss`),

  // dag
  listDAGs: () => api.get<{ dags: Omit<DAG, 'nodes'>[] }>('/api/dags'),
  getDAG: (id: string) =>
    api.get<{ dag: DAG; levels: string[][] }>(`/api/dags/${id}`),
  createDAG: (body: unknown) => api.post<{ dag: DAG }>('/api/dags', body),
  validateDAG: (body: unknown) =>
    api.post<{ valid: boolean; levels: string[][] }>('/api/dags/validate', body),

  // audit
  audit: (params: { task_id?: string; dag_id?: string; limit?: number } = {}) =>
    api.get<{ audit: import('./types').AuditEvent[] }>('/api/audit', params as Record<string, number>),
}
