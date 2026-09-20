// Endpoint wrappers grouped by domain. Types mirror the backend DTOs.

export type Priority = 'critical' | 'high' | 'normal' | 'low' | 'bulk'
export type TaskState = 'pending' | 'ready' | 'running' | 'succeeded' | 'failed' | 'dead' | 'canceled'

export interface RetryPolicy {
  kind: 'exponential' | 'fixed' | 'cron'
  base_interval_seconds: number
  cron: string
  max_retries: number
}

export interface Task {
  id: string
  type: string
  payload: unknown
  priority: Priority
  state: TaskState
  attempt: number
  max_retries: number
  callback_url?: string
  last_error?: string
  error_type?: string
  worker_id?: string
  dag_id?: string
  dag_node?: string
  run_at?: string
  ready_at: string
  started_at?: string
  finished_at?: string
  created_at: string
  timeout_seconds: number
  retry_policy: RetryPolicy
}

export interface Attempt {
  id: string
  attempt_no: number
  worker_id: string
  state: 'running' | 'succeeded' | 'failed' | 'preempted' | 'timeout'
  started_at: string
  ended_at?: string
  error_type?: string
  error_message?: string
}

export interface AuditEvent {
  id: number
  task_id: string
  dag_id: string
  worker_id: string
  event: string
  from_state: string
  to_state: string
  detail: string
  created_at: string
}

export interface TaskListResp {
  tasks: Task[]
  total: number
}

export interface TaskDetailResp {
  task: Task
  attempts: Attempt[]
  audit: AuditEvent[]
}

export interface MetricsSnapshot {
  now: string
  queue_depth: Record<string, number>
  delay_depth: number
  dead_backlog: number
  workers: { online: number; draining: number; offline: number; active_slots: number; total_slots: number }
  utilization: number
  throughput_per_sec: number
  success_rate: Record<string, number>
  failure_rate: Record<string, number>
  avg_exec_latency_ms: number
  avg_sched_latency_ms: number
  trend: { ts: number; completed: number; succeeded: number; failed: number; dead: number }[]
  dead_trend: { ts: number; dead: number }[]
}

export interface Worker {
  id: string
  total_slots: number
  active_slots: number
  status: 'online' | 'draining' | 'offline'
  last_heartbeat: string
  started_at: string
  succeeded: number
  failed: number
  preempted: number
}

export interface WorkerDetail {
  worker: Worker
  running: Task[]
}

export interface DeadStat {
  error_type: string
  count: number
}

export type NodeState = 'pending' | 'ready' | 'running' | 'succeeded' | 'failed' | 'skipped'

export interface DAGNodeDef {
  id: string
  task_type: string
  payload: unknown
  priority: Priority
  timeout: number
  depends_on: string[]
  on_failure: 'abort' | 'skip' | 'retry'
  max_retries: number
  retries_used?: number
}

export interface DAGNode {
  dag_id: string
  node_id: string
  task_id: string
  state: NodeState
  depends_on: string[]
  def: DAGNodeDef
  started_at?: string
  finished_at?: string
  error?: string
}

export interface DAG {
  id: string
  name: string
  status: 'pending' | 'running' | 'succeeded' | 'failed' | 'aborted'
  failure_policy: 'abort' | 'skip' | 'retry'
  created_at: string
  finished_at?: string
  nodes: DAGNode[]
}

export interface SubmitTaskInput {
  type: string
  payload: Record<string, unknown>
  priority: Priority
  delay_seconds?: number
  timeout_seconds?: number
  max_retries?: number
  retry_policy?: { kind: string; base_interval_seconds: number; cron?: string; max_retries: number }
  callback_url?: string
  idempotency_key?: string
}
