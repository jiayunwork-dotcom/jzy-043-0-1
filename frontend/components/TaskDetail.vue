<template>
  <div v-if="task" class="overlay" @click.self="$emit('close')">
    <div class="modal wide">
      <div class="modal-head">
        <div>
          <strong>任务详情</strong>
          <span class="mono muted" style="margin-left:10px">{{ task.id }}</span>
        </div>
        <button @click="$emit('close')">关闭</button>
      </div>

      <div class="grid" style="grid-template-columns:repeat(3,1fr);margin:14px 0">
        <div><div class="muted">类型</div><div>{{ task.type }}</div></div>
        <div><div class="muted">优先级</div><PriorityBadge :kind="task.priority" /></div>
        <div><div class="muted">状态</div><StateBadge :state="task.state" /></div>
        <div><div class="muted">Worker</div><div class="mono">{{ task.worker_id || '—' }}</div></div>
        <div><div class="muted">尝试次数</div><div>{{ task.attempt }}</div></div>
        <div><div class="muted">最大重试</div><div>{{ task.max_retries }}</div></div>
        <div v-if="task.dag_id">
          <div class="muted">DAG</div>
          <NuxtLink :to="`/dags/${task.dag_id}`">{{ task.dag_node }} @ {{ task.dag_id.slice(0,10) }}</NuxtLink>
        </div>
        <div v-if="task.callback_url"><div class="muted">回调</div><div class="mono">{{ task.callback_url }}</div></div>
        <div><div class="muted">超时</div><div>{{ task.timeout_seconds }}s</div></div>
      </div>

      <div class="muted">负载</div>
      <pre class="json">{{ pretty(task.payload) }}</pre>
      <div v-if="task.last_error" class="error-text" style="margin-top:6px">
        <strong>{{ task.error_type }}</strong>: {{ task.last_error }}
      </div>

      <h3 style="margin:18px 0 10px">执行时间线</h3>
      <div class="timeline">
        <div v-for="a in attempts" :key="a.id" class="tl-item">
          <div :class="['tl-dot', a.state]"></div>
          <div class="tl-body">
            <div>
              <strong>#{{ a.attempt_no + 1 }}</strong>
              <StateBadge :state="attemptLabel(a.state)" :label="attemptLabel(a.state)" />
              <span class="muted mono" style="margin-left:8px">{{ a.worker_id }}</span>
            </div>
            <div class="muted" style="font-size:12px;margin-top:3px">
              开始 {{ fmt(a.started_at) }}
              <template v-if="a.ended_at"> · 结束 {{ fmt(a.ended_at) }} · 耗时 {{ duration(a) }}ms</template>
            </div>
            <div v-if="a.error_message" class="error-text" style="font-size:12px;margin-top:3px">
              {{ a.error_type }}: {{ a.error_message }}
            </div>
          </div>
        </div>
        <div v-if="attempts.length===0" class="muted">尚无执行记录</div>
      </div>

      <h3 style="margin:18px 0 10px">审计日志</h3>
      <div class="card" style="padding:10px;max-height:200px;overflow:auto">
        <div v-for="e in audit" :key="e.id" class="mono" style="padding:3px 0;font-size:12px">
          <span class="muted">{{ fmt(e.created_at) }}</span>
          {{ e.event }}
          <span v-if="e.from_state || e.to_state" class="muted">
            {{ e.from_state }} → {{ e.to_state }}
          </span>
          <span class="muted">{{ e.detail }}</span>
        </div>
      </div>

      <div style="margin-top:16px;display:flex;gap:8px">
        <button v-if="canCancel" class="danger" @click="cancel">取消任务</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import type { Task, Attempt, AuditEvent } from '~/lib/api/types'
import { endpoints } from '~/lib/api/endpoints'
import { useToast } from '~/stores/toast'

const props = defineProps<{ taskId: string }>()
defineEmits<{ close: [] }>()
const toast = useToast()

const task = ref<Task | null>(null)
const attempts = ref<Attempt[]>([])
const audit = ref<AuditEvent[]>([])

const canCancel = computed(() => task.value && ['pending', 'ready'].includes(task.value.state))

async function load() {
  if (!props.taskId) {
    task.value = null
    return
  }
  const res = await endpoints.getTask(props.taskId)
  task.value = res.task
  attempts.value = res.attempts
  audit.value = res.audit
}
watch(() => props.taskId, load, { immediate: true })

async function cancel() {
  await toast.run('任务已取消', () => endpoints.cancelTask(props.taskId))
  load()
}

function pretty(v: unknown) { return JSON.stringify(v ?? {}, null, 2) }
function fmt(s: string) { return new Date(s).toLocaleString() }
function duration(a: Attempt) {
  if (!a.ended_at) return ''
  return Math.max(0, new Date(a.ended_at).getTime() - new Date(a.started_at).getTime())
}
function attemptLabel(s: string) {
  return { succeeded: '成功', failed: '失败', preempted: '被抢占', timeout: '超时', running: '执行中' }[s] || s
}
</script>

<style scoped>
.overlay { position: fixed; inset: 0; background: rgba(0,0,0,.6); display: flex; align-items: center; justify-content: center; z-index: 50; }
.modal { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; width: 640px; max-height: 88vh; overflow:auto; padding: 20px; }
.modal.wide { width: 820px; }
.modal-head { display:flex; justify-content:space-between; align-items:center; }
.json { background: var(--panel-2); border-radius:8px; padding:10px; font-size:12px; white-space:pre-wrap; margin:6px 0 0; }
.timeline { position: relative; padding-left: 8px; }
.tl-item { display:flex; gap:12px; padding-bottom:16px; position:relative; }
.tl-item:not(:last-child)::before { content:''; position:absolute; left:5px; top:16px; bottom:0; width:2px; background:var(--border); }
.tl-dot { width:12px;height:12px;border-radius:50%;margin-top:4px;z-index:1;flex:none;background:var(--muted); }
.tl-dot.succeeded { background: var(--ok); }
.tl-dot.failed { background: var(--err); }
.tl-dot.preempted { background: var(--high); }
.tl-dot.timeout { background: var(--warn); }
.tl-dot.running { background: var(--normal); }
</style>
