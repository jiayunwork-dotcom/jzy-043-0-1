<template>
  <h1 class="page-title">死信队列</h1>
  <p class="page-sub">重试耗尽的任务，支持单条/批量重新入队、批量丢弃与按错误类型聚合</p>

  <div class="grid" style="grid-template-columns:1fr 1fr;margin-bottom:16px">
    <div class="card">
      <div class="stat-label" style="margin-bottom:10px">按错误类型聚合</div>
      <div v-for="s in stats" :key="s.error_type" style="margin-bottom:8px">
        <div style="display:flex;justify-content:space-between">
          <span class="error-text mono">{{ s.error_type }}</span>
          <strong>{{ s.count }}</strong>
        </div>
        <div class="bar" style="margin-top:4px">
          <span :style="{ width: pct(s.count) + '%', background: 'var(--err)' }" />
        </div>
      </div>
      <div v-if="stats.length===0" class="muted">暂无死信 🎉</div>
    </div>
    <MetricCard label="死信任务总数" :value="total" hint="批量重试会按队首重新入就绪队列" />
  </div>

  <div class="toolbar">
    <button class="primary" :disabled="selected.length===0" @click="retrySelected">
      批量重试 ({{ selected.length }})
    </button>
    <button class="danger" :disabled="selected.length===0" @click="discardSelected">
      批量丢弃 ({{ selected.length }})
    </button>
    <div class="spacer" />
    <button @click="load">刷新</button>
  </div>

  <div class="card">
    <table>
      <thead>
        <tr>
          <th style="width:36px"><input type="checkbox" :checked="allSelected" @change="toggleAll" /></th>
          <th>ID</th><th>类型</th><th>优先级</th><th>错误类型</th><th>错误信息</th>
          <th>尝试</th><th>失败时间</th><th></th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="t in tasks" :key="t.id">
          <td><input type="checkbox" :checked="selected.includes(t.id)" @change="toggle(t.id)" /></td>
          <td class="mono">{{ t.id.slice(0,13) }}</td>
          <td>{{ t.type }}</td>
          <td><PriorityBadge :kind="t.priority" /></td>
          <td class="error-text">{{ t.error_type || '—' }}</td>
          <td class="muted" style="max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
            {{ t.last_error }}
          </td>
          <td class="mono">{{ t.attempt }}</td>
          <td class="muted">{{ fmt(t.finished_at) }}</td>
          <td style="white-space:nowrap">
            <button @click="openDetail=t.id">详情</button>
            <button class="primary" style="margin-left:6px" @click="retryOne(t.id)">重试</button>
          </td>
        </tr>
        <tr v-if="tasks.length===0">
          <td colspan="9" class="muted" style="text-align:center;padding:30px">死信区为空</td>
        </tr>
      </tbody>
    </table>
  </div>

  <TaskDetail :task-id="openDetail" @close="openDetail=''" />
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import type { Task, DeadStat } from '~/lib/api/types'
import { endpoints } from '~/lib/api/endpoints'
import { useToast } from '~/stores/toast'
import MetricCard from '~/components/MetricCard.vue'
import TaskDetail from '~/components/TaskDetail.vue'

const tasks = ref<Task[]>([])
const stats = ref<DeadStat[]>([])
const total = ref(0)
const selected = ref<string[]>([])
const openDetail = ref('')
const toast = useToast()

const allSelected = computed(() => tasks.value.length > 0 && selected.value.length === tasks.value.length)

async function load() {
  const res = await endpoints.listDead()
  tasks.value = res.tasks
  total.value = res.total
  stats.value = res.error_stats
  selected.value = []
}
function toggle(id: string) {
  selected.value = selected.value.includes(id)
    ? selected.value.filter((x) => x !== id)
    : [...selected.value, id]
}
function toggleAll() {
  selected.value = allSelected.value ? [] : tasks.value.map((t) => t.id)
}
async function retrySelected() {
  const ids = selected.value
  await toast.run(`已重新入队`, async () => {
    const r = await endpoints.retryDead(ids)
    toast.info(`重新入队 ${r.requeued} 条`)
    await load()
  })
}
async function discardSelected() {
  const ids = selected.value
  if (!confirm(`确认丢弃 ${ids.length} 条死信任务？`)) return
  await toast.run('已丢弃', async () => {
    const r = await endpoints.discardDead(ids)
    toast.info(`丢弃 ${r.discarded} 条`)
    await load()
  })
}
async function retryOne(id: string) {
  await toast.run('任务已重新入队', async () => {
    await endpoints.retryDead([id])
    await load()
  })
}
function pct(n: number) {
  const max = Math.max(1, ...stats.value.map((s) => s.count))
  return Math.round((n / max) * 100)
}
function fmt(s?: string) { return s ? new Date(s).toLocaleString() : '—' }

onMounted(load)
</script>
