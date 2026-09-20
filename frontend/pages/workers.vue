<template>
  <h1 class="page-title">Worker 集群</h1>
  <p class="page-sub">在线状态、心跳、正在执行的任务与历史统计；失联任务由引擎自动回收重入队</p>

  <div class="toolbar">
    <button @click="load">刷新</button>
    <span class="spacer" />
      <span class="muted">自动每 2 秒刷新</span>
  </div>

  <div class="grid" style="display:flex;flex-direction:column;gap:14px">
    <div v-for="d in details" :key="d.worker.id" class="card">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <div>
          <strong style="font-size:16px">{{ d.worker.id }}</strong>
          <span class="badge" :class="`st-${d.worker.status}`" style="margin-left:10px">
            {{ statusLabel(d.worker.status) }}
          </span>
        </div>
        <div style="display:flex;gap:8px">
          <button @click="drain(d.worker.id)" :disabled="d.worker.status!=='online'">优雅关闭</button>
          <button class="danger" @click="simulateLoss(d.worker.id)" :disabled="d.worker.status!=='online'">
            模拟失联
          </button>
        </div>
      </div>

      <div class="grid" style="grid-template-columns:repeat(5,1fr);margin-top:14px">
        <div><div class="muted">槽位</div><strong>{{ d.worker.active_slots }}/{{ d.worker.total_slots }}</strong></div>
        <div><div class="muted">最后心跳</div><div class="mono" :class="heartbeatStale(d.worker.last_heartbeat) ? 'error-text':''">
          {{ relHeartbeat(d.worker.last_heartbeat) }}
        </div></div>
        <div><div class="muted">成功</div><strong>{{ d.worker.succeeded }}</strong></div>
        <div><div class="muted">失败</div><strong>{{ d.worker.failed }}</strong></div>
        <div><div class="muted">被抢占</div><strong>{{ d.worker.preempted }}</strong></div>
      </div>

      <div style="margin-top:12px">
        <div class="stat-label" style="margin-bottom:6px">正在执行（{{ d.running.length }}）</div>
        <table v-if="d.running.length">
          <thead><tr><th>任务</th><th>类型</th><th>优先级</th><th>尝试</th><th>开始时间</th></tr></thead>
          <tbody>
            <tr v-for="t in d.running" :key="t.id" class="clickable" @click="openTask=t.id">
              <td class="mono">{{ t.id.slice(0,13) }}</td>
              <td>{{ t.type }}</td>
              <td><PriorityBadge :kind="t.priority" /></td>
              <td class="mono">{{ t.attempt }}</td>
              <td class="muted">{{ fmt(t.started_at) }}</td>
            </tr>
          </tbody>
        </table>
        <div v-else class="muted">空闲</div>
      </div>
    </div>
  </div>

  <TaskDetail :task-id="openTask" @close="openTask=''" />
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import type { WorkerDetail } from '~/lib/api/types'
import { endpoints } from '~/lib/api/endpoints'
import { useToast } from '~/stores/toast'
import TaskDetail from '~/components/TaskDetail.vue'

const details = ref<WorkerDetail[]>([])
const openTask = ref('')
const toast = useToast()
let timer: ReturnType<typeof setInterval> | null = null

async function load() {
  const res = await endpoints.listWorkers()
  details.value = res.workers
}
async function drain(id: string) {
  await toast.run(`${id} 进入优雅关闭，不再接收新任务`, () => endpoints.drainWorker(id))
  load()
}
async function simulateLoss(id: string) {
  if (!confirm(`模拟 ${id} 心跳超时？其在执行任务将被回收重新入队。`)) return
  await toast.run(`${id} 已被标记为失联（等待回收）`, () => endpoints.simulateLoss(id))
  load()
}
function statusLabel(s: string) {
  return { online: '在线', draining: '排空中', offline: '离线' }[s] || s
}
function fmt(s?: string) { return s ? new Date(s).toLocaleString() : '—' }
function relHeartbeat(s: string) {
  const secs = Math.round((Date.now() - new Date(s).getTime()) / 1000)
  if (secs < 5) return '刚刚'
  if (secs < 60) return `${secs} 秒前`
  return `${Math.round(secs / 60)} 分钟前`
}
function heartbeatStale(s: string) {
  return Date.now() - new Date(s).getTime() > 15_000
}

onMounted(() => { load(); timer = setInterval(load, 2000) })
onUnmounted(() => timer && clearInterval(timer))
</script>
