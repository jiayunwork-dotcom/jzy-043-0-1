<template>
  <h1 class="page-title">任务列表</h1>
  <p class="page-sub">按状态、优先级、类型与时间范围筛选，点击查看每次尝试的执行时间线</p>

  <div class="toolbar">
    <select v-model="filters.state">
      <option value="">全部状态</option>
      <option v-for="s in states" :key="s" :value="s">{{ stateLabels[s] }}</option>
    </select>
    <select v-model="filters.priority">
      <option value="">全部优先级</option>
      <option v-for="p in priorities" :key="p" :value="p">{{ p }}</option>
    </select>
    <input v-model="filters.type" placeholder="任务类型，逗号分隔" style="width:180px" />
    <input v-model="filters.from" type="datetime-local" />
    <input v-model="filters.to" type="datetime-local" />
    <button class="primary" @click="load">筛选</button>
    <button @click="reset">重置</button>
    <div class="spacer" />
    <button class="primary" @click="showSubmit = true">提交任务</button>
  </div>

  <div class="card">
    <table>
      <thead>
        <tr>
          <th>ID</th><th>类型</th><th>优先级</th><th>状态</th><th>尝试</th>
          <th>Worker</th><th>创建时间</th><th>错误类型</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="t in tasks" :key="t.id" class="clickable" @click="open(t.id)">
          <td class="mono">{{ short(t.id) }}</td>
          <td>{{ t.type }}</td>
          <td><PriorityBadge :kind="t.priority" /></td>
          <td><StateBadge :state="t.state" :label="stateLabels[t.state] || t.state" /></td>
          <td class="mono">{{ t.attempt }}</td>
          <td class="mono">{{ t.worker_id || '—' }}</td>
          <td class="muted">{{ fmt(t.created_at) }}</td>
          <td class="error-text">{{ t.error_type || '—' }}</td>
        </tr>
        <tr v-if="tasks.length === 0">
          <td colspan="8" class="muted" style="text-align:center;padding:30px">暂无任务</td>
        </tr>
      </tbody>
    </table>
    <div style="display:flex;justify-content:space-between;align-items:center;margin-top:14px">
      <span class="muted">共 {{ total }} 条</span>
      <div style="display:flex;gap:8px">
        <button :disabled="offset===0" @click="offset=Math.max(0,offset-limit);load()">上一页</button>
        <button :disabled="offset+limit>=total" @click="offset+=limit;load()">下一页</button>
      </div>
    </div>
  </div>

  <TaskDetail :task-id="detailId" @close="detailId=''" />
  <SubmitTaskModal v-if="showSubmit" @close="showSubmit=false" @submitted="onSubmitted" />
</template>

<script setup lang="ts">
import { reactive, ref, onMounted } from 'vue'
import type { Task } from '~/lib/api/types'
import { endpoints } from '~/lib/api/endpoints'
import TaskDetail from '~/components/TaskDetail.vue'
import SubmitTaskModal from '~/components/SubmitTaskModal.vue'

const states = ['pending', 'ready', 'running', 'succeeded', 'failed', 'dead', 'canceled']
const stateLabels: Record<string, string> = {
  pending: '等待(延迟)', ready: '就绪', running: '执行中', succeeded: '成功',
  failed: '失败重试中', dead: '死信', canceled: '已取消',
}
const priorities = ['critical', 'high', 'normal', 'low', 'bulk']

const tasks = ref<Task[]>([])
const total = ref(0)
const limit = ref(20)
const offset = ref(0)
const detailId = ref('')
const showSubmit = ref(false)

const filters = reactive({ state: '', priority: '', type: '', from: '', to: '' })

function iso(local: string) {
  return local ? new Date(local).toISOString() : undefined
}

async function load() {
  const res = await endpoints.listTasks({
    state: filters.state || undefined,
    priority: filters.priority || undefined,
    type: filters.type || undefined,
    from: iso(filters.from),
    to: iso(filters.to),
    limit: limit.value,
    offset: offset.value,
  })
  tasks.value = res.tasks
  total.value = res.total
}
function reset() {
  Object.assign(filters, { state: '', priority: '', type: '', from: '', to: '' })
  offset.value = 0
  load()
}
function open(id: string) { detailId.value = id }
function onSubmitted() { showSubmit.value = false; load() }
function short(id: string) { return id.slice(0, 13) }
function fmt(s: string) { return new Date(s).toLocaleString() }

onMounted(load)
</script>
