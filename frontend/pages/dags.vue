<template>
  <h1 class="page-title">DAG 编排</h1>
  <p class="page-sub">以层级列表呈现节点前驱后继关系与执行状态，支持定义并提交新 DAG（提交前环检测）</p>

  <div class="toolbar">
    <button class="primary" @click="showCompose=true">定义并提交 DAG</button>
    <div class="spacer" />
    <button @click="load">刷新</button>
  </div>

  <div class="grid" style="grid-template-columns:380px 1fr;gap:16px">
    <div class="card">
      <h3 style="margin:0 0 12px">DAG 实例</h3>
      <div
        v-for="d in dags" :key="d.id"
        :class="['dag-row', selected===d.id ? 'active' : '']"
        @click="selected=d.id; loadDAG()"
      >
        <div style="display:flex;justify-content:space-between;align-items:center">
          <strong>{{ d.name }}</strong>
          <span class="badge" :class="dagBadge(d.status)">{{ dagLabel(d.status) }}</span>
        </div>
        <div class="muted mono" style="font-size:11px;margin-top:4px">{{ d.id.slice(0,18) }} · {{ fmt(d.created_at) }}</div>
      </div>
      <div v-if="dags.length===0" class="muted">还没有 DAG 实例</div>
    </div>

    <div class="card" v-if="dag">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <h3 style="margin:0">{{ dag.name }}
          <span class="badge" :class="dagBadge(dag.status)" style="margin-left:8px">{{ dagLabel(dag.status) }}</span>
        </h3>
        <span class="muted mono">{{ dag.id }}</span>
      </div>
      <div class="muted" style="margin:6px 0 16px">失败处置：{{ failLabel(dag.failure_policy) }}</div>

      <div v-for="(level, li) in levels" :key="li" class="level">
        <div class="muted" style="font-size:12px;margin-bottom:6px">第 {{ li + 1 }} 层</div>
        <div class="level-nodes">
          <div v-for="nodeId in level" :key="nodeId" :class="['node-card', node(nodeId)?.state]">
            <div class="node-head">
              <strong>{{ nodeId }}</strong>
              <span class="badge" :class="`st-${node(nodeId)?.state}-node`">{{ nodeLabel(node(nodeId)?.state) }}</span>
            </div>
            <div class="muted" style="font-size:12px;margin-top:4px">
              {{ node(nodeId)?.def.task_type }}
              <PriorityBadge :kind="node(nodeId)?.def.priority || 'normal'" />
            </div>
            <div class="muted" style="font-size:11px;margin-top:4px">
              前驱：{{ (node(nodeId)?.depends_on || []).join(', ') || '（根节点）' }}
            </div>
            <div class="muted" style="font-size:11px">
              后继：{{ successors(nodeId).join(', ') || '（末端）' }}
            </div>
            <div v-if="node(nodeId)?.task_id" class="muted mono" style="font-size:11px;margin-top:4px">
              <NuxtLink :to="`/tasks?dag_id=${dag.id}`">{{ node(nodeId)?.task_id.slice(0,13) }}</NuxtLink>
            </div>
            <div v-if="node(nodeId)?.error" class="error-text" style="font-size:11px;margin-top:4px">
              {{ node(nodeId)?.error }}
            </div>
          </div>
        </div>
      </div>
    </div>
    <div class="card muted" v-else>选择左侧一个 DAG 查看其依赖层级</div>
  </div>

  <DAGComposer v-if="showCompose" @close="showCompose=false" @created="onCreated" />
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import type { DAG, DAGNode } from '~/lib/api/types'
import { endpoints } from '~/lib/api/endpoints'
import DAGComposer from '~/components/DAGComposer.vue'

const dags = ref<Omit<DAG, 'nodes'>[]>([])
const dag = ref<DAG | null>(null)
const levels = ref<string[][]>([])
const selected = ref('')
const showCompose = ref(false)
let timer: ReturnType<typeof setInterval> | null = null

async function load() {
  const res = await endpoints.listDAGs()
  dags.value = res.dags
}
async function loadDAG() {
  if (!selected.value) return
  const res = await endpoints.getDAG(selected.value)
  dag.value = res.dag
  levels.value = res.levels
}
function node(id: string): DAGNode | undefined {
  return dag.value?.nodes.find((n) => n.node_id === id)
}
function successors(id: string): string[] {
  return (dag.value?.nodes || []).filter((n) => n.depends_on.includes(id)).map((n) => n.node_id)
}
function onCreated(id: string) {
  showCompose.value = false
  load().then(() => { selected.value = id; loadDAG() })
}
function fmt(s: string) { return new Date(s).toLocaleString() }
function dagBadge(s: string) { return `st-${s}` }
function dagLabel(s: string) {
  return { running: '运行中', succeeded: '成功', failed: '失败', aborted: '已终止', pending: '等待' }[s] || s
}
function nodeLabel(s?: string) {
  return ({ pending: '等待', ready: '就绪', running: '执行中', succeeded: '成功', failed: '失败', skipped: '跳过' } as Record<string, string>)[s || ''] || s
}
function failLabel(p: string) {
  return { abort: '终止整个 DAG', skip: '跳过失败节点继续', retry: '重试失败节点' }[p] || p
}

onMounted(() => {
  load()
  timer = setInterval(() => { if (selected.value) loadDAG() }, 2000)
})
onUnmounted(() => timer && clearInterval(timer))
</script>

<style scoped>
.dag-row { padding:10px 12px; border-radius:8px; cursor:pointer; border:1px solid transparent; margin-bottom:6px; }
.dag-row:hover { background:var(--panel-2); }
.dag-row.active { background:var(--panel-2); border-color:var(--accent); }
.level { margin-bottom:18px; }
.level-nodes { display:flex; gap:12px; flex-wrap:wrap; }
.node-card { background:var(--panel-2); border:1px solid var(--border); border-left-width:4px; border-radius:8px; padding:12px; width:210px; }
.node-card.succeeded { border-left-color:var(--ok); }
.node-card.running, .node-card.ready { border-left-color:var(--normal); }
.node-card.failed { border-left-color:var(--err); }
.node-card.pending { border-left-color:var(--warn); }
.node-card.skipped { border-left-color:var(--muted); }
.node-head { display:flex; justify-content:space-between; align-items:center; gap:6px; }
</style>
