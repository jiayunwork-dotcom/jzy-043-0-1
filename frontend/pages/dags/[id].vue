<template>
  <div v-if="dag">
    <NuxtLink to="/dags">← 返回 DAG 列表</NuxtLink>
    <h1 class="page-title" style="margin-top:10px">{{ dag.name }}</h1>
    <p class="page-sub">
      <span class="badge" :class="`st-${dag.status}`">{{ dag.status }}</span>
      · 失败处置 {{ dag.failure_policy }} · 创建于 {{ fmt(dag.created_at) }}
    </p>

    <div class="card">
      <div v-for="(level, li) in levels" :key="li" class="level">
        <div class="muted" style="font-size:12px;margin-bottom:6px">第 {{ li + 1 }} 层</div>
        <div class="level-nodes">
          <div v-for="nodeId in level" :key="nodeId" :class="['node-card', node(nodeId)?.state]">
            <div class="node-head">
              <strong>{{ nodeId }}</strong>
              <span class="badge" :class="`st-${node(nodeId)?.state}-node`">{{ node(nodeId)?.state }}</span>
            </div>
            <div class="muted" style="font-size:12px;margin-top:4px">{{ node(nodeId)?.def.task_type }}</div>
            <div class="muted" style="font-size:11px;margin-top:4px">
              前驱：{{ (node(nodeId)?.depends_on || []).join(', ') || '（根节点）' }}
            </div>
            <NuxtLink v-if="node(nodeId)?.task_id" :to="`/tasks`" class="mono" style="font-size:11px">
              {{ node(nodeId)?.task_id.slice(0,13) }}
            </NuxtLink>
            <div v-if="node(nodeId)?.error" class="error-text" style="font-size:11px;margin-top:4px">
              {{ node(nodeId)?.error }}
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import type { DAG, DAGNode } from '~/lib/api/types'
import { endpoints } from '~/lib/api/endpoints'

const route = useRoute()
const dag = ref<DAG | null>(null)
const levels = ref<string[][]>([])

function node(id: string): DAGNode | undefined {
  return dag.value?.nodes.find((n) => n.node_id === id)
}
function fmt(s: string) { return new Date(s).toLocaleString() }

onMounted(async () => {
  const res = await endpoints.getDAG(route.params.id as string)
  dag.value = res.dag
  levels.value = res.levels
})
</script>

<style scoped>
.level { margin-bottom:18px; }
.level-nodes { display:flex; gap:12px; flex-wrap:wrap; }
.node-card { background:var(--panel-2); border:1px solid var(--border); border-left-width:4px; border-radius:8px; padding:12px; width:220px; }
.node-card.succeeded { border-left-color:var(--ok); }
.node-card.running, .node-card.ready { border-left-color:var(--normal); }
.node-card.failed { border-left-color:var(--err); }
.node-card.pending { border-left-color:var(--warn); }
.node-card.skipped { border-left-color:var(--muted); }
.node-head { display:flex; justify-content:space-between; align-items:center; gap:6px; }
</style>
