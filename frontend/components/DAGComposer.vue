<template>
  <div class="overlay" @click.self="$emit('close')">
    <div class="modal">
      <div class="modal-head"><strong>定义并提交 DAG</strong><button @click="$emit('close')">关闭</button></div>

      <div class="grid2">
        <label>DAG 名称<input v-model="name" placeholder="例如 订单履约流程" /></label>
        <label>默认失败处置
          <select v-model="failurePolicy">
            <option value="abort">终止整个 DAG</option>
            <option value="skip">跳过失败节点继续</option>
            <option value="retry">重试失败节点</option>
          </select>
        </label>
      </div>

      <div style="display:flex;justify-content:space-between;align-items:center;margin:14px 0 8px">
        <strong>节点（{{ nodes.length }}）</strong>
        <button @click="addNode">+ 添加节点</button>
      </div>

      <div class="nodes">
        <div v-for="(n, i) in nodes" :key="i" class="node-edit card">
          <div class="grid3">
            <label>节点 ID<input v-model="n.id" placeholder="A" /></label>
            <label>任务类型
              <select v-model="n.task_type">
                <option value="" disabled>类型</option>
                <option v-for="t in types" :key="t" :value="t">{{ t }}</option>
              </select>
            </label>
            <label>优先级
              <select v-model="n.priority">
                <option v-for="p in priorities" :key="p" :value="p">{{ p }}</option>
              </select>
            </label>
          </div>
          <div class="grid3">
            <label>依赖节点（逗号分隔）<input v-model="n.depsText" placeholder="B,C" /></label>
            <label>节点失败处置
              <select v-model="n.on_failure">
                <option value="abort">终止</option>
                <option value="skip">跳过</option>
                <option value="retry">重试</option>
              </select>
            </label>
            <label>节点重试次数<input v-model.number="n.max_retries" type="number" min="0" /></label>
          </div>
          <label class="muted">负载 (JSON)<textarea v-model="n.payload" rows="2" class="mono"></textarea></label>
          <div style="text-align:right"><button class="danger" @click="nodes.splice(i,1)">删除节点</button></div>
        </div>
      </div>

      <div v-if="message" :class="messageOk ? '' : 'error-text'" style="margin-top:10px">{{ message }}</div>

      <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:14px">
        <button @click="validate">环检测 / 校验</button>
        <button class="primary" @click="submit">提交 DAG</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { endpoints } from '~/lib/api/endpoints'
import { useToast } from '~/stores/toast'

const emit = defineEmits<{ close: []; created: [id: string] }>()
const toast = useToast()
const types = ref<string[]>([])
const priorities = ['critical', 'high', 'normal', 'low', 'bulk']

interface NodeDraft {
  id: string; task_type: string; priority: string; depsText: string
  on_failure: string; max_retries: number; payload: string
}
const name = ref('新 DAG')
const failurePolicy = ref('abort')
const nodes = ref<NodeDraft[]>([])
const message = ref('')
const messageOk = ref(false)

function addNode() {
  nodes.value.push({
    id: '', task_type: types.value[0] || '', priority: 'normal', depsText: '',
    on_failure: 'abort', max_retries: 1, payload: '{}',
  })
}

function buildBody() {
  return {
    name: name.value,
    failure_policy: failurePolicy.value,
    nodes: nodes.value.map((n) => ({
      id: n.id,
      task_type: n.task_type,
      payload: safeJSON(n.payload),
      priority: n.priority,
      timeout_seconds: 60,
      depends_on: n.depsText.split(',').map((s) => s.trim()).filter(Boolean),
      on_failure: n.on_failure,
      max_retries: Number(n.max_retries) || 0,
    })),
  }
}
function safeJSON(s: string) {
  try { return JSON.parse(s || '{}') } catch { return {} }
}

async function validate() {
  try {
    const r = await endpoints.validateDAG(buildBody())
    messageOk.value = true
    message.value = `校验通过，共 ${r.levels.length} 个拓扑层级`
  } catch (e) {
    messageOk.value = false
    message.value = '校验失败：' + (e instanceof Error ? e.message : String(e))
  }
}

async function submit() {
  try {
    const r = await endpoints.createDAG(buildBody())
    toast.success('DAG 已提交并开始执行')
    emit('created', r.dag.id)
  } catch (e) {
    messageOk.value = false
    message.value = '提交失败：' + (e instanceof Error ? e.message : String(e))
  }
}

onMounted(async () => {
  const res = await endpoints.taskTypes()
  types.value = res.types
  addNode()
})
</script>

<style scoped>
.overlay { position:fixed; inset:0; background:rgba(0,0,0,.6); display:flex; align-items:center; justify-content:center; z-index:50; }
.modal { background:var(--panel); border:1px solid var(--border); border-radius:12px; width:760px; max-height:92vh; overflow:auto; padding:20px; }
.modal-head { display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; }
.grid2 { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
.grid3 { display:grid; grid-template-columns:1fr 1fr 1fr; gap:10px; margin-bottom:8px; }
label { display:flex; flex-direction:column; gap:5px; font-size:12px; }
.nodes { max-height:46vh; overflow:auto; display:flex; flex-direction:column; gap:10px; }
.node-edit { padding:12px; }
</style>
