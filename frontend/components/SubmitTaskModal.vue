<template>
  <div class="overlay" @click.self="$emit('close')">
    <div class="modal">
      <div class="modal-head"><strong>提交任务</strong><button @click="$emit('close')">关闭</button></div>

      <div class="form">
        <label>任务类型
          <select v-model="form.type" @change="onType">
            <option value="" disabled>选择已注册类型</option>
            <option v-for="t in types" :key="t" :value="t">{{ t }}</option>
          </select>
        </label>
        <label>优先级
          <select v-model="form.priority">
            <option v-for="p in priorities" :key="p" :value="p">{{ p }}</option>
          </select>
        </label>
        <label>负载 (JSON)
          <textarea v-model="payloadText" rows="5" class="mono"></textarea>
          <span v-if="payloadError" class="error-text">{{ payloadError }}</span>
        </label>
        <div class="grid2">
          <label>延迟执行（秒）<input v-model.number="form.delay_seconds" type="number" min="0" /></label>
          <label>单次超时（秒）<input v-model.number="form.timeout_seconds" type="number" min="1" /></label>
        </div>
        <div class="grid2">
          <label>重试策略
            <select v-model="form.retry_kind">
              <option value="exponential">指数退避</option>
              <option value="fixed">固定间隔</option>
              <option value="cron">Cron 表达式</option>
            </select>
          </label>
          <label>最大重试次数<input v-model.number="form.max_retries" type="number" min="0" /></label>
        </div>
        <div class="grid2">
          <label>基础间隔（秒，cron 可留空）
            <input v-model.number="form.base_seconds" type="number" min="0" :disabled="form.retry_kind==='cron'" />
          </label>
          <label v-if="form.retry_kind==='cron'">Cron 表达式
            <input v-model="form.cron" placeholder="*/5 * * * *" />
          </label>
        </div>
        <label>完成回调 URL（可选）<input v-model="form.callback_url" placeholder="https://..." /></label>
        <label>幂等键（可选）<input v-model="form.idempotency_key" /></label>
      </div>

      <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:16px">
        <button @click="$emit('close')">取消</button>
        <button class="primary" :disabled="!canSubmit" @click="submit">提交</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { reactive, ref, computed, onMounted } from 'vue'
import { endpoints } from '~/lib/api/endpoints'
import { useToast } from '~/stores/toast'

const emit = defineEmits<{ close: []; submitted: [] }>()
const toast = useToast()
const types = ref<string[]>([])
const priorities = ['critical', 'high', 'normal', 'low', 'bulk']

const form = reactive({
  type: '', priority: 'normal', delay_seconds: 0, timeout_seconds: 60,
  retry_kind: 'exponential', max_retries: 3, base_seconds: 1, cron: '',
  callback_url: '', idempotency_key: '',
})
const payloadText = ref('{}')
const payloadError = ref('')

const payload = computed(() => {
  try {
    const v = JSON.parse(payloadText.value || '{}')
    payloadError.value = ''
    return v
  } catch (e) {
    payloadError.value = '负载不是合法 JSON'
    return null
  }
})
const canSubmit = computed(() => !!form.type && payload.value !== null)

function onType() {
  if (form.type === 'sleep') payloadText.value = '{"sleep_ms": 200}'
  if (form.type === 'flaky') payloadText.value = '{"fail_first_n": 2}'
  if (form.type === 'always-fail') payloadText.value = '{}'
  if (form.type === 'compute') payloadText.value = '{"a": 6, "b": 7, "op": "mul"}'
  if (form.type === 'echo') payloadText.value = '{"hello": "world"}'
}

async function submit() {
  await toast.run('任务已提交', async () => {
    await endpoints.submitTask({
      type: form.type,
      payload: payload.value as Record<string, unknown>,
      priority: form.priority as never,
      delay_seconds: form.delay_seconds || undefined,
      timeout_seconds: form.timeout_seconds || undefined,
      max_retries: form.max_retries,
      retry_policy: {
        kind: form.retry_kind,
        base_interval_seconds: form.base_seconds,
        cron: form.cron,
        max_retries: form.max_retries,
      },
      callback_url: form.callback_url || undefined,
      idempotency_key: form.idempotency_key || undefined,
    })
    emit('submitted')
  })
}

onMounted(async () => {
  const res = await endpoints.taskTypes()
  types.value = res.types
})
</script>

<style scoped>
.overlay { position: fixed; inset: 0; background: rgba(0,0,0,.6); display:flex; align-items:center; justify-content:center; z-index:50; }
.modal { background: var(--panel); border:1px solid var(--border); border-radius:12px; width:560px; max-height:90vh; overflow:auto; padding:20px; }
.modal-head { display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; }
.form { display:flex; flex-direction:column; gap:12px; }
.form label { display:flex; flex-direction:column; gap:6px; font-size:13px; color:var(--muted); }
.form input, .form select, .form textarea { color:var(--text); }
.grid2 { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
</style>
