<template>
  <h1 class="page-title">总览大盘</h1>
  <p class="page-sub">队列深度、吞吐、Worker 利用率与死信积压的实时视图</p>

  <div class="grid" style="grid-template-columns:repeat(4,1fr)">
    <MetricCard label="Worker 在线 / 离线" :value="`${m.workers.online} / ${m.workers.offline}`"
      :hint="`排空中 ${m.workers.draining} · 槽位 ${m.workers.active_slots}/${m.workers.total_slots}`" />
    <MetricCard label="槽位利用率" :value="`${Math.round(m.utilization * 100)}%`"
      :hint="`${m.workers.active_slots} 个执行中槽位`" />
    <MetricCard label="每秒完成数（近 5 分钟均值）" :value="m.throughput_per_sec.toFixed(2)"
      :hint="`平均执行耗时 ${m.avg_exec_latency_ms.toFixed(0)} ms`" />
    <MetricCard label="死信积压" :value="m.dead_backlog" :hint="`延迟等待区 ${m.delay_depth}`" />
  </div>

  <div class="grid" style="grid-template-columns:1fr 1fr;margin-top:16px">
    <div class="card">
      <div class="stat-label" style="margin-bottom:8px">吞吐量趋势（完成 / 秒）</div>
      <Sparkline :values="completedTrend" color="#4c8dff" :height="90" />
      <div class="grid" style="grid-template-columns:1fr 1fr;margin-top:10px">
        <div class="muted">成功 <span style="color:var(--ok)">{{ sumField('succeeded') }}</span></div>
        <div class="muted">失败 <span class="error-text">{{ sumField('failed') }}</span></div>
      </div>
    </div>
    <QueueDepth :depth="m.queue_depth" />
  </div>

  <div class="grid" style="grid-template-columns:1fr 1fr;margin-top:16px">
    <div class="card">
      <div class="stat-label" style="margin-bottom:10px">各优先级成功率 / 失败率</div>
      <table>
        <thead><tr><th>优先级</th><th>成功率</th><th>失败率</th></tr></thead>
        <tbody>
          <tr v-for="p in priorities" :key="p">
            <td><span class="badge" :class="`pri-${p}`">{{ p }}</span></td>
            <td class="mono">{{ pct(m.success_rate[p]) }}%</td>
            <td class="mono error-text">{{ pct(m.failure_rate[p]) }}%</td>
          </tr>
        </tbody>
      </table>
    </div>
    <div class="card">
      <div class="stat-label" style="margin-bottom:8px">死信增长趋势（新增 / 秒）</div>
      <Sparkline :values="deadTrend" color="#ff5252" :height="90" />
      <div class="muted" style="margin-top:10px">
        平均调度延迟：{{ m.avg_sched_latency_ms.toFixed(0) }} ms（入就绪队列 → 开始执行）
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useMetricsStore } from '~/stores/metrics'

const { snapshot } = useMetricsStore()
const m = computed(() => snapshot.value)
const priorities = ['critical', 'high', 'normal', 'low', 'bulk']

const completedTrend = computed(() => m.value.trend.map((t) => t.completed))
const deadTrend = computed(() => {
  // produce a dense series aligned to the trend window
  const byTs = new Map(m.value.dead_trend.map((d) => [d.ts, d.dead]))
  return m.value.trend.map((t) => byTs.get(t.ts) || 0)
})

function pct(v: number | undefined) {
  return v === undefined ? '—' : (v * 100).toFixed(1)
}
function sumField(key: 'succeeded' | 'failed') {
  return m.value.trend.reduce((acc, t) => acc + t[key], 0)
}
</script>
