<template>
  <div class="card">
    <div class="stat-label" style="margin-bottom:12px">五级就绪队列实时深度</div>
    <div v-for="p in priorities" :key="p.key" style="margin-bottom:10px">
      <div style="display:flex;justify-content:space-between;margin-bottom:4px">
        <span class="badge" :class="`pri-${p.key}`">{{ p.label }}</span>
        <span class="mono">{{ depth[p.key] || 0 }}</span>
      </div>
      <div class="bar">
        <span :style="{ width: barWidth(depth[p.key] || 0), background: p.color }" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{ depth: Record<string, number> }>()
const priorities = [
  { key: 'critical', label: 'Critical', color: '#ff5252' },
  { key: 'high', label: 'High', color: '#ff9f43' },
  { key: 'normal', label: 'Normal', color: '#4c8dff' },
  { key: 'low', label: 'Low', color: '#26c6da' },
  { key: 'bulk', label: 'Bulk', color: '#9b8cff' },
]
const max = computed(() => Math.max(1, ...Object.values(props.depth).map(Number)))
function barWidth(n: number) {
  return `${Math.max(2, Math.round((n / max.value) * 100))}%`
}
</script>
