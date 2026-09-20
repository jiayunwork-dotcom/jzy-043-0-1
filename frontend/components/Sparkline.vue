<template>
  <svg class="spark" :viewBox="`0 0 ${w} ${h}`" preserveAspectRatio="none">
    <polyline
      :points="points"
      fill="none"
      :stroke="color"
      stroke-width="2"
      stroke-linejoin="round"
      stroke-linecap="round"
    />
    <polygon :points="area" :fill="fillColor" />
  </svg>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = withDefaults(defineProps<{
  values: number[]
  color?: string
  height?: number
}>(), {
  color: '#4c8dff',
  height: 60,
})

const w = 300
const h = computed(() => props.height)

const points = computed(() => {
  const v = props.values.length ? props.values : [0]
  const max = Math.max(1, ...v)
  const step = w / Math.max(1, v.length - 1)
  return v.map((x, i) => `${(i * step).toFixed(1)},${(h.value - (x / max) * (h.value - 6) - 3).toFixed(1)}`).join(' ')
})

const area = computed(() => {
  if (!points.value) return ''
  return `0,${h.value} ${points.value} ${w},${h.value}`
})

const fillColor = computed(() => props.color + '22')
</script>
