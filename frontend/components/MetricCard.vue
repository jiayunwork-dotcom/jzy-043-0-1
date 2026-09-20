<template>
  <div class="card">
    <div class="stat-label">{{ label }}</div>
    <div :class="['stat-value', size === 'small' ? 'small' : '']">
      <slot>{{ value }}</slot>
    </div>
    <div v-if="hint" class="muted" style="margin-top:6px;font-size:12px">{{ hint }}</div>
  </div>
</template>

<script setup lang="ts">
defineProps<{ label: string; value?: string | number; hint?: string; size?: 'normal' | 'small' }>()
</script>
