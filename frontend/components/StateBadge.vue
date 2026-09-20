<template>
  <span :class="['badge', `st-${state}`]">{{ label || state }}</span>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{ state: string; label?: string }>()
const _ = computed(() => props.state)
</script>
