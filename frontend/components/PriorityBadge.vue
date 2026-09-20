<template>
  <span :class="['badge', cls]"><slot /></span>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{ kind: string }>()
const cls = computed(() => `pri-${props.kind}`)
</script>
