<template>
  <div>
    <NuxtLayout>
      <NuxtPage />
    </NuxtLayout>
  </div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted } from 'vue'
import { useMetricsStore } from '~/stores/metrics'

const metrics = useMetricsStore()
onMounted(() => metrics.start())
onUnmounted(() => metrics.stop())
</script>
