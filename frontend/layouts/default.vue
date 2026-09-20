<template>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand"><span class="dot" /> TaskForge</div>
      <NuxtLink to="/">总览</NuxtLink>
      <NuxtLink to="/tasks">任务</NuxtLink>
      <NuxtLink to="/dead">死信区</NuxtLink>
      <NuxtLink to="/dags">DAG 编排</NuxtLink>
      <NuxtLink to="/workers">Worker 集群</NuxtLink>
      <div class="conn">
        <span :class="['led', metrics.connected.value ? 'on' : 'off']" />
        {{ metrics.connected.value ? '实时已连接' : '连接中断' }}
      </div>
    </aside>
    <main class="main">
      <slot />
    </main>
    <div class="toasts">
      <div v-for="t in toast.toasts.value" :key="t.id" :class="['toast', t.kind]">
        {{ t.message }}
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useMetricsStore } from '~/stores/metrics'
import { useToast } from '~/stores/toast'

const metrics = useMetricsStore()
const toast = useToast()
</script>

<style scoped></style>
