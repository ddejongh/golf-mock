<script setup lang="ts">
import { RouterView } from 'vue-router'
</script>

<template>
  <header class="app-header">
    <strong>VenuePulse</strong>
  </header>
  <main>
    <RouterView />
  </main>
</template>

<style>
:root {
  font-family: system-ui, sans-serif;
  color-scheme: light dark;
}

body {
  margin: 0;
}

.app-header {
  padding: 0.75rem 1rem;
  border-bottom: 1px solid color-mix(in srgb, currentColor 15%, transparent);
}

main {
  padding: 1rem;
}
</style>
