import { createPinia } from 'pinia'
import { createApp } from 'vue'
import App from './App.vue'
import { router } from './router'

const app = createApp(App)

app.config.errorHandler = (err, _instance, info) => {
  // Replaced by the toast/reporting pipeline once the API client lands.
  console.error('[venuepulse] unhandled error', { info, err })
}

app.use(createPinia()).use(router).mount('#app')
