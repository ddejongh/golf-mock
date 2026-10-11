import { defineConfigWithVueTs, vueTsConfigs } from '@vue/eslint-config-typescript'
import pluginVue from 'eslint-plugin-vue'

export default defineConfigWithVueTs(
  { ignores: ['dist/**', 'node_modules/**', 'src/api/generated/**'] },
  pluginVue.configs['flat/recommended'],
  vueTsConfigs.recommended,
)
