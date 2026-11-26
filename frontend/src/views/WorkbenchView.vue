<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button
          v-if="evidenceEnabled"
          class="btn"
          data-testid="open-evidence"
          type="button"
          @click="openEvidence"
        >
          打开采用证据
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showEvidence" class="modal-backdrop" data-testid="evidence-modal">
      <div class="modal wide">
        <h3>采用证据 · {{ meta.name }}</h3>
        <p class="page-desc">
          证据来自服务端批次规则版本记录：每次采用的规则版本、同次嗅探、确认列数与迁移状态。
        </p>
        <form class="filter-bar" @submit.prevent="queryEvidence">
          <label class="filter-item">
            <span>关联批次</span>
            <input v-model="evidenceQuery.intakeBatch" data-testid="evidence-input-batch" placeholder="FI-xxx" />
          </label>
          <label class="filter-item">
            <span>规则编号</span>
            <input v-model="evidenceQuery.ruleNo" data-testid="evidence-input-rule" placeholder="DR-xxx" />
          </label>
          <button class="btn primary" data-testid="evidence-query" type="submit">查询证据</button>
          <button class="btn ghost" type="button" @click="showEvidence = false">关闭</button>
        </form>
        <table class="data-table compact">
          <thead>
            <tr>
              <th>版本记录</th><th>批次</th><th>规则</th><th>版本</th><th>轮次</th>
              <th>采用状态</th><th>嗅探</th><th>确认列数</th><th>起始行</th><th>迁移状态</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in evidenceItems" :key="String(item.版本记录编号)">
              <td>{{ item.版本记录编号 }}</td>
              <td>{{ item.关联批次 }}</td>
              <td>{{ item.规则编号 }}</td>
              <td>{{ item.规则版本号 }}</td>
              <td>{{ item.采用轮次 }}</td>
              <td>{{ item.采用状态 }}</td>
              <td>{{ item.结构嗅探编号 || '—' }}</td>
              <td>{{ item.确认列数 }}</td>
              <td>{{ item.起始行 }}</td>
              <td>{{ item.迁移状态 ?? '—' }}</td>
            </tr>
            <tr v-if="!evidenceItems.length">
              <td colspan="10" class="empty-state">暂无符合条件的采用证据</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  moduleMeta,
  runAction as applyAction,
} from '@/api/backend-service'
import { fetchEvidence, type EvidenceItem } from '@/api/rule-migration'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
// 结构嗅探页与分隔规则页都能打开同一份采用证据
const evidenceEnabled = props.moduleKey === 'schema_sniff' || props.moduleKey === 'delimiter_rule'
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const showEvidence = ref(false)
const evidenceItems = ref<EvidenceItem[]>([])
const evidenceQuery = ref({ intakeBatch: '', ruleNo: '' })
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

function openEvidence() {
  // 分隔规则页默认按当前行规则号、嗅探页默认按关联批次带入，也可手工改
  evidenceQuery.value = {
    intakeBatch: props.moduleKey === 'schema_sniff' ? '' : '',
    ruleNo: props.moduleKey === 'delimiter_rule' ? '' : '',
  }
  evidenceItems.value = []
  showEvidence.value = true
}

async function queryEvidence() {
  errorMessage.value = ''
  try {
    const result = await fetchEvidence(evidenceQuery.value.intakeBatch, evidenceQuery.value.ruleNo)
    evidenceItems.value = result.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采用证据读取失败'
  }
}

onMounted(reload)
</script>
