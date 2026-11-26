<template>
  <section class="page" data-module="rule_migration_console">
    <header class="page-head">
      <div>
        <h2>分隔规则迁移控制台</h2>
        <p class="page-desc">
          存量分隔规则的可预览、可启用、可回滚版本化迁移：两份分列预览永久保留，
          样本领取与同次嗅探绑定后字段才能交付，回滚后按服务端资格导向旧批继续或重选规则。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh-board" type="button" @click="loadBoard">刷新发布页</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="console-error">{{ errorMessage }}</p>
    <p v-if="infoMessage" class="info-text" data-testid="console-info">{{ infoMessage }}</p>

    <!-- 1. 分列预览 -->
    <article class="console-card" data-testid="preview-panel">
      <h3>① 分列预览（旧引号/转义行为保持兼容）</h3>
      <div class="form-grid">
        <label>规则编号
          <input v-model="preview.ruleNo" data-testid="preview-rule" placeholder="如 DR-001" />
        </label>
        <label class="grow">样本行（每行一条）
          <textarea v-model="preview.sampleText" data-testid="preview-lines" rows="3"
            placeholder="rec_id,note,val&#10;R-1,&quot;hello, world&quot;,100.5"></textarea>
        </label>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="preview-run" type="button" @click="runPreview">生成预览</button>
      </div>
      <div v-if="previewResult" class="preview-box" data-testid="preview-result">
        <p class="preview-meta">
          规则 {{ previewResult.entry?.规则编号 }}（{{ previewResult.entry?.规则版本号 }}）：
          实际列数 <strong>{{ previewResult.entry?.column_count }}</strong>
          ，列数一致：{{ previewResult.entry?.consistent ? '是' : '否' }}
        </p>
        <table class="data-table compact">
          <tbody>
            <tr v-for="(row, idx) in previewResult.entry?.preview_rows ?? []" :key="idx">
              <td v-for="(cell, ci) in row" :key="ci">{{ cell }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </article>

    <!-- 2. 建迁移单 -->
    <article class="console-card" data-testid="create-panel">
      <h3>② 建立迁移单（固化新旧两份分列预览）</h3>
      <div class="form-grid">
        <label>迁移单编号
          <input v-model="form.migrationNo" data-testid="order-no" placeholder="RM-xxx" />
        </label>
        <label>适用格式
          <input v-model="form.fmt" data-testid="order-fmt" placeholder="csv" />
        </label>
        <label>旧规则编号
          <input v-model="form.oldRuleNo" data-testid="order-old" placeholder="DR-001" />
        </label>
        <label>新规则编号
          <input v-model="form.newRuleNo" data-testid="order-new" placeholder="DR-003" />
        </label>
        <label>操作者
          <input v-model="form.operator" data-testid="order-operator" />
        </label>
        <label class="grow">迁移样本（每行一条）
          <textarea v-model="form.sampleText" data-testid="order-lines" rows="3"></textarea>
        </label>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="order-create" type="button" @click="createOrder">建单并固化预览</button>
      </div>
    </article>

    <!-- 3. 迁移单流转 -->
    <article class="console-card" data-testid="orders-panel">
      <h3>③ 迁移单流转（启用 / 中断 / 回滚 / 重试）</h3>
      <table class="data-table compact">
        <thead>
          <tr>
            <th>迁移单</th><th>格式</th><th>旧→新规则</th><th>状态</th>
            <th>旧预览列数</th><th>新预览列数</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="order in orders" :key="String(order.迁移单编号)" :data-status="String(order.迁移状态)">
            <td>{{ order.迁移单编号 }}</td>
            <td>{{ order.适用格式 }}</td>
            <td>{{ order.旧规则编号 }} → {{ order.新规则编号 }}</td>
            <td>{{ order.迁移状态 }}</td>
            <td>{{ order.旧预览列数 }}</td>
            <td>{{ order.新预览列数 }}</td>
            <td class="row-actions">
              <button class="link" data-testid="view-preview" type="button" @click="viewPreview(String(order.迁移单编号))">看两版预览</button>
              <button class="link" data-testid="activate-order" type="button" @click="activate(String(order.迁移单编号))">启用</button>
              <button class="link" data-testid="interrupt-order" type="button" @click="interrupt(String(order.迁移单编号))">中断</button>
              <button class="link" data-testid="rollback-order" type="button" @click="rollback(String(order.迁移单编号))">回滚</button>
              <button class="link" data-testid="retry-order" type="button" @click="retry(String(order.迁移单编号))">回滚后重试</button>
            </td>
          </tr>
        </tbody>
      </table>
      <div v-if="previewDetail" class="preview-box" data-testid="order-preview-detail">
        <h4>{{ previewDetail.迁移单编号 }} 两版分列对照（状态：{{ previewDetail.迁移状态 }}）</h4>
        <div class="preview-cols">
          <div>
            <p class="preview-meta">旧规则 {{ previewDetail.旧规则编号 }}（{{ previewDetail.旧预览列数 }} 列）</p>
            <table class="data-table compact">
              <tbody>
                <tr v-for="(row, i) in previewDetail.旧规则预览" :key="'o' + i">
                  <td v-for="(cell, j) in row" :key="j">{{ cell }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div>
            <p class="preview-meta">新规则 {{ previewDetail.新规则编号 }}（{{ previewDetail.新预览列数 }} 列）</p>
            <table class="data-table compact">
              <tbody>
                <tr v-for="(row, i) in previewDetail.新规则预览" :key="'n' + i">
                  <td v-for="(cell, j) in row" :key="j">{{ cell }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </article>

    <!-- 4. 发布页：已领取样本 vs 待处理新批 -->
    <article class="console-card" data-testid="board-panel">
      <h3>④ 发布页（已领取样本 / 待处理新批）</h3>
      <div class="preview-cols">
        <div>
          <p class="preview-meta">已领取样本（在途，字段未交付）</p>
          <table class="data-table compact">
            <thead><tr><th>批次</th><th>规则</th><th>状态</th></tr></thead>
            <tbody>
              <tr v-for="item in board.claimed_samples" :key="item.版本记录编号">
                <td>{{ item.关联批次 }}</td>
                <td>{{ item.规则编号 }}</td>
                <td>{{ item.采用状态 }}</td>
              </tr>
              <tr v-if="!board.claimed_samples.length"><td colspan="3" class="empty-state">暂无在途样本</td></tr>
            </tbody>
          </table>
        </div>
        <div>
          <p class="preview-meta">待处理的新批</p>
          <table class="data-table compact">
            <thead><tr><th>批次</th></tr></thead>
            <tbody>
              <tr v-for="item in board.pending_new_batches" :key="item.关联批次">
                <td>{{ item.关联批次 }}</td>
              </tr>
              <tr v-if="!board.pending_new_batches.length"><td class="empty-state">暂无待处理批次</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </article>

    <!-- 5. 领取 / 绑定 / 交付 / 恢复逗号 -->
    <article class="console-card" data-testid="adoption-panel">
      <h3>⑤ 批次采用动作（领取→绑定同次嗅探→交付；恢复逗号不覆盖确认列）</h3>
      <div class="form-grid">
        <label>迁移单编号
          <input v-model="adopt.migrationNo" data-testid="adopt-migration" placeholder="RM-xxx" />
        </label>
        <label>关联批次
          <input v-model="adopt.intakeBatch" data-testid="adopt-batch" placeholder="FI-xxx" />
        </label>
        <label>版本记录编号
          <input v-model="adopt.recordNo" data-testid="adopt-record" placeholder="BRV-..." />
        </label>
        <label>结构嗅探编号
          <input v-model="adopt.sniffNo" data-testid="adopt-sniff" placeholder="SS-..." />
        </label>
        <label>操作者
          <input v-model="adopt.operator" data-testid="adopt-operator" />
        </label>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="adopt-claim" type="button" @click="claim">从嗅探页领取一次</button>
        <button class="btn" data-testid="adopt-bind" type="button" @click="bind">绑定同次嗅探</button>
        <button class="btn" data-testid="adopt-deliver" type="button" @click="deliver">确认交付字段</button>
        <button class="btn ghost" data-testid="adopt-restore" type="button" @click="restore">恢复逗号解析</button>
        <button class="btn ghost" data-testid="adopt-guidance" type="button" @click="guidance">查服务端资格</button>
      </div>
      <div v-if="guidanceResult" class="preview-box" data-testid="adopt-guidance-result">
        <p>
          导向：<strong>{{ guidanceResult.direction }}</strong>
          <span v-if="guidanceResult.eligible">（轮次 {{ guidanceResult.版本记录编号 }}，规则 {{ guidanceResult.规则编号 }}）</span>
          <span v-else>（合格规则：{{ (guidanceResult.qualified_rules ?? []).join('、') }}）</span>
        </p>
        <p>{{ guidanceResult.message }}</p>
      </div>
    </article>

    <!-- 6. 采用证据 -->
    <article class="console-card" data-testid="evidence-panel">
      <h3>⑥ 打开采用证据（结构嗅探页 / 分隔规则页同源）</h3>
      <div class="form-grid">
        <label>按关联批次
          <input v-model="evidenceFilter.intakeBatch" data-testid="evidence-batch" placeholder="FI-xxx" />
        </label>
        <label>按规则编号
          <input v-model="evidenceFilter.ruleNo" data-testid="evidence-rule" placeholder="DR-xxx" />
        </label>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="evidence-query" type="button" @click="loadEvidence">查询采用证据</button>
      </div>
      <table class="data-table compact">
        <thead>
          <tr>
            <th>版本记录</th><th>批次</th><th>规则</th><th>版本</th><th>轮次</th>
            <th>状态</th><th>嗅探</th><th>确认列数</th><th>起始行</th><th>迁移状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in evidence" :key="item.版本记录编号">
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
          <tr v-if="!evidence.length"><td colspan="10" class="empty-state">暂无采用证据</td></tr>
        </tbody>
      </table>
    </article>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import {
  activateMigration,
  bindSniff,
  claimSample,
  createMigration,
  deliverFields,
  fetchEvidence,
  fetchOperatorGuidance,
  fetchPublishBoard,
  interruptMigration,
  migrationPreview,
  previewRule,
  restoreComma,
  retryMigration,
  rollbackMigration,
  type EvidenceItem,
  type Guidance,
  type MigrationPreview,
  type PublishBoard,
} from '@/api/rule-migration'
import { listEntries } from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const now = () => new Date().toISOString()
const toLines = (text: string) => text.split('\n').map((line) => line.trim()).filter(Boolean)

const errorMessage = ref('')
const infoMessage = ref('')

const preview = reactive({ ruleNo: 'DR-001', sampleText: '' })
const previewResult = ref<MigrationPreview | null>(null)

const form = reactive({
  migrationNo: '',
  fmt: 'csv',
  oldRuleNo: 'DR-001',
  newRuleNo: 'DR-003',
  operator: 'on-duty-01',
  sampleText: '',
})

const adopt = reactive({
  migrationNo: '',
  intakeBatch: '',
  recordNo: '',
  sniffNo: '',
  operator: 'on-duty-01',
})

const evidenceFilter = reactive({ intakeBatch: '', ruleNo: '' })
const orders = ref<EntryRow[]>([])
const evidence = ref<EvidenceItem[]>([])
const board = ref<PublishBoard>({ claimed_samples: [], pending_new_batches: [] })
const guidanceResult = ref<Guidance | null>(null)
const previewDetail = ref<Record<string, any> | null>(null)

function flashOk(message: string) {
  errorMessage.value = ''
  infoMessage.value = message
}

function flashError(error: unknown) {
  infoMessage.value = ''
  errorMessage.value = error instanceof Error ? error.message : '操作失败'
}

async function runPreview() {
  try {
    previewResult.value = await previewRule(preview.ruleNo, toLines(preview.sampleText))
    if (!previewResult.value.ok) throw new Error(previewResult.value.message)
    flashOk(previewResult.value.message)
  } catch (error) {
    flashError(error)
  }
}

async function createOrder() {
  try {
    const result = await createMigration({
      migration_no: form.migrationNo,
      fmt: form.fmt,
      old_rule_no: form.oldRuleNo,
      new_rule_no: form.newRuleNo,
      sample_lines: toLines(form.sampleText),
      operator: form.operator,
      at: now(),
    })
    if (!result.ok) throw new Error(result.message)
    flashOk(result.message)
    await loadOrders()
  } catch (error) {
    flashError(error)
  }
}

async function loadOrders() {
  const page = await listEntries('rule_migration', {})
  orders.value = page.items
}

async function viewPreview(migrationNo: string) {
  try {
    const result = await migrationPreview(migrationNo)
    if (!result.ok) throw new Error(result.message ?? '预览不存在')
    previewDetail.value = (result.detail ?? null) as Record<string, any> | null
  } catch (error) {
    flashError(error)
  }
}

async function activate(migrationNo: string) {
  try {
    const result = await activateMigration(migrationNo, now())
    if (!result.ok) throw new Error(result.message)
    flashOk(result.message)
    await loadOrders()
    await loadBoard()
  } catch (error) {
    flashError(error)
  }
}

async function interrupt(migrationNo: string) {
  try {
    const result = await interruptMigration(migrationNo, '值班中断：字段在途未交付', now())
    if (!result.ok) throw new Error(result.message)
    flashOk(result.message)
    await loadOrders()
  } catch (error) {
    flashError(error)
  }
}

async function rollback(migrationNo: string) {
  try {
    const result = await rollbackMigration(migrationNo, '回滚：保留预览与已确认列', now())
    if (!result.ok) throw new Error(result.message)
    flashOk(result.message)
    await loadOrders()
    await loadBoard()
  } catch (error) {
    flashError(error)
  }
}

async function retry(migrationNo: string) {
  try {
    const result = await retryMigration(migrationNo, now())
    if (!result.ok) throw new Error(result.message)
    flashOk(result.message)
    await loadOrders()
  } catch (error) {
    flashError(error)
  }
}

async function loadBoard() {
  try {
    board.value = await fetchPublishBoard()
  } catch (error) {
    flashError(error)
  }
}

async function claim() {
  try {
    const result = await claimSample({
      migration_no: adopt.migrationNo,
      intake_batch: adopt.intakeBatch,
      operator: adopt.operator,
      at: now(),
    })
    if (!result.ok) throw new Error(result.message)
    if (result.entry && '版本记录编号' in result.entry) {
      adopt.recordNo = String(result.entry.版本记录编号)
    }
    flashOk(result.message)
    await loadBoard()
  } catch (error) {
    flashError(error)
  }
}

async function bind() {
  try {
    const result = await bindSniff(adopt.recordNo, adopt.sniffNo, now())
    if (!result.ok) throw new Error(result.message)
    flashOk(result.message)
    await loadBoard()
  } catch (error) {
    flashError(error)
  }
}

async function deliver() {
  try {
    const result = await deliverFields(adopt.recordNo, now())
    if (!result.ok) throw new Error(result.message)
    flashOk(result.message)
    await loadBoard()
  } catch (error) {
    flashError(error)
  }
}

async function restore() {
  try {
    const result = await restoreComma({
      intake_batch: adopt.intakeBatch,
      operator: adopt.operator,
      at: now(),
    })
    if (!result.ok) throw new Error(result.message)
    flashOk(result.message)
    await loadBoard()
  } catch (error) {
    flashError(error)
  }
}

async function guidance() {
  try {
    guidanceResult.value = await fetchOperatorGuidance(adopt.intakeBatch, adopt.operator)
  } catch (error) {
    flashError(error)
  }
}

async function loadEvidence() {
  try {
    const result = await fetchEvidence(evidenceFilter.intakeBatch, evidenceFilter.ruleNo)
    evidence.value = result.items
  } catch (error) {
    flashError(error)
  }
}

onMounted(async () => {
  await Promise.all([loadOrders(), loadBoard()])
})
</script>
