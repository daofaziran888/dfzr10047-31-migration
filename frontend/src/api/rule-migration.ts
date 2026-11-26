/**
 * 规则迁移专用接口封装：预览、迁移单流转、样本领取、同次嗅探绑定、字段交付、
 * 恢复逗号、字段解析、采用证据、发布看板与操作员资格引导。
 */
import { request } from './client'

export type MigrationPreview = {
  ok: boolean
  message: string
  entry?: {
    规则编号: string
    规则版本号: string
    适用格式: string
    分隔符: string
    引号字符: string
    转义字符: string
    preview_rows: string[][]
    column_count: number
    consistent: boolean
  } | null
}

export type MigrationAction = {
  ok: boolean
  message: string
  entry?: Record<string, unknown> | null
}

export type EvidenceItem = {
  版本记录编号: string
  关联批次: string
  规则编号: string
  规则版本号: string
  迁移单编号: string
  采用轮次: string
  采用状态: string
  结构嗅探编号: string
  确认列数: string
  起始行: string
  迁移状态?: string
  旧预览列数?: string
  新预览列数?: string
  [field: string]: unknown
}

export type PublishBoard = {
  claimed_samples: EvidenceItem[]
  pending_new_batches: { 关联批次: string }[]
}

export type Guidance = {
  eligible: boolean
  direction: string
  关联批次: string
  规则编号?: string
  版本记录编号?: string
  qualified_rules?: string[]
  message: string
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  if (!response.ok) throw new Error(`接口返回 ${response.status}，操作未生效`)
  return (await response.json()) as T
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error(`接口返回 ${response.status}，数据未更新`)
  return (await response.json()) as T
}

const BASE = '/api/rule-migration'

export function previewRule(ruleNo: string, sampleLines: string[]): Promise<MigrationPreview> {
  return postJson(`${BASE}/previews/${encodeURIComponent(ruleNo)}`, { sample_lines: sampleLines })
}

export function createMigration(payload: {
  migration_no: string
  fmt: string
  old_rule_no: string
  new_rule_no: string
  sample_lines: string[]
  operator: string
  at: string
}): Promise<MigrationAction> {
  return postJson(`${BASE}/orders`, payload)
}

export function migrationPreview(migrationNo: string): Promise<{
  ok: boolean
  detail?: Record<string, unknown>
  message?: string
}> {
  return getJson(`${BASE}/orders/${encodeURIComponent(migrationNo)}/preview`)
}

export function activateMigration(migrationNo: string, at: string): Promise<MigrationAction> {
  return postJson(`${BASE}/orders/${encodeURIComponent(migrationNo)}/activate`, { at })
}

export function interruptMigration(migrationNo: string, reason: string, at: string): Promise<MigrationAction> {
  return postJson(`${BASE}/orders/${encodeURIComponent(migrationNo)}/interrupt`, { reason, at })
}

export function rollbackMigration(migrationNo: string, reason: string, at: string): Promise<MigrationAction> {
  return postJson(`${BASE}/orders/${encodeURIComponent(migrationNo)}/rollback`, { reason, at })
}

export function retryMigration(migrationNo: string, at: string): Promise<MigrationAction> {
  return postJson(`${BASE}/orders/${encodeURIComponent(migrationNo)}/retry`, { at })
}

export function claimSample(payload: {
  migration_no: string
  intake_batch: string
  operator: string
  at: string
}): Promise<MigrationAction> {
  return postJson(`${BASE}/claims`, payload)
}

export function bindSniff(recordNo: string, sniffNo: string, at: string): Promise<MigrationAction> {
  return postJson(`${BASE}/adoptions/bind-sniff`, { record_no: recordNo, sniff_no: sniffNo, at })
}

export function deliverFields(recordNo: string, at: string): Promise<MigrationAction> {
  return postJson(`${BASE}/adoptions/${encodeURIComponent(recordNo)}/deliver`, { at })
}

export function restoreComma(payload: {
  intake_batch: string
  operator: string
  at: string
}): Promise<MigrationAction> {
  return postJson(`${BASE}/restore-comma`, payload)
}

export function parseFields(intakeBatch: string, dataLines: string[]): Promise<MigrationAction> {
  return postJson(`${BASE}/parse-fields`, { intake_batch: intakeBatch, data_lines: dataLines })
}

export function fetchEvidence(intakeBatch = '', ruleNo = ''): Promise<{ items: EvidenceItem[] }> {
  const query = new URLSearchParams()
  if (intakeBatch) query.set('intake_batch', intakeBatch)
  if (ruleNo) query.set('rule_no', ruleNo)
  const suffix = query.toString() ? `?${query.toString()}` : ''
  return getJson(`${BASE}/evidence${suffix}`)
}

export function fetchPublishBoard(): Promise<PublishBoard> {
  return getJson(`${BASE}/publish-board`)
}

export function fetchOperatorGuidance(intakeBatch: string, operator: string): Promise<Guidance> {
  return postJson(`${BASE}/operator-guidance`, { intake_batch: intakeBatch, operator })
}
