import { bitable, FieldType, ITable } from '@lark-base-open/js-sdk';
import { DbTablePayload } from './db-api';

type ProgressHandler = (progress: number, message: string) => void;

function mapFieldType(dataType: string): FieldType {
  const type = String(dataType || '').toLowerCase();
  if (
    type.includes('int') ||
    type.includes('decimal') ||
    type.includes('float') ||
    type.includes('double') ||
    type.includes('numeric') ||
    type.includes('real') ||
    type.includes('bit') ||
    type.includes('serial') ||
    type.includes('money') ||
    type === 'number'
  ) {
    return FieldType.Number;
  }
  if (type.includes('date') || type.includes('time') || type.includes('year') || type.includes('datetime') || type.includes('timestamp')) {
    return FieldType.DateTime;
  }
  if (type === 'boolean' || type === 'bool') {
    return FieldType.Text;
  }
  return FieldType.Text;
}

async function ensureTable(tableName: string): Promise<ITable> {
  const allTables = await bitable.base.getTableMetaList();
  const found = allTables.find((table) => table.name === tableName);
  if (found) {
    return bitable.base.getTableById(found.id);
  }
  const created = await bitable.base.addTable({
    name: tableName,
    fields: [{ name: '占位字段', type: FieldType.Text }]
  });
  return bitable.base.getTableById(created.tableId);
}

async function clearTableRecords(table: ITable): Promise<void> {
  const recordIds = await table.getRecordIdList();
  const chunkSize = 100;
  for (let i = 0; i < recordIds.length; i += chunkSize) {
    await table.deleteRecords(recordIds.slice(i, i + chunkSize));
  }
}

async function ensureFields(table: ITable, columns: DbTablePayload['columns']): Promise<Record<string, string>> {
  const fieldMetaList = await table.getFieldMetaList();
  const fieldMap = new Map(fieldMetaList.map((item) => [item.name, item.id]));
  for (const column of columns) {
    if (fieldMap.has(column.name)) continue;
    await table.addField({
      name: column.name,
      type: mapFieldType(column.dataType) as FieldType.Text | FieldType.Number | FieldType.DateTime
    });
  }
  if (!fieldMap.has('同步时间')) {
    await table.addField({
      name: '同步时间',
      type: FieldType.DateTime
    });
  }
  // Clean up the placeholder field created by ensureTable
  if (fieldMap.has('占位字段')) {
    try {
      await table.deleteField(fieldMap.get('占位字段')!);
    } catch (_) {
      // Bitable requires at least one field — ignore if deletion fails
    }
  }
  const latestMeta = await table.getFieldMetaList();
  return latestMeta.reduce<Record<string, string>>((acc, item) => {
    acc[item.name] = item.id;
    return acc;
  }, {});
}

function toCellValue(value: unknown, dataType: string): unknown {
  if (value === null || value === undefined) return '';
  const type = String(dataType || '').toLowerCase();
  if (type.includes('date') || type.includes('time') || type.includes('year') || type.includes('timestamp')) {
    if (value instanceof Date) return value.getTime();
    const time = new Date(String(value)).getTime();
    return Number.isNaN(time) ? '' : time;
  }
  if (
    type.includes('int') ||
    type.includes('decimal') ||
    type.includes('float') ||
    type.includes('double') ||
    type.includes('numeric') ||
    type.includes('real') ||
    type.includes('bit') ||
    type.includes('serial') ||
    type.includes('money') ||
    type === 'number'
  ) {
    const n = Number(value);
    return Number.isFinite(n) ? n : '';
  }
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

export interface SyncTableOptions {
  syncMode?: 'full' | 'incremental';
  /** 增量模式下用作 upsert 比对的主键列名（来自 MySQL 列） */
  primaryKey?: string;
}

/**
 * 把飞书 cell 值规整为可比对的字符串 key。
 * Text 字段从 SDK 读出来通常是 [{type:'text', text:'xxx'}] 形态，需要拼回字符串。
 */
function cellValueToKey(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (Array.isArray(value)) {
    return value
      .map((seg) => (seg && typeof seg === 'object' && 'text' in (seg as any) ? (seg as any).text : String(seg)))
      .join('');
  }
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

/**
 * 拉取目标表全部记录的 [{ recordId, fields }]，用于按主键建 upsert 映射。
 * 任意一页失败抛异常（不返回残缺数据）。
 */
async function listAllRecords(table: ITable): Promise<Array<{ recordId: string; fields: Record<string, unknown> }>> {
  const out: Array<{ recordId: string; fields: Record<string, unknown> }> = [];
  let pageToken: string | undefined;
  do {
    const resp: any = await table.getRecords({ pageSize: 5000, pageToken });
    const records = resp?.records || [];
    for (const r of records) {
      out.push({ recordId: r.recordId, fields: r.fields || {} });
    }
    pageToken = resp?.hasMore ? resp.pageToken : undefined;
  } while (pageToken);
  return out;
}

export async function syncTableToBitable(
  tablePayload: DbTablePayload,
  tablePrefix: string,
  onProgress?: ProgressHandler,
  options?: SyncTableOptions
): Promise<{ tableName: string; rowCount: number; created: number; updated: number }> {
  const tableName = `${tablePrefix}${tablePayload.tableName}`;
  const syncMode = options?.syncMode || 'full';
  const primaryKey = options?.primaryKey || '';

  onProgress?.(10, `准备同步表 ${tablePayload.tableName}`);
  const table = await ensureTable(tableName);

  if (syncMode === 'full') {
    onProgress?.(20, `清理目标表 ${tableName} 历史数据`);
    await clearTableRecords(table);
  }

  onProgress?.(35, `校验字段 ${tableName}`);
  const fieldIdMap = await ensureFields(table, tablePayload.columns);

  const rows = Array.isArray(tablePayload.rows) ? tablePayload.rows : [];
  if (!rows.length) {
    return { tableName, rowCount: 0, created: 0, updated: 0 };
  }

  const syncTimeFieldId = fieldIdMap['同步时间'];
  const pkFieldId = primaryKey ? fieldIdMap[primaryKey] : '';

  // ── 增量模式 + 有主键：拉飞书已有记录建映射，做真 upsert ──
  if (syncMode === 'incremental' && primaryKey && pkFieldId) {
    onProgress?.(25, `读取 ${tableName} 已有记录用于去重`);
    const existing = await listAllRecords(table);
    const pkToRecordId = new Map<string, string>();
    for (const rec of existing) {
      const v = rec.fields[pkFieldId];
      const k = cellValueToKey(v);
      if (k) pkToRecordId.set(k, rec.recordId);
    }

    const toInsert: Array<{ fields: Record<string, any> }> = [];
    const toUpdate: Array<{ recordId: string; fields: Record<string, any> }> = [];
    const now = Date.now();
    for (const row of rows) {
      const fields: Record<string, any> = {};
      for (const column of tablePayload.columns) {
        const fieldId = fieldIdMap[column.name];
        if (!fieldId) continue;
        fields[fieldId] = toCellValue(row[column.name], column.dataType);
      }
      if (syncTimeFieldId) fields[syncTimeFieldId] = now;

      const pkVal = row[primaryKey];
      const pkKey = pkVal === undefined || pkVal === null ? '' : String(pkVal);
      const matchedId = pkKey ? pkToRecordId.get(pkKey) : undefined;
      if (matchedId) {
        toUpdate.push({ recordId: matchedId, fields });
      } else {
        toInsert.push({ fields });
      }
    }

    const batchSize = 50;
    let written = 0;
    const total = toInsert.length + toUpdate.length;
    for (let i = 0; i < toUpdate.length; i += batchSize) {
      const batch = toUpdate.slice(i, i + batchSize);
      await table.setRecords(batch as any);
      written += batch.length;
      const p = 35 + (written / Math.max(1, total)) * 65;
      onProgress?.(p, `已更新 ${written}/${total} 行 ${tableName}`);
    }
    for (let i = 0; i < toInsert.length; i += batchSize) {
      const batch = toInsert.slice(i, i + batchSize);
      await table.addRecords(batch as any);
      written += batch.length;
      const p = 35 + (written / Math.max(1, total)) * 65;
      onProgress?.(p, `已新增 ${written}/${total} 行 ${tableName}`);
    }

    return { tableName, rowCount: rows.length, created: toInsert.length, updated: toUpdate.length };
  }

  // ── full / 无主键回退：批量 addRecords ──
  const batchSize = 50;
  const total = rows.length;
  const totalBatch = Math.ceil(total / batchSize);
  for (let batchIndex = 0; batchIndex < totalBatch; batchIndex += 1) {
    const start = batchIndex * batchSize;
    const end = Math.min(start + batchSize, total);
    const batchRows = rows.slice(start, end);
    const records = batchRows.map((row) => {
      const fields: Record<string, any> = {};
      for (const column of tablePayload.columns) {
        const fieldId = fieldIdMap[column.name];
        if (!fieldId) continue;
        fields[fieldId] = toCellValue(row[column.name], column.dataType);
      }
      if (syncTimeFieldId) {
        fields[syncTimeFieldId] = Date.now();
      }
      return { fields };
    });
    await table.addRecords(records as any);
    const progress = 35 + ((batchIndex + 1) / totalBatch) * 65;
    onProgress?.(progress, `已写入 ${end}/${total} 行到 ${tableName}`);
  }
  return { tableName, rowCount: rows.length, created: rows.length, updated: 0 };
}
