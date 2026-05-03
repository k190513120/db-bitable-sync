/**
 * Feishu Bitable API - 使用多维表格授权码（PersonalBaseToken）访问
 *
 * 重要：PersonalBaseToken 必须使用 base-api.feishu.cn 域名，
 *       而不是 open.feishu.cn（那是飞书开放平台 OAuth token 用的）。
 * 文档: https://open.feishu.cn/document/server-docs/docs/bitable-v1
 */

const FEISHU_BASE_API = 'https://open.feishu.cn/open-apis';

// 飞书 Base 已知的瞬时错误关键词与错误码（命中即重试）
const TRANSIENT_MSG_KEYWORDS = [
  'data not ready',
  'try again',
  'timeout',
  'rate limit',
  'too many request',
  'service unavailable',
  'internal error'
];
const TRANSIENT_CODES = new Set([1254290, 1254607, 91402, 99991400, 99991663, 99991672]);

function isTransientFeishuError(code, msg) {
  const m = String(msg || '').toLowerCase();
  if (TRANSIENT_MSG_KEYWORDS.some((k) => m.includes(k))) return true;
  return TRANSIENT_CODES.has(Number(code));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function request(method, path, token, body, opName) {
  const url = `${FEISHU_BASE_API}${path}`;
  const headers = {
    'Authorization': `Bearer ${token}`,
    'Content-Type': 'application/json'
  };
  const options = { method, headers };
  if (body) options.body = JSON.stringify(body);
  const label = opName || `${method} ${path}`;
  const maxAttempts = 5;
  let lastErr;

  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      const res = await fetch(url, options);
      // HTTP 5xx / 429 也按瞬时错误对待
      if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
        if (attempt < maxAttempts - 1) {
          const wait = 2 ** attempt * 1000;
          console.warn(`[feishu-retry] ${label} HTTP ${res.status}, retry ${attempt + 1}/${maxAttempts} in ${wait}ms`);
          await sleep(wait);
          continue;
        }
        throw new Error(`飞书 API HTTP ${res.status}: ${await res.text().catch(() => '')}`);
      }
      const data = await res.json();
      if (data.code === 0) return data.data;
      if (isTransientFeishuError(data.code, data.msg) && attempt < maxAttempts - 1) {
        const wait = 2 ** attempt * 1000;
        console.warn(`[feishu-retry] ${label} 瞬时错误 [${data.code}] ${data.msg}, retry ${attempt + 1}/${maxAttempts} in ${wait}ms`);
        await sleep(wait);
        continue;
      }
      throw new Error(`飞书 API 错误 (${data.code}): ${data.msg || '未知错误'}`);
    } catch (err) {
      lastErr = err;
      // 已经构造过的飞书业务错误（带"飞书 API 错误"前缀）不再重试
      if (String(err.message || '').startsWith('飞书 API ')) throw err;
      if (attempt < maxAttempts - 1) {
        const wait = 2 ** attempt * 1000;
        console.warn(`[feishu-retry] ${label} 网络异常: ${err.message}, retry ${attempt + 1}/${maxAttempts} in ${wait}ms`);
        await sleep(wait);
        continue;
      }
      throw err;
    }
  }
  throw lastErr || new Error(`${label} 重试 ${maxAttempts} 次仍失败`);
}

export async function listBitableTables(token, appToken) {
  const data = await request('GET', `/bitable/v1/apps/${appToken}/tables`, token, null, 'listTables');
  return (data.items || []).map((t) => ({
    tableId: t.table_id,
    name: t.name
  }));
}

export async function createBitableTable(token, appToken, name, fields) {
  const data = await request('POST', `/bitable/v1/apps/${appToken}/tables`, token, {
    table: { name, default_view_name: '默认视图', fields }
  }, 'createTable');
  return data.table_id;
}

export async function listBitableFields(token, appToken, tableId) {
  const data = await request('GET', `/bitable/v1/apps/${appToken}/tables/${tableId}/fields`, token, null, 'listFields');
  return (data.items || []).map((f) => ({
    fieldId: f.field_id,
    fieldName: f.field_name,
    type: f.type
  }));
}

export async function addBitableField(token, appToken, tableId, fieldName, type) {
  const data = await request('POST', `/bitable/v1/apps/${appToken}/tables/${tableId}/fields`, token, {
    field_name: fieldName,
    type
  }, 'addField');
  // API returns field info directly in data, or nested under data.field
  const field = data.field || data;
  return { field_id: field.field_id, field_name: field.field_name, type: field.type };
}

export function mapToFeishuFieldType(dataType) {
  const t = String(dataType || '').toLowerCase();
  if (t.includes('int') || t.includes('decimal') || t.includes('float') ||
      t.includes('double') || t.includes('numeric') || t.includes('real') ||
      t.includes('number') || t.includes('serial') || t.includes('money')) {
    return 2; // Number
  }
  if (t.includes('date') || t.includes('time') || t.includes('year') || t.includes('datetime') || t.includes('timestamp')) {
    return 5; // DateTime
  }
  return 1; // Text
}

function toCellValue(value, fieldType) {
  if (value === null || value === undefined) return null;
  if (fieldType === 2) { // Number
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  if (fieldType === 5) { // DateTime
    if (value instanceof Date) return value.getTime();
    const time = new Date(String(value)).getTime();
    return Number.isNaN(time) ? null : time;
  }
  return typeof value === 'object' ? JSON.stringify(value) : String(value);
}

export async function batchInsertRecords(token, appToken, tableId, records) {
  const BATCH_SIZE = 450;
  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    await request('POST', `/bitable/v1/apps/${appToken}/tables/${tableId}/records/batch_create`, token, {
      records: batch
    }, 'batchInsert');
  }
}

export async function batchUpdateRecords(token, appToken, tableId, records) {
  const BATCH_SIZE = 450;
  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    await request('POST', `/bitable/v1/apps/${appToken}/tables/${tableId}/records/batch_update`, token, {
      records: batch
    }, 'batchUpdate');
  }
}

/**
 * 拉取目标表全部记录，返回 [{ record_id, fields }] 数组（带字段值）。
 * 任意一页失败会抛异常（不返回残缺数据），调用方需中止该表本次同步。
 */
export async function listAllRecords(token, appToken, tableId) {
  let pageToken = '';
  const all = [];
  do {
    const query = pageToken ? `?page_token=${pageToken}&page_size=500` : '?page_size=500';
    const data = await request('GET', `/bitable/v1/apps/${appToken}/tables/${tableId}/records${query}`, token, null, 'listRecords');
    const items = data.items || [];
    for (const item of items) all.push({ record_id: item.record_id, fields: item.fields || {} });
    pageToken = data.page_token || '';
  } while (pageToken);
  return all;
}

export async function clearBitableRecords(token, appToken, tableId) {
  const records = await listAllRecords(token, appToken, tableId);
  const allIds = records.map((r) => r.record_id);
  const BATCH_SIZE = 500;
  for (let i = 0; i < allIds.length; i += BATCH_SIZE) {
    const batch = allIds.slice(i, i + BATCH_SIZE);
    await request('POST', `/bitable/v1/apps/${appToken}/tables/${tableId}/records/batch_delete`, token, {
      records: batch
    }, 'batchDelete');
  }
}

/**
 * 把飞书记录 fields 中某字段的值规整成可比对的字符串 key。
 * 飞书的 Text 字段读出来是 [{type:'text', text:'xxx'}] 数组形式，需要拼回字符串。
 */
function fieldValueToKey(value) {
  if (value === null || value === undefined) return '';
  if (Array.isArray(value)) {
    return value.map((seg) => (seg && typeof seg === 'object' && 'text' in seg ? seg.text : String(seg))).join('');
  }
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

/**
 * 完整同步单张表到 Bitable
 *
 * @param {string} token PersonalBaseToken
 * @param {string} appToken
 * @param {{tableName, columns, rows}} tablePayload
 * @param {string} tablePrefix
 * @param {'full'|'incremental'} syncMode
 * @param {string|null} primaryKeyColumn 增量模式下 MySQL 主键列名（用于 upsert 比对）
 */
export async function syncTableToBitable(token, appToken, tablePayload, tablePrefix, syncMode, primaryKeyColumn) {
  const targetName = `${tablePrefix}${tablePayload.tableName}`;

  // Find or create table
  const existingTables = await listBitableTables(token, appToken);
  let targetTable = existingTables.find((t) => t.name === targetName);
  let tableId;

  if (targetTable) {
    tableId = targetTable.tableId;
  } else {
    // Create table — first field must be Text type (Bitable requirement)
    const fields = [];
    for (const col of tablePayload.columns) {
      const fieldType = mapToFeishuFieldType(col.dataType);
      // First field in Bitable must be Text (index field)
      fields.push({
        field_name: col.name,
        type: fields.length === 0 ? 1 : fieldType
      });
    }
    fields.push({ field_name: '同步时间', type: 5 }); // DateTime
    tableId = await createBitableTable(token, appToken, targetName, fields);
  }

  // Ensure fields exist
  const existingFields = await listBitableFields(token, appToken, tableId);
  const fieldNameMap = new Map(existingFields.map((f) => [f.fieldName, f]));

  for (const col of tablePayload.columns) {
    if (!fieldNameMap.has(col.name)) {
      const field = await addBitableField(token, appToken, tableId, col.name, mapToFeishuFieldType(col.dataType));
      fieldNameMap.set(col.name, { fieldId: field.field_id, fieldName: col.name, type: field.type });
    }
  }
  if (!fieldNameMap.has('同步时间')) {
    const field = await addBitableField(token, appToken, tableId, '同步时间', 5);
    fieldNameMap.set('同步时间', { fieldId: field.field_id, fieldName: '同步时间', type: 5 });
  }

  // Refresh field list for accurate IDs
  const allFields = await listBitableFields(token, appToken, tableId);
  const fieldIdMap = {};
  const fieldTypeMap = {};
  for (const f of allFields) {
    fieldIdMap[f.fieldName] = f.fieldId;
    fieldTypeMap[f.fieldName] = f.type;
  }

  // ── 增量模式：按主键 upsert ──
  if (syncMode === 'incremental' && primaryKeyColumn) {
    // 1. 拉飞书侧已有记录，建主键 → record_id 映射
    //    任意分页失败抛异常（不会拿残缺映射继续写入）
    const existingRecords = await listAllRecords(token, appToken, tableId);
    const pkFieldName = primaryKeyColumn;
    const pkToRecordId = new Map();
    for (const rec of existingRecords) {
      const v = rec.fields[pkFieldName];
      if (v === undefined || v === null) continue;
      pkToRecordId.set(fieldValueToKey(v), rec.record_id);
    }

    // 2. 拆分新增 / 更新
    const now = Date.now();
    const toInsert = [];
    const toUpdate = [];
    for (const row of tablePayload.rows) {
      const fields = {};
      for (const col of tablePayload.columns) {
        if (fieldIdMap[col.name]) {
          fields[col.name] = toCellValue(row[col.name], fieldTypeMap[col.name] || 1);
        }
      }
      fields['同步时间'] = now;

      const pkValue = row[pkFieldName];
      const pkKey = pkValue === undefined || pkValue === null ? '' : String(pkValue);
      const matchedRecordId = pkKey ? pkToRecordId.get(pkKey) : undefined;

      if (matchedRecordId) {
        toUpdate.push({ record_id: matchedRecordId, fields });
      } else {
        toInsert.push({ fields });
      }
    }

    if (toUpdate.length) await batchUpdateRecords(token, appToken, tableId, toUpdate);
    if (toInsert.length) await batchInsertRecords(token, appToken, tableId, toInsert);

    return { tableName: targetName, rowCount: tablePayload.rows.length, created: toInsert.length, updated: toUpdate.length };
  }

  // ── full / 无主键回退：清空 + 全量写入 ──
  if (syncMode !== 'incremental') {
    await clearBitableRecords(token, appToken, tableId);
  }

  // Build records
  const now = Date.now();
  const records = tablePayload.rows.map((row) => {
    const fields = {};
    for (const col of tablePayload.columns) {
      const fieldName = col.name;
      if (fieldIdMap[fieldName]) {
        fields[fieldName] = toCellValue(row[fieldName], fieldTypeMap[fieldName] || 1);
      }
    }
    fields['同步时间'] = now;
    return { fields };
  });

  // Batch insert
  await batchInsertRecords(token, appToken, tableId, records);

  return { tableName: targetName, rowCount: tablePayload.rows.length, created: records.length, updated: 0 };
}
