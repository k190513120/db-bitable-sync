import { bitable, FieldType, ITable } from '@lark-base-open/js-sdk';
import { WereadHighlight } from './weread-api';

type ProgressHandler = (progress: number, message: string) => void;

const DEFAULT_TABLE_NAME = '微信读书划线同步';

const REQUIRED_FIELDS: Array<{ name: string; type: FieldType }> = [
  { name: '书名', type: FieldType.Text },
  { name: '作者', type: FieldType.Text },
  { name: '章节', type: FieldType.Text },
  { name: '划线内容', type: FieldType.Text },
  { name: '笔记内容', type: FieldType.Text },
  { name: '标签', type: FieldType.Text },
  { name: '书籍ID', type: FieldType.Text },
  { name: '划线ID', type: FieldType.Text },
  { name: '划线时间', type: FieldType.DateTime },
  { name: '更新时间', type: FieldType.DateTime },
  { name: '同步时间', type: FieldType.DateTime },
  { name: '来源', type: FieldType.Text }
];

async function ensureFields(table: ITable): Promise<void> {
  const existing = await table.getFieldMetaList();
  const existingNames = new Set(existing.map((f) => f.name));
  for (const field of REQUIRED_FIELDS) {
    if (!existingNames.has(field.name)) {
      await table.addField({
        type: field.type as FieldType.Text | FieldType.DateTime,
        name: field.name
      });
    }
  }
}

async function clearTableRecords(table: ITable): Promise<void> {
  const recordIds = await table.getRecordIdList();
  const batchSize = 100;
  for (let i = 0; i < recordIds.length; i += batchSize) {
    await table.deleteRecords(recordIds.slice(i, i + batchSize));
  }
}

async function getFieldMap(table: ITable): Promise<Record<string, string>> {
  const fields = await table.getFieldMetaList();
  const map: Record<string, string> = {};
  for (const name of REQUIRED_FIELDS.map((f) => f.name)) {
    const field = fields.find((item) => item.name === name);
    if (field) {
      map[name] = field.id;
    }
  }
  return map;
}

function requireFieldId(fieldMap: Record<string, string>, name: string): string {
  const id = fieldMap[name];
  if (!id) {
    throw new Error(`字段缺失: ${name}`);
  }
  return id;
}

export async function prepareWereadTable(tableName?: string, onProgress?: ProgressHandler): Promise<ITable> {
  const finalTableName = tableName?.trim() || DEFAULT_TABLE_NAME;
  onProgress?.(15, '正在检查目标数据表');
  const tables = await bitable.base.getTableMetaList();
  const existing = tables.find((table) => table.name === finalTableName);
  let table: ITable;

  if (existing) {
    table = await bitable.base.getTableById(existing.id);
    onProgress?.(25, '已找到历史数据表，正在清理旧数据');
    await clearTableRecords(table);
  } else {
    onProgress?.(25, '正在创建微信读书同步数据表');
    const created = await bitable.base.addTable({
      name: finalTableName,
      fields: [{ name: '书名', type: FieldType.Text }]
    });
    table = await bitable.base.getTableById(created.tableId);
  }

  onProgress?.(35, '正在校验并补齐字段');
  await ensureFields(table);
  return table;
}

export async function writeWereadHighlights(
  table: ITable,
  highlights: WereadHighlight[],
  onProgress?: ProgressHandler
): Promise<void> {
  if (!highlights.length) {
    return;
  }

  const fieldMap = await getFieldMap(table);
  const bookTitleFieldId = requireFieldId(fieldMap, '书名');
  const authorFieldId = requireFieldId(fieldMap, '作者');
  const chapterFieldId = requireFieldId(fieldMap, '章节');
  const highlightTextFieldId = requireFieldId(fieldMap, '划线内容');
  const noteTextFieldId = requireFieldId(fieldMap, '笔记内容');
  const tagsFieldId = requireFieldId(fieldMap, '标签');
  const bookIdFieldId = requireFieldId(fieldMap, '书籍ID');
  const highlightIdFieldId = requireFieldId(fieldMap, '划线ID');
  const highlightedAtFieldId = requireFieldId(fieldMap, '划线时间');
  const updatedAtFieldId = requireFieldId(fieldMap, '更新时间');
  const syncTimeFieldId = requireFieldId(fieldMap, '同步时间');
  const sourceFieldId = requireFieldId(fieldMap, '来源');
  const syncTime = Date.now();
  const batchSize = 50;
  const totalBatches = Math.ceil(highlights.length / batchSize);

  for (let batchIndex = 0; batchIndex < totalBatches; batchIndex += 1) {
    const start = batchIndex * batchSize;
    const end = Math.min(start + batchSize, highlights.length);
    const batch = highlights.slice(start, end);

    const records = batch.map((item) => ({
      fields: {
        [bookTitleFieldId]: item.bookTitle,
        [authorFieldId]: item.author,
        [chapterFieldId]: item.chapter,
        [highlightTextFieldId]: item.highlightText,
        [noteTextFieldId]: item.noteText,
        [tagsFieldId]: item.tags,
        [bookIdFieldId]: item.bookId,
        [highlightIdFieldId]: item.highlightId,
        [highlightedAtFieldId]: item.highlightedAt ?? syncTime,
        [updatedAtFieldId]: item.updatedAt ?? syncTime,
        [syncTimeFieldId]: syncTime,
        [sourceFieldId]: '微信读书'
      }
    }));

    await table.addRecords(records);
    const progress = ((batchIndex + 1) / totalBatches) * 100;
    onProgress?.(progress, `已写入 ${end}/${highlights.length} 条划线`);
  }
}
