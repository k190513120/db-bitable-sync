import $ from 'jquery';
import { bitable } from '@lark-base-open/js-sdk';
import './index.scss';
import {
  getWereadSyncResult,
  normalizeWereadHighlights,
  startWereadSync
} from './weread-api';
import { prepareWereadTable, writeWereadHighlights } from './weread-table-operations';

const LOCAL_SYNC_BASE_URL = (import.meta.env.VITE_SYNC_BASE_URL as string) || 'http://localhost:8787';

$(function () {
  initializeApp();
});

function initializeApp() {
  setDefaultConfig();
  bindEvents();
}

function setDefaultConfig() {
  $('#maxRecords').val('200');
  $('#tableName').val('微信读书笔记');
}

function bindEvents() {
  $('#startSync').on('click', handleStartSync);
}

function getTableName(): string {
  return String($('#tableName').val() || '').trim();
}

function getWereadCookie(): string {
  return String($('#wereadCookie').val() || '').trim();
}

async function getCurrentUserId(): Promise<string> {
  try {
    const userId = await bitable.bridge.getBaseUserId();
    if (userId) return String(userId);
  } catch (_) {}
  try {
    const userId = await bitable.bridge.getUserId();
    if (userId) return String(userId);
  } catch (_) {}
  throw new Error('无法获取当前登录用户信息，请在多维表格内打开插件后重试');
}

function getMaxRecords(): number {
  const raw = Number($('#maxRecords').val());
  if (!Number.isFinite(raw)) return 200;
  return Math.max(1, Math.min(2000, Math.floor(raw)));
}

async function handleStartSync() {
  const tableName = getTableName();
  const wereadCookie = getWereadCookie();
  const maxRecords = getMaxRecords();

  if (!wereadCookie) {
    showResult('请先粘贴微信读书 Cookie', 'error');
    return;
  }

  try {
    const userId = await getCurrentUserId();
    setSyncLoading(true);
    updateProgress(10, '正在提交同步任务');
    const startResult = await startWereadSync(LOCAL_SYNC_BASE_URL, undefined, undefined, wereadCookie, maxRecords, userId);
    if (startResult.status === 'payment_required') {
      const checkoutUrl = String(startResult.checkoutUrl || '');
      if (checkoutUrl) {
        const opened = openCheckoutInNewTab(checkoutUrl);
        if (!opened) {
          showResult(`免费 10 条额度已用完，请点击前往支付：<a href="${checkoutUrl}" target="_blank" rel="noopener noreferrer">打开支付页面</a>`, 'info');
          return;
        }
        showResult('免费 10 条额度已用完，已在新窗口打开支付页面', 'info');
        return;
      }
      throw new Error(startResult.message || '免费额度已用完，请先支付后继续');
    }
    const rawHighlights = await waitForSyncData(LOCAL_SYNC_BASE_URL, startResult);
    const highlights = normalizeWereadHighlights(rawHighlights);

    if (!highlights.length) {
      throw new Error('未获取到可写入的划线或笔记');
    }

    updateProgress(70, '正在准备多维表格');
    const table = await prepareWereadTable(tableName, (progress, message) => {
      updateProgress(70 + progress * 0.15, message);
    });

    updateProgress(85, '正在写入记录');
    await writeWereadHighlights(table, highlights, (progress, message) => {
      updateProgress(85 + progress * 0.15, message);
    });

    updateProgress(100, '同步完成');
    showResult(`同步完成，已写入 ${highlights.length} 条微信读书记录。`, 'success');
  } catch (error) {
    showResult(`同步失败：${(error as Error).message}`, 'error');
  } finally {
    setSyncLoading(false);
  }
}

async function waitForSyncData(serviceUrl: string, startResult: { status?: string; jobId?: string; highlights?: unknown[] }) {
  if (Array.isArray(startResult.highlights)) {
    return startResult.highlights;
  }
  if (!startResult.jobId) {
    throw new Error('同步服务未返回可用数据或任务 ID');
  }

  const maxRetry = 45;
  for (let i = 0; i < maxRetry; i += 1) {
    updateProgress(45, `服务端同步中（${i + 1}/${maxRetry}）`);
    await wait(2000);
    const result = await getWereadSyncResult(serviceUrl, startResult.jobId);
    if (result.status === 'completed' && Array.isArray(result.highlights)) {
      return result.highlights;
    }
  }
  throw new Error('同步超时，请稍后重试');
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => {
    window.setTimeout(() => resolve(), ms);
  });
}

function openCheckoutInNewTab(checkoutUrl: string): boolean {
  try {
    const nextWindow = window.open(checkoutUrl, '_blank', 'noopener,noreferrer');
    return Boolean(nextWindow);
  } catch (_) {
    return false;
  }
}

function setSyncLoading(loading: boolean) {
  const button = $('#startSync');
  const text = $('#syncBtnText');
  const spinner = $('#syncLoadingSpinner');
  button.prop('disabled', loading);
  text.text(loading ? '同步中...' : '确认并同步');
  if (loading) spinner.show();
  else spinner.hide();
}

function updateProgress(progress: number, message: string) {
  const safeProgress = Math.max(0, Math.min(100, progress));
  $('#syncProgressContainer').show();
  $('#syncProgressBar').css('width', `${safeProgress}%`);
  $('#syncProgressText').text(message);
  $('#syncProgressValue').text(`${Math.round(safeProgress)}%`);
}

function showResult(message: string, type: 'success' | 'error' | 'info') {
  const messageEl = $('#resultMessage');
  messageEl.removeClass('success error info').addClass(type).html(message.replace(/\n/g, '<br>'));
  $('#resultContainer').show();
}
