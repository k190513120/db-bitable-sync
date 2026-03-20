import { bitable } from '@lark-base-open/js-sdk';
import zhLocale from './locales/zh.json';
import enLocale from './locales/en.json';
import jaLocale from './locales/ja.json';

export type Locale = 'zh' | 'en' | 'ja';

const locales: Record<string, Record<string, string>> = {
  zh: zhLocale,
  en: enLocale,
  ja: jaLocale
};

let currentLocale: Locale = 'zh';

export function getLocale(): Locale {
  return currentLocale;
}

export function setLocale(locale: Locale) {
  currentLocale = locale;
}

/**
 * Detect language from Feishu Bitable SDK, fallback to browser language.
 * Feishu SDK returns: 'zh' | 'en' | 'ja' etc.
 */
export async function detectLocale(): Promise<Locale> {
  try {
    const lang = await bitable.bridge.getLanguage();
    const langStr = String(lang || '').toLowerCase();
    if (langStr.startsWith('ja')) return 'ja';
    if (langStr.startsWith('zh')) return 'zh';
    return 'en';
  } catch (_) {
    // Fallback to browser language
    const browserLang = (navigator.language || '').toLowerCase();
    if (browserLang.startsWith('ja')) return 'ja';
    if (browserLang.startsWith('zh')) return 'zh';
    return 'en';
  }
}

/**
 * Translate a key with optional interpolation.
 * Usage: t('msg.connectSuccess', { count: 5 }) => "连接成功，已获取 5 张数据表..."
 */
export function t(key: string, params?: Record<string, string | number>): string {
  const dict = locales[currentLocale] || locales.en;
  let text = dict[key] ?? locales.en[key] ?? key;
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      text = text.replace(new RegExp(`\\{${k}\\}`, 'g'), String(v));
    }
  }
  return text;
}

/**
 * Returns true if current locale is Chinese (for Alipay routing).
 */
export function isChineseLocale(): boolean {
  return currentLocale === 'zh';
}

/**
 * Apply translations to all elements with data-i18n attribute.
 * Supports data-i18n="key" for textContent, data-i18n-placeholder="key" for placeholder,
 * data-i18n-title="key" for title attribute.
 */
export function applyI18n() {
  document.querySelectorAll('[data-i18n]').forEach((el) => {
    const key = el.getAttribute('data-i18n');
    if (key) el.textContent = t(key);
  });
  document.querySelectorAll('[data-i18n-placeholder]').forEach((el) => {
    const key = el.getAttribute('data-i18n-placeholder');
    if (key) (el as HTMLInputElement).placeholder = t(key);
  });
  document.querySelectorAll('[data-i18n-title]').forEach((el) => {
    const key = el.getAttribute('data-i18n-title');
    if (key) el.setAttribute('title', t(key));
  });
  document.querySelectorAll('[data-i18n-html]').forEach((el) => {
    const key = el.getAttribute('data-i18n-html');
    if (key) el.innerHTML = t(key);
  });

  // Update html lang attribute
  const langMap: Record<Locale, string> = { zh: 'zh-CN', en: 'en', ja: 'ja' };
  document.documentElement.lang = langMap[currentLocale] || 'en';

  // Update font-family for Japanese
  if (currentLocale === 'ja') {
    document.body.style.fontFamily = '"ヒラギノ角ゴ Pro W3", "Hiragino Kaku Gothic Pro", "Yu Gothic UI", "游ゴシック体", "Noto Sans Japanese", "Microsoft Jhenghei UI", "Microsoft Yahei UI", "ＭＳ Ｐゴシック", Arial, sans-serif, Apple Color Emoji, Segoe UI Emoji, Segoe UI Symbol, Noto Color Emoji';
  } else {
    document.body.style.fontFamily = '';
  }
}
