export interface WereadLoginSession {
  sessionId: string;
  qrCodeUrl?: string;
  qrCodeBase64?: string;
  expiresAt?: number;
  pollIntervalMs?: number;
  mode?: string;
  message?: string;
}

export type WereadLoginStatus = 'pending' | 'authorized' | 'expired' | 'failed';

export interface WereadLoginStatusResponse {
  status: WereadLoginStatus;
  message?: string;
}

export interface WereadHighlight {
  bookTitle: string;
  author: string;
  chapter: string;
  highlightText: string;
  noteText: string;
  tags: string;
  highlightId: string;
  bookId: string;
  highlightedAt?: number;
  updatedAt?: number;
}

export interface WereadSyncResponse {
  status?: 'processing' | 'completed' | 'payment_required';
  jobId?: string;
  highlights?: unknown[];
  message?: string;
  checkoutUrl?: string;
}

function buildUrl(baseUrl: string, path: string): string {
  const normalizedBase = baseUrl.trim().replace(/\/+$/, '');
  const normalizedPath = path.startsWith('/') ? path : `/${path}`;
  return `${normalizedBase}${normalizedPath}`;
}

function buildHeaders(apiKey?: string): HeadersInit {
  const headers: HeadersInit = {
    'Content-Type': 'application/json'
  };
  if (apiKey?.trim()) {
    headers['Authorization'] = `Bearer ${apiKey.trim()}`;
  }
  return headers;
}

async function requestJson<T>(url: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(url, init);
  } catch (error) {
    const message = (error as Error).message || '网络请求失败';
    throw new Error(`网络请求失败，请检查同步服务地址可访问性（HTTPS页面需HTTPS接口）：${message}`);
  }
  if (!response.ok) {
    const message = await response.text();
    let serverMessage = '';
    try {
      const parsed = JSON.parse(message) as { message?: string };
      serverMessage = parsed?.message || '';
    } catch (_) {}
    throw new Error(serverMessage || message || `请求失败: ${response.status}`);
  }
  return response.json() as Promise<T>;
}

export async function createLoginSession(baseUrl: string, apiKey?: string): Promise<WereadLoginSession> {
  return requestJson<WereadLoginSession>(buildUrl(baseUrl, '/api/weread/login/session'), {
    method: 'POST',
    headers: buildHeaders(apiKey)
  });
}

export async function getLoginSessionStatus(
  baseUrl: string,
  sessionId: string,
  apiKey?: string
): Promise<WereadLoginStatusResponse> {
  return requestJson<WereadLoginStatusResponse>(
    buildUrl(baseUrl, `/api/weread/login/session/${encodeURIComponent(sessionId)}/status`),
    {
      method: 'GET',
      headers: buildHeaders(apiKey)
    }
  );
}

export async function startWereadSync(
  baseUrl: string,
  sessionId?: string,
  apiKey?: string,
  wereadCookie?: string,
  maxRecords?: number,
  userId?: string
): Promise<WereadSyncResponse> {
  return requestJson<WereadSyncResponse>(buildUrl(baseUrl, '/api/weread/sync'), {
    method: 'POST',
    headers: buildHeaders(apiKey),
    body: JSON.stringify({ sessionId, wereadCookie, maxRecords, userId })
  });
}

export async function bindSessionCookie(
  baseUrl: string,
  sessionId: string,
  wereadCookie: string,
  apiKey?: string
): Promise<WereadLoginStatusResponse> {
  return requestJson<WereadLoginStatusResponse>(
    buildUrl(baseUrl, `/api/weread/login/session/${encodeURIComponent(sessionId)}/cookie`),
    {
      method: 'POST',
      headers: buildHeaders(apiKey),
      body: JSON.stringify({ wereadCookie })
    }
  );
}

export async function getWereadSyncResult(baseUrl: string, jobId: string, apiKey?: string): Promise<WereadSyncResponse> {
  return requestJson<WereadSyncResponse>(buildUrl(baseUrl, `/api/weread/sync/${encodeURIComponent(jobId)}`), {
    method: 'GET',
    headers: buildHeaders(apiKey)
  });
}

function asTimestamp(value: unknown): number | undefined {
  if (typeof value === 'number') {
    if (value > 1000000000000) return value;
    if (value > 1000000000) return value * 1000;
    return undefined;
  }
  if (typeof value === 'string' && value.trim()) {
    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) {
      return date.getTime();
    }
  }
  return undefined;
}

function pickString(source: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const value = source[key];
    if (typeof value === 'string' && value.trim()) {
      return value.trim();
    }
    if (typeof value === 'number') {
      return String(value);
    }
  }
  return '';
}

export function normalizeWereadHighlights(input: unknown[] | undefined): WereadHighlight[] {
  if (!Array.isArray(input)) {
    return [];
  }

  return input
    .map((item) => {
      if (!item || typeof item !== 'object') return null;
      const raw = item as Record<string, unknown>;

      const tagsValue = raw.tags;
      const tags = Array.isArray(tagsValue)
        ? tagsValue.map((v) => String(v)).join('、')
        : pickString(raw, ['tags', 'tag']);

      const mapped: WereadHighlight = {
        bookTitle: pickString(raw, ['bookTitle', 'book_name', 'bookName', 'title']),
        author: pickString(raw, ['author', 'bookAuthor']),
        chapter: pickString(raw, ['chapter', 'chapterTitle', 'chapter_name']),
        highlightText: pickString(raw, ['highlightText', 'markText', 'text', 'highlight_content']),
        noteText: pickString(raw, ['noteText', 'reviewContent', 'note', 'comment']),
        tags,
        highlightId: pickString(raw, ['highlightId', 'markId', 'bookmarkId']),
        bookId: pickString(raw, ['bookId', 'book_id']),
        highlightedAt: asTimestamp(raw.highlightedAt ?? raw.createTime ?? raw.markTime),
        updatedAt: asTimestamp(raw.updatedAt ?? raw.updateTime)
      };

      if (!mapped.bookTitle && !mapped.highlightText && !mapped.noteText) {
        return null;
      }
      return mapped;
    })
    .filter((item): item is WereadHighlight => Boolean(item));
}
