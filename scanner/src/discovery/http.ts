import type { DiscoveryStatus } from './types';

export class PublicApiError extends Error {
  constructor(
    message: string,
    readonly discoveryStatus: DiscoveryStatus,
    readonly httpStatus?: number,
  ) {
    super(message);
    this.name = 'PublicApiError';
  }
}
type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;
export type JsonRequestOptions = { fetchImpl?: FetchLike; retries?: number; timeoutMs?: number; };

export async function fetchJson<T = unknown>(url: string, options: JsonRequestOptions = {}): Promise<T> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const retries = options.retries ?? 2;
  const timeoutMs = options.timeoutMs ?? 12_000;
  let lastError: unknown;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    let response: Response | undefined;
    try {
      response = await fetchImpl(url, {
        headers: { accept: 'application/json', 'user-agent': 'samurab-market-discovery/1.0' },
        signal: AbortSignal.timeout(timeoutMs),
      });
      if (!response.ok) {
        const retryable = response.status === 408 || response.status === 429 || response.status >= 500;
        const message = `HTTP ${response.status} from ${new URL(url).host}`;
        const status: DiscoveryStatus = response.status === 429 ? 'RATE_LIMITED' : response.status >= 500 || response.status === 408 ? 'UNAVAILABLE' : 'ERROR';
        lastError = new PublicApiError(message, status, response.status);
        if (!retryable || attempt === retries) throw lastError;
        await delay(retryDelay(response.headers.get('retry-after'), attempt));
        continue;
      }
      try {
        return await response.json() as T;
      } catch {
        throw new PublicApiError(`Malformed JSON from ${new URL(url).host}`, 'ERROR', response.status);
      }
    } catch (error) {
      if (error instanceof PublicApiError) {
        lastError = error;
        if (error.discoveryStatus === 'ERROR' || attempt === retries) throw error;
      } else {
        const timedOut = error instanceof Error && (error.name === 'TimeoutError' || error.name === 'AbortError');
        lastError = new PublicApiError(
          `${timedOut ? 'Timed out' : 'Network error'} requesting ${new URL(url).host}: ${error instanceof Error ? error.message : String(error)}`,
          'UNAVAILABLE',
          response?.status,
        );
        if (attempt === retries) throw lastError;
      }
      await delay(retryDelay(null, attempt));
    }
  }
  throw lastError instanceof Error ? lastError : new PublicApiError('Request failed', 'UNAVAILABLE');
}

function retryDelay(retryAfter: string | null, attempt: number): number {
  const requested = retryAfter ? Number(retryAfter) * 1000 : NaN;
  if (Number.isFinite(requested) && requested >= 0) return Math.min(5000, requested);
  return Math.min(2000, 250 * 2 ** attempt);
}
function delay(ms: number): Promise<void> { return new Promise((resolve) => setTimeout(resolve, ms)); }
export function errorHealth(error: unknown, endpoint: string) {
  const known = error instanceof PublicApiError ? error : undefined;
  return { status: known?.discoveryStatus ?? 'ERROR' as const, endpoint, error: error instanceof Error ? error.message : String(error), ...(known?.httpStatus ? { httpStatus: known.httpStatus } : {}) };
}
