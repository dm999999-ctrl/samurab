export type SnapshotResponse = {
  status: number;
  ok: boolean;
  headers: { get(name: string): string | null };
  json(): Promise<unknown>;
};

export type SnapshotSchedulerOptions = {
  minIntervalMs?: number;
  maxRetries?: number;
  backoffBaseMs?: number;
  maxBackoffMs?: number;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
};

type SnapshotJob = { symbol: string; request: () => Promise<SnapshotResponse>; resolve: (value: SnapshotResponse) => void; reject: (error: unknown) => void };
type SymbolMetric = { state: string; attempts: number; retries: number; failures: number; successes: number; lastStatus: number | null; lastError: string | null; totalLatencyMs: number; lastLatencyMs: number | null };

/** Serializes Binance snapshot traffic (IP limits are shared across symbols and processes). */
export class BinanceSnapshotScheduler {
  private readonly queue: SnapshotJob[] = [];
  private readonly inFlight = new Map<string, Promise<SnapshotResponse>>();
  private readonly now: () => number;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly random: () => number;
  private readonly minIntervalMs: number;
  private readonly maxRetries: number;
  private readonly backoffBaseMs: number;
  private readonly maxBackoffMs: number;
  private lastRequestAt = Number.NEGATIVE_INFINITY;
  private blockedUntil = 0;
  private running = false;
  private lastError: string | null = null;
  private latestStatus: number | null = null;
  private readonly weightHeaders: Record<string, string> = {};
  private readonly symbolMetrics = new Map<string, SymbolMetric>();
  private activeRequests = 0;
  private maxObservedConcurrency = 0;
  private readonly metrics = { requests: 0, successes: 0, failures: 0, retries: 0, rateLimits: 0, clientErrors: 0, serverErrors: 0, timeouts: 0, totalLatencyMs: 0, maxLatencyMs: 0 };

  constructor(options: SnapshotSchedulerOptions = {}) {
    this.minIntervalMs = options.minIntervalMs ?? positiveEnv('BINANCE_SNAPSHOT_MIN_INTERVAL_MS', 1_000);
    this.maxRetries = options.maxRetries ?? positiveEnv('BINANCE_SNAPSHOT_MAX_RETRIES', 3);
    this.backoffBaseMs = options.backoffBaseMs ?? positiveEnv('BINANCE_SNAPSHOT_BACKOFF_BASE_MS', 1_000);
    this.maxBackoffMs = options.maxBackoffMs ?? positiveEnv('BINANCE_SNAPSHOT_MAX_BACKOFF_MS', 30_000);
    this.now = options.now ?? Date.now;
    this.sleep = options.sleep ?? wait;
    this.random = options.random ?? Math.random;
  }

  request(symbol: string, request: () => Promise<SnapshotResponse>): Promise<SnapshotResponse> {
    const normalized = symbol.toUpperCase();
    const existing = this.inFlight.get(normalized);
    if (existing) return existing;
    let resolve!: (value: SnapshotResponse) => void;
    let reject!: (error: unknown) => void;
    const result = new Promise<SnapshotResponse>((yes, no) => { resolve = yes; reject = no; });
    this.inFlight.set(normalized, result);
    this.getSymbolMetric(normalized).state = 'QUEUED';
    this.queue.push({ symbol: normalized, request, resolve, reject });
    void this.pump();
    return result;
  }

  telemetry() {
    const pending = this.queue.map((job) => job.symbol);
    return {
      queueDepth: pending.length, inFlight: [...this.inFlight.keys()], lastRequestAt: Number.isFinite(this.lastRequestAt) ? this.lastRequestAt : null,
      cooldownRemainingMs: Math.max(0, this.blockedUntil - this.now()), latestStatus: this.latestStatus, lastError: this.lastError,
      weightHeaders: { ...this.weightHeaders }, metrics: { ...this.metrics,
        averageLatencyMs: this.metrics.requests ? this.metrics.totalLatencyMs / this.metrics.requests : null,
      }, perSymbol: Object.fromEntries([...this.symbolMetrics].map(([symbol, stats]) => [symbol, { ...stats }])),
      currentConcurrency: this.activeRequests, maxObservedConcurrency: this.maxObservedConcurrency,
    };
  }

  private async pump() {
    if (this.running) return;
    this.running = true;
    try {
      while (this.queue.length) {
        const job = this.queue.shift()!;
        try { job.resolve(await this.execute(job)); }
        catch (error) { job.reject(error); }
        finally { this.inFlight.delete(job.symbol); }
      }
    } finally { this.running = false; }
  }

  private async execute(job: SnapshotJob): Promise<SnapshotResponse> {
    for (let attempt = 0; ; attempt += 1) {
      const waitMs = Math.max(this.blockedUntil - this.now(), this.lastRequestAt + this.minIntervalMs - this.now(), 0);
      if (waitMs > 0) await this.sleep(waitMs);
      const startedAt = this.now();
      this.lastRequestAt = startedAt;
      this.metrics.requests += 1;
      const symbolStats = this.getSymbolMetric(job.symbol);
      symbolStats.state = 'SNAPSHOTTING';
      symbolStats.attempts += 1;
      this.activeRequests += 1;
      this.maxObservedConcurrency = Math.max(this.maxObservedConcurrency, this.activeRequests);
      try {
        const response = await job.request();
        const latency = Math.max(0, this.now() - startedAt);
        this.recordLatency(latency);
        this.latestStatus = response.status;
        symbolStats.lastStatus = response.status;
        symbolStats.lastLatencyMs = latency;
        symbolStats.totalLatencyMs += latency;
        this.captureWeight(response.headers);
        if (response.ok) { this.metrics.successes += 1; symbolStats.successes += 1; symbolStats.state = 'SNAPSHOT_RECEIVED'; symbolStats.lastError = null; this.lastError = null; return response; }
        const detail = `Binance depth snapshot HTTP ${response.status} for ${job.symbol}`;
        this.lastError = detail;
        symbolStats.lastError = detail;
        if (response.status >= 400 && response.status < 500) this.metrics.clientErrors += 1;
        const retryable = response.status === 429 || response.status === 418 || response.status >= 500;
        if (response.status === 429 || response.status === 418) {
          this.metrics.rateLimits += 1;
          const retryAfter = parseRetryAfter(response.headers.get('retry-after'), this.now());
          this.blockedUntil = Math.max(this.blockedUntil, this.now() + (retryAfter ?? this.backoff(attempt)));
          await this.sleep(Math.max(0, this.blockedUntil - this.now()));
        } else if (response.status >= 500) this.metrics.serverErrors += 1;
        if (!retryable || attempt >= this.maxRetries) { symbolStats.state = 'FAILED'; throw new Error(detail); }
        symbolStats.state = 'BACKOFF';
        this.metrics.retries += 1;
        symbolStats.retries += 1;
      } catch (error) {
        // HTTP errors above are already classified; transport errors are retried with bounded backoff.
        if (error instanceof Error && /^Binance depth snapshot HTTP/.test(error.message)) {
          this.metrics.failures += 1;
          symbolStats.failures += 1;
          symbolStats.state = 'FAILED';
          throw error;
        }
        const text = error instanceof Error ? error.message : String(error);
        this.lastError = text;
        symbolStats.lastError = text;
        if (/timeout|aborted/i.test(text)) this.metrics.timeouts += 1;
        if (attempt >= this.maxRetries) { this.metrics.failures += 1; symbolStats.failures += 1; symbolStats.state = 'FAILED'; throw error; }
        symbolStats.state = 'BACKOFF';
        this.metrics.retries += 1;
        symbolStats.retries += 1;
        this.blockedUntil = Math.max(this.blockedUntil, this.now() + this.backoff(attempt));
      } finally {
        this.activeRequests = Math.max(0, this.activeRequests - 1);
      }
    }
  }

  private backoff(attempt: number) {
    const base = Math.min(this.maxBackoffMs, this.backoffBaseMs * 2 ** Math.min(attempt, 20));
    return base + Math.floor(this.random() * Math.max(1, base * 0.2));
  }
  private recordLatency(ms: number) { this.metrics.totalLatencyMs += ms; this.metrics.maxLatencyMs = Math.max(this.metrics.maxLatencyMs, ms); }
  private getSymbolMetric(symbol: string) {
    let stats = this.symbolMetrics.get(symbol);
    if (!stats) { stats = { state: 'PENDING', attempts: 0, retries: 0, failures: 0, successes: 0, lastStatus: null, lastError: null, totalLatencyMs: 0, lastLatencyMs: null }; this.symbolMetrics.set(symbol, stats); }
    return stats;
  }
  private captureWeight(headers: SnapshotResponse['headers']) {
    for (const name of ['x-mbx-used-weight-1m', 'x-mbx-used-weight-1s', 'x-mbx-used-weight-1h', 'x-mbx-used-weight-1d']) {
      const value = headers.get(name);
      if (value !== null) this.weightHeaders[name] = value;
    }
  }
}

function parseRetryAfter(value: string | null, now: number): number | null {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1_000;
  const date = Date.parse(value);
  return Number.isFinite(date) ? Math.max(0, date - now) : null;
}
function positiveEnv(name: string, fallback: number) {
  const parsed = Number(process.env[name]);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}
function wait(ms: number) { return new Promise<void>((resolve) => setTimeout(resolve, ms)); }
