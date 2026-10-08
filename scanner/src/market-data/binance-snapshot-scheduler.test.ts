import { describe, expect, it } from 'vitest';
import { BinanceSnapshotScheduler, type SnapshotResponse } from './binance-snapshot-scheduler';

describe('Binance snapshot scheduler', () => {
  it('deduplicates same-symbol work, serializes requests, and captures used-weight telemetry', async () => {
    let calls = 0;
    let active = 0;
    let maximumActive = 0;
    let now = 1_000;
    const delays: number[] = [];
    const scheduler = new BinanceSnapshotScheduler({ minIntervalMs: 250, maxRetries: 0, now: () => now,
      sleep: async (ms) => { delays.push(ms); now += ms; }, random: () => 0 });
    const request = async () => {
      calls += 1; active += 1; maximumActive = Math.max(maximumActive, active);
      await Promise.resolve(); active -= 1;
      return response(200, { 'x-mbx-used-weight-1m': String(calls) });
    };
    const [a, b, c] = await Promise.all([
      scheduler.request('BTCUSDT', request), scheduler.request('btcusdt', request), scheduler.request('ETHUSDT', request),
    ]);
    expect([a.status, b.status, c.status]).toEqual([200, 200, 200]);
    expect(calls).toBe(2);
    expect(maximumActive).toBe(1);
    expect(delays).toContain(250);
    expect(scheduler.telemetry().weightHeaders['x-mbx-used-weight-1m']).toBe('2');
  });

  it('honors Retry-After globally after 429 and retries the failed request', async () => {
    let now = 5_000;
    const delays: number[] = [];
    let calls = 0;
    const scheduler = new BinanceSnapshotScheduler({ minIntervalMs: 0, maxRetries: 1, backoffBaseMs: 10,
      now: () => now, sleep: async (ms) => { delays.push(ms); now += ms; }, random: () => 0 });
    const result = await scheduler.request('BTCUSDT', async () => {
      calls += 1;
      return calls === 1 ? response(429, {}, '2') : response(200);
    });
    expect(result.status).toBe(200);
    expect(calls).toBe(2);
    expect(delays).toContain(2_000);
    expect(scheduler.telemetry().metrics.rateLimits).toBe(1);
    expect(scheduler.telemetry().metrics.retries).toBe(1);
  });

  it('uses bounded retry for server errors and does not retry other client errors', async () => {
    let serverCalls = 0;
    const scheduler = new BinanceSnapshotScheduler({ minIntervalMs: 0, maxRetries: 2, backoffBaseMs: 1,
      now: () => 1_000, sleep: async () => {}, random: () => 0 });
    const ok = await scheduler.request('BTCUSDT', async () => {
      serverCalls += 1; return serverCalls < 3 ? response(503) : response(200);
    });
    expect(ok.status).toBe(200);
    expect(serverCalls).toBe(3);
    await expect(scheduler.request('ETHUSDT', async () => response(400))).rejects.toThrow('HTTP 400');
    expect(scheduler.telemetry().metrics.serverErrors).toBe(2);
    expect(scheduler.telemetry().metrics.retries).toBe(2);
  });

  it('fails after the bounded retry count instead of looping indefinitely', async () => {
    let calls = 0;
    const scheduler = new BinanceSnapshotScheduler({ minIntervalMs: 0, maxRetries: 1, backoffBaseMs: 1,
      now: () => 1_000, sleep: async () => {}, random: () => 0 });
    await expect(scheduler.request('BTCUSDT', async () => { calls += 1; return response(500); })).rejects.toThrow('HTTP 500');
    expect(calls).toBe(2);
    expect(scheduler.telemetry().metrics.failures).toBe(1);
  });
});

function response(status: number, headers: Record<string, string> = {}, retryAfter?: string): SnapshotResponse {
  const values = new Map(Object.entries({ ...headers, ...(retryAfter ? { 'retry-after': retryAfter } : {}) }).map(([key, value]) => [key.toLowerCase(), value]));
  return { status, ok: status >= 200 && status < 300, headers: { get: (name) => values.get(name.toLowerCase()) ?? null }, json: async () => ({}) };
}
