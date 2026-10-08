import { afterEach, describe, expect, it } from 'vitest';
import { MarketDataRuntime, runtimeOptionsFromEnv } from './runtime';

const runtimes: MarketDataRuntime[] = [];
afterEach(async () => { await Promise.all(runtimes.splice(0).map((runtime) => runtime.stop())); });

describe('market-data runtime boundary', () => {
  it('reads controlled rollout settings without changing the legacy scanner port', () => {
    expect(runtimeOptionsFromEnv({ MAX_LIVE_PAIRS: '3', MARKET_DATA_PORT: '4101', LIVE_PAIRS: 'BTC/USDT,ETH/USDT' })).toEqual({
      reportPath: 'data/discovery/latest.json', maxPairs: 3, pairs: ['BTC/USDT', 'ETH/USDT'], depthLevels: 20, port: 4101,
      exchanges: ['binance', 'bybit', 'okx', 'kucoin', 'gate', 'bitget', 'htx', 'crypto.com', 'coinbase'],
      requiredExchanges: [], connectExchanges: true,
    });
  });

  it('rejects malformed limits rather than silently widening the rollout', () => {
    expect(() => runtimeOptionsFromEnv({ MAX_LIVE_PAIRS: '0' })).toThrow('MAX_LIVE_PAIRS');
    expect(() => runtimeOptionsFromEnv({ MARKET_DATA_PORT: '4000' })).not.toThrow();
    expect(() => runtimeOptionsFromEnv({ MARKET_DATA_EXCHANGES: 'binance,unknown' })).toThrow('MARKET_DATA_EXCHANGES');
    expect(runtimeOptionsFromEnv({ MARKET_DATA_EXCHANGES: 'okx' }).exchanges).toEqual(['okx']);
    expect(runtimeOptionsFromEnv({ MARKET_DATA_EXCHANGES: 'kucoin' }).exchanges).toEqual(['kucoin']);
    expect(runtimeOptionsFromEnv({ MARKET_DATA_EXCHANGES: 'mexc' }).exchanges).toEqual(['mexc']);
    expect(runtimeOptionsFromEnv({ MARKET_DATA_EXCHANGES: 'gate' }).exchanges).toEqual(['gate']);
    expect(runtimeOptionsFromEnv({ MARKET_DATA_EXCHANGES: 'bitget' }).exchanges).toEqual(['bitget']);
    expect(runtimeOptionsFromEnv({ MARKET_DATA_EXCHANGES: 'htx' }).exchanges).toEqual(['htx']);
    expect(runtimeOptionsFromEnv({ MARKET_DATA_REQUIRED_EXCHANGES: 'none' }).requiredExchanges).toEqual([]);
    expect(() => runtimeOptionsFromEnv({ MARKET_DATA_EXCHANGES: 'okx', MARKET_DATA_REQUIRED_EXCHANGES: 'binance' })).toThrow('subset of MARKET_DATA_EXCHANGES');
  });

  it('consumes Phase A universe and exposes resource/market telemetry in isolation', async () => {
    const runtime = await MarketDataRuntime.create({ reportPath: 'data/discovery/latest.json', maxPairs: 3,
      pairs: ['BTC/USDT', 'ETH/USDT', 'SOL/USDT'], depthLevels: 20, port: 0, connectExchanges: false });
    runtimes.push(runtime);
    expect(runtime.universe.selectedPairCount).toBe(3);
    expect(runtime.universe.subscriptions.length).toBeGreaterThan(3);
    const telemetry = runtime.telemetry();
    expect(telemetry.status).toBe('STARTING');
    expect(telemetry.selectedPairs).toBe(3);
    expect(telemetry.resources.activeWebSocketConnections).toBe(0);
    expect(telemetry.resources.rssBytes).toBeGreaterThan(0);
    expect(telemetry.bitgetSnapshotBootstrap).toMatchObject({ pairsRequested: 0, snapshotsCompleted: 0 });
    expect(telemetry.htxSnapshotBootstrap).toMatchObject({ pairsRequested: 0, snapshotsCompleted: 0 });
    expect(telemetry.resources.systemMemoryTotalBytes).toBeGreaterThan(0);
    expect(telemetry.resources.systemMemoryAvailableBytes).toBeGreaterThan(0);
    expect(telemetry.resources.systemCpuPercent).not.toBeUndefined();
    expect(telemetry.resources.networkRxBytesPerSecond).not.toBeUndefined();
    expect(telemetry.marketStateCounts).toEqual({ synchronized: 0, quiet: 0, stale: 0, failed: 0, pending: telemetry.books });
    expect(telemetry.markets[0].diagnostics).toMatchObject({ pending: true, synchronized: false, failed: false,
      messagesReceived: 0, updatesApplied: 0, everSynchronized: false });
    expect(Object.values(telemetry.exchanges).every((item) => item?.synchronizedBooks === 0)).toBe(true);
    await runtime.start();
    expect(runtime.telemetry().status).toBe('RUNNING');
  });

  it('supports a sparse selected universe while retaining every requested adapter', async () => {
    const runtime = await MarketDataRuntime.create({ reportPath: 'data/discovery/latest.json', maxPairs: 10,
      depthLevels: 20, port: 0, exchanges: ['binance', 'bybit', 'okx', 'kucoin', 'mexc', 'gate', 'bitget', 'htx', 'crypto.com', 'coinbase'],
      requiredExchanges: [], connectExchanges: false });
    runtimes.push(runtime);
    expect(runtime.universe.selectedPairCount).toBe(10);
    expect(new Set(runtime.universe.subscriptions.map((item) => item.exchange)).size).toBeGreaterThan(1);
    expect(runtime.universe.subscriptions.some((item) => item.exchange === 'coinbase')).toBe(true);
  });
});
