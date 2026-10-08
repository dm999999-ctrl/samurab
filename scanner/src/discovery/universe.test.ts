import { describe, expect, it } from 'vitest';
import { buildDiscoveryReport } from './universe';
import { discoveryConfig, SUPPORTED_EXCHANGES } from './engine';
import { canonicalizeMarket } from './symbol-map';
import type { ExchangeDiscoveryResult, NormalizedSpotMarket } from './types';

function market(exchange: NormalizedSpotMarket['exchange'], base: string, quote: string, volume?: number): NormalizedSpotMarket {
  return canonicalizeMarket({
    exchange, exchangeSymbol: exchange === 'gate' ? `${base}_${quote}` : `${base}${quote}`,
    baseAsset: base, quoteAsset: quote, marketType: 'spot', status: 'online',
    ...(volume === undefined ? {} : { volumeQuote24h: volume, lastPrice: 10 }),
  });
}
function result(exchange: NormalizedSpotMarket['exchange'], markets: NormalizedSpotMarket[], status: ExchangeDiscoveryResult['status'] = 'AVAILABLE'): ExchangeDiscoveryResult {
  return {
    exchange, status, discoveredAt: 1, marketCount: markets.length, markets,
    marketSource: { status, endpoint: 'fixture' }, warnings: [], durationMs: 0,
  };
}
function config(overrides: Partial<ReturnType<typeof discoveryConfig>> = {}) {
  return { ...discoveryConfig({}), ...overrides, supportedExchanges: ['binance','bybit'] as NormalizedSpotMarket['exchange'][], majorQuoteAssets: ['USDT'], scoreWeights: { coverage: 35, liquidity: 35, liquidExchanges: 15, quoteAvailability: 10, activity: 5 } };
}
describe('dynamic universe selection', () => {
  it('retains assets when discovery is below 1,000 and excludes stablecoin and leveraged bases', () => {
    const b = result('binance', [market('binance','BTC','USDT',100), market('binance','USDT','BTC',10), market('binance','ETHUP','USDT',50)]);
    const y = result('bybit', [market('bybit','BTC','USDT',80), market('bybit','USDT','BTC',10), market('bybit','ETHUP','USDT',50)]);
    const report = buildDiscoveryReport([b,y], config(), 1, 2, 'fixture');
    expect(report.nonStableAssetsEligible).toBe(1);
    expect(report.selectedAssetCount).toBe(1);
    expect(report.assets[0].symbol).toBe('BTC');
    expect(report.assets[0].rank).toBe(1);
    expect(report.status).toBe('COMPLETE');
  });
  it('caps output at 1,000 with deterministic score tie-breaking', () => {
    const bases = Array.from({ length: 1005 }, (_, i) => `COIN${String(i).padStart(4,'0')}`);
    const markets = bases.map((base) => market('binance',base,'USDT',10));
    const report = buildDiscoveryReport([result('binance',markets), result('bybit',markets.map((m) => ({ ...m, exchange: 'bybit' as const })))], config({ target: 1000 }), 1, 2, 'fixture');
    expect(report.assets).toHaveLength(1000);
    expect(report.assets[0].symbol).toBe('COIN0000');
    expect(report.assets[999].symbol).toBe('COIN0999');
  });
  it('does not blend volumes across quote currencies and reports mixed-unit totals as null', () => {
    const entries = ['binance','bybit'] as const;
    const all = entries.map((exchange) => result(exchange, [market(exchange,'BTC','USDT',100), market(exchange,'BTC','BTCX',20)]));
    const report = buildDiscoveryReport(all, config(), 1, 2, 'fixture');
    const asset = report.assets[0];
    expect(asset.totalVolume24hByQuoteAsset).toEqual({ BTCX: 40, USDT: 200 });
    expect(asset.totalVolume24h).toBeNull();
    expect(asset.medianVolume24h).toBeNull();
  });
  it('marks output partial and never treats failed discovery as zero venues', () => {
    const failed: ExchangeDiscoveryResult = { ...result('bybit', []), status: 'RATE_LIMITED', marketSource: { status: 'RATE_LIMITED', endpoint: 'fixture', httpStatus: 429, error: 'rate limited' }, warnings: ['rate limited'] };
    const report = buildDiscoveryReport([result('binance',[market('binance','BTC','USDT',1)]), failed], config(), 1, 2, 'fixture');
    expect(report.status).toBe('PARTIAL');
    expect(report.coverageComplete).toBe(false);
    expect(report.assets).toHaveLength(0);
    expect(report.exchanges[1].status).toBe('RATE_LIMITED');
  });
  it('unifies only explicit aliases and preserves exchange-native instrument symbols', () => {
    const alias = market('binance','XBT','USDT',10);
    const btc = market('bybit','BTC','USDT',10);
    const report = buildDiscoveryReport([result('binance',[alias]), result('bybit',[btc])], config(), 1, 2, 'fixture');
    expect(report.assets[0].assetId).toBe('asset:BTC');
    expect(report.assets[0].symbol).toBe('BTC');
    expect(report.eligiblePairs[0].canonicalPair).toBe('BTC/USDT');
    expect(report.eligiblePairs[0].markets.some((listing) => listing.exchangeSymbol === 'XBTUSDT')).toBe(true);
    expect(SUPPORTED_EXCHANGES).toHaveLength(10);
  });
});
