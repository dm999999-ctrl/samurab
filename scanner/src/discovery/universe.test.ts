import { describe, expect, it } from 'vitest';
import { buildDiscoveryReport, buildMarketPruningTiers } from './universe';
import { discoveryConfig, SUPPORTED_EXCHANGES } from './engine';
import { canonicalizeMarket } from './symbol-map';
import { selectLiveUniverse } from '../market-data/universe';
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
  it('builds a real multi-quote selected-token market universe without synthesizing pairs', () => {
    const exchanges = ['binance','bybit'] as const;
    const rows = exchanges.map((exchange) => result(exchange, [
      market(exchange, 'BTC', 'USDT', 100), market(exchange, 'BTC', 'USDC', 50),
      market(exchange, 'ETH', 'BTC', 20), market(exchange, 'BTC', 'ETH', 15),
      market(exchange, 'USDC', 'USDT', 999),
    ]));
    const report = buildDiscoveryReport(rows, config({ target: 2, supportedMarketQuotes: ['USDT','USDC','BTC','ETH'], maxSelectedTokenMarkets: 20 }), 1, 2, 'fixture');
    expect(report.schemaVersion).toBe(1);
    expect(report.selectedTokenIds).toEqual(['asset:ETH','asset:BTC']);
    expect(report.selectedTokenMarkets?.map((item) => item.canonicalPair).sort()).toEqual(['BTC/ETH','BTC/USDC','BTC/USDT','ETH/BTC']);
    expect(report.eligibleMarketCount).toBe(4);
    expect(report.projectedExchangeBookCount).toBe(8);
    expect(report.exactCrossExchangeMarketMatchCount).toBe(4);
    expect(report.selectedTokenMarkets?.some((item) => item.canonicalPair === 'USDC/USDT')).toBe(false);
    expect(report.selectedTokenMarkets?.find((item) => item.canonicalPair === 'ETH/BTC')?.markets[0].exchangeSymbol).toBe('ETHBTC');
    expect(report.selectedTokenMarkets?.find((item) => item.canonicalPair === 'BTC/USDT')?.quoteAsset).toBe('USDT');
    expect(report.selectedTokenMarkets?.find((item) => item.canonicalPair === 'BTC/USDC')?.quoteAsset).toBe('USDC');
  });
  it('ranks and caps markets deterministically by exact cross-exchange coverage and liquidity', () => {
    const report = buildDiscoveryReport([
      result('binance', [market('binance','BTC','USDT',100), market('binance','ETH','USDT',1000), market('binance','SOL','USDT',200)]),
      result('bybit', [market('bybit','BTC','USDT',100), market('bybit','SOL','USDT',300)]),
    ], config({ target: 3, supportedMarketQuotes: ['USDT','USDC','BTC','ETH'], maxSelectedTokenMarkets: 2, selectedTokenSymbols: ['BTC','ETH','SOL'] }), 1, 2, 'fixture');
    expect(report.selectedTokenMarkets?.map((item) => item.canonicalPair)).toEqual(['SOL/USDT','BTC/USDT']);
    expect(report.eligibleMarketCount).toBe(3);
    expect(report.projectedExchangeBookCount).toBe(4);
  });
  it('retains partial exchange availability and does not infer missing listings', () => {
    const report = buildDiscoveryReport([
      result('binance', [market('binance','BTC','ETH',5), market('binance','ETH','BTC',4)]),
      result('bybit', []),
    ], config({ target: 2, supportedMarketQuotes: ['USDT','USDC','BTC','ETH'], selectedTokenSymbols: ['BTC','ETH'] }), 1, 2, 'fixture');
    expect(report.selectedTokenMarkets?.map((item => item.canonicalPair)).sort()).toEqual(['BTC/ETH','ETH/BTC']);
    expect(report.selectedTokenMarkets?.every((item) => item.exchangeCount === 1)).toBe(true);
    expect(report.projectedExchangeBookCount).toBe(2);
    expect(report.exactCrossExchangeMarketMatchCount).toBe(0);
  });
  it('excludes single-exchange markets from A/B and reports exact-pair comparison and book counts', () => {
    const listing = (pair: string, exchanges: Array<NormalizedSpotMarket['exchange']>, volume: number) => {
      const markets = exchanges.map((exchange) => market(exchange, ...pair.split('/') as [string,string]));
      return { canonicalPair: pair, baseAsset: pair.split('/')[0], quoteAsset: pair.split('/')[1], selectedAssetIds: [`asset:${pair.split('/')[0]}`],
        selectedTokenRelevance: 1 as const,
        exchangeCount: exchanges.length, exchanges, totalQuoteVolume24hByExchange: {}, totalQuoteVolume24h: volume, liquidityScore: 1, quotePriority: 0,
        activityScore: 1, rank: 0, markets };
    };
    const source = [
      listing('BTC/USDT',['binance','bybit','okx'],100),
      listing('ETH/BTC',['binance','gate'],50),
      listing('SOL/USDC',['coinbase'],20),
    ].map((entry, index) => ({ ...entry, rank: index + 1 }));
    const [a,b,c] = buildMarketPruningTiers(source);
    expect(a.markets.map((m) => m.canonicalPair)).toEqual(['BTC/USDT','ETH/BTC']);
    expect(a.marketCount).toBe(2);
    expect(a.exchangeBookCount).toBe(5);
    expect(a.quoteDistribution).toEqual({ USDT: 1, BTC: 1 });
    expect(a.possibleExactCrossExchangeComparisons).toBe(4); // 3 choose 2 + 2 choose 2
    expect(b.exchangeBookCount).toBe(5);
    expect(c.marketCount).toBe(3);
    expect(c.exchangeBookCount).toBe(6);
    expect(c.possibleExactCrossExchangeComparisons).toBe(4);
    expect(c.exchangesByMarket['SOL/USDC']).toEqual(['coinbase']);
  });
  it('caps Tier A and Tier B deterministically while keeping the full Tier C catalog view', () => {
    const listings = Array.from({ length: 45 }, (_, index) => {
      const base = `T${index}`;
      const exchanges = ['binance','bybit'] as const;
      return { canonicalPair: `${base}/USDT`, baseAsset: base, quoteAsset: 'USDT', selectedAssetIds: [`asset:${base}`],
        selectedTokenRelevance: 1 as const,
        exchangeCount: 2, exchanges: [...exchanges], totalQuoteVolume24hByExchange: {}, totalQuoteVolume24h: 1000 - index, liquidityScore: 1,
        quotePriority: 0, activityScore: 1, rank: index + 1,
        markets: exchanges.map((exchange) => market(exchange, base, 'USDT')) };
    });
    const [a,b,c] = buildMarketPruningTiers(listings);
    expect(a.marketCount).toBe(20);
    expect(a.exchangeBookCount).toBe(40);
    expect(b.marketCount).toBe(40);
    expect(b.exchangeBookCount).toBe(80);
    expect(c.marketCount).toBe(45);
    expect(c.exchangeBookCount).toBe(90);
    expect(a.markets[0].canonicalPair).toBe('T0/USDT');
  });
  it('assigns selected-token relevance for base, quote, both, and neither sides', () => {
    const rows = [
      market('binance', 'BTC', 'USDT', 100), // selected base only
      market('binance', 'SOL', 'BTC', 90),   // selected quote only
      market('binance', 'BTC', 'ETH', 80),   // both selected
      market('binance', 'SOL', 'USDT', 70),  // neither selected
    ];
    const report = buildDiscoveryReport([result('binance', rows), result('bybit', rows.map((entry) => ({ ...entry, exchange: 'bybit' as const })))],
      config({ selectedTokenSymbols: ['BTC','ETH'], supportedMarketQuotes: ['USDT','USDC','BTC','ETH'] }), 1, 2, 'relevance');
    const byPair = new Map(report.selectedTokenMarkets?.map((item) => [item.canonicalPair, item]));
    expect(byPair.get('BTC/USDT')?.selectedTokenRelevance).toBe(1);
    expect(byPair.get('SOL/BTC')?.selectedTokenRelevance).toBe(1);
    expect(byPair.get('BTC/ETH')?.selectedTokenRelevance).toBe(2);
    expect(byPair.has('SOL/USDT')).toBe(false);
    expect(byPair.get('SOL/BTC')?.selectedAssetIds).toEqual(['asset:BTC']);
    expect(byPair.get('BTC/ETH')?.selectedAssetIds).toEqual(['asset:BTC','asset:ETH']);
  });
  it('defaults F2 to an explicit top-25 token slice while retaining all 1,000 Phase A assets', () => {
    const rows = Array.from({ length: 30 }, (_, index) => market('binance', `ASSET${String(index).padStart(2,'0')}`, 'USDT', 100 - index));
    const report = buildDiscoveryReport([result('binance', rows), result('bybit', rows.map((entry) => ({ ...entry, exchange: 'bybit' as const })))],
      config({ target: 30, selectedTokenLimit: 25 }), 1, 2, 'default-selection');
    expect(report.selectedAssetCount).toBe(30);
    expect(report.selectedTokenIds).toHaveLength(25);
    expect(report.selectedTokenIds).toContain('asset:ASSET00');
    expect(report.selectedTokenIds).not.toContain('asset:ASSET29');
  });
  it('keeps an F2 discovery report consumable by the unchanged Phase B selector', () => {
    const report = buildDiscoveryReport([
      result('binance', [market('binance','BTC','USDT',100)]),
      result('bybit', [market('bybit','BTC','USDT',90)]),
    ], config({ selectedTokenSymbols: ['BTC'], supportedMarketQuotes: ['USDT','USDC','BTC','ETH'] }), 1, 2, 'phase-b-compat');
    expect(report.schemaVersion).toBe(1);
    const live = selectLiveUniverse(report, { maxPairs: 1, pairs: ['BTC/USDT'], exchanges: ['binance','bybit'], requiredExchanges: [] });
    expect(live.selectedPairCount).toBe(1);
    expect(live.subscriptions.map((subscription) => [subscription.exchange, subscription.canonicalPair])).toEqual([
      ['binance','BTC/USDT'], ['bybit','BTC/USDT'],
    ]);
  });
});
