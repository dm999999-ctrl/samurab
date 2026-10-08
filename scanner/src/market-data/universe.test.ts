import { describe, expect, it } from 'vitest';
import { selectLiveUniverse } from './universe';
import type { DiscoveryReport, EligiblePair, NormalizedSpotMarket } from '../discovery/types';

function market(exchange: NormalizedSpotMarket['exchange'], pair: string, symbol: string): NormalizedSpotMarket {
  const [baseAsset, quoteAsset] = pair.split('/');
  return { exchange, exchangeSymbol: symbol, baseAsset, quoteAsset, canonicalAssetId: `asset:${baseAsset}`, canonicalPair: pair, marketType: 'spot', status: 'TRADING', assetMapping: 'exact-symbol' };
}
function pair(canonicalPair: string, markets: NormalizedSpotMarket[]): EligiblePair {
  const [baseAsset, quoteAsset] = canonicalPair.split('/');
  return { canonicalPair, baseAsset, quoteAsset, exchangeCount: new Set(markets.map((m) => m.exchange)).size, exchanges: markets.map((m) => m.exchange), markets, status: 'ELIGIBLE' };
}
function report(pairs: EligiblePair[]): DiscoveryReport {
  return { schemaVersion: 1, runId: 'phase-a-fixture', startedAt: 1, completedAt: 2, status: 'COMPLETE', config: { target: 1000, minExchangeCount: 2, supportedExchanges: ['binance','bybit'], majorQuoteAssets: ['USDT'], meaningfulVolumeMinimum: 0, scoreWeights: { coverage: 35, liquidity: 35, liquidExchanges: 15, quoteAvailability: 10, activity: 5 } }, exchanges: [], marketsDiscovered: 0, uniqueCanonicalAssetsDiscovered: 0, nonStableAssetsEligible: 0, selectedAssetCount: 0, quoteAssets: [], assets: [], eligiblePairs: pairs, pairCount: pairs.length, coverageComplete: true, methodology: { scoring: '', missingData: '', volumeAggregation: '', stablecoinClassification: '', marketLiquidityDisclaimer: '' } };
}
describe('Phase A live-universe selection', () => {
  const btc = pair('BTC/USDT', [market('binance','BTC/USDT','BTCUSDT'), market('bybit','BTC/USDT','BTCUSDT')]);
  const eth = pair('ETH/USDT', [market('okx','ETH/USDT','ETH-USDT'), market('gate','ETH/USDT','ETH_USDT')]);
  const sol = pair('SOL/USDT', [market('mexc','SOL/USDT','SOLUSDT'), market('coinbase','SOL/USDT','SOL-USDT')]);
  it('honors pair caps and retains exact exchange-native instrument identifiers', () => {
    const selected = selectLiveUniverse(report([btc, eth, sol]), { maxPairs: 2, pairs: ['BTC/USDT','ETH/USDT'] });
    expect(selected.sourceRunId).toBe('phase-a-fixture');
    expect(selected.candidatePairCount).toBe(2);
    expect(selected.selectedPairCount).toBe(2);
    expect(selected.subscriptions.find((x) => x.exchange === 'gate')).toMatchObject({ canonicalPair: 'ETH/USDT', exchangeSymbol: 'ETH_USDT' });
  });
  it('uses only candidate pairs and rejects requested pairs not in the Phase A eligible set', () => {
    expect(() => selectLiveUniverse(report([btc]), { pairs: ['DOGE/USDT'] })).toThrow('absent from Phase A');
  });
  it('keeps the required Binance/Bybit pair set while including only available OKX markets', () => {
    const selected = selectLiveUniverse(report([btc, eth]), { maxPairs: 2, exchanges: ['binance', 'bybit', 'okx'], requiredExchanges: ['binance', 'bybit'] });
    expect(selected.selectedPairCount).toBe(1);
    expect(selected.subscriptions.map((item) => item.exchange)).toEqual(['binance', 'bybit']);
    const okxOnly = selectLiveUniverse(report([btc, eth]), { maxPairs: 1, exchanges: ['okx'], requiredExchanges: ['okx'], pairs: ['ETH/USDT'] });
    expect(okxOnly.subscriptions).toMatchObject([{ exchange: 'okx', exchangeSymbol: 'ETH-USDT' }]);
  });
  it('selects KuCoin-native Spot symbols from the Phase A report', () => {
    const kucoin = pair('DOGE/USDT', [market('kucoin','DOGE/USDT','DOGE-USDT')]);
    const selected = selectLiveUniverse(report([kucoin]), { maxPairs: 1, exchanges: ['kucoin'], requiredExchanges: ['kucoin'] });
    expect(selected.subscriptions).toMatchObject([{ exchange: 'kucoin', canonicalPair: 'DOGE/USDT', exchangeSymbol: 'DOGE-USDT' }]);
  });
  it('filters Coinbase Phase A candidates against the live public product catalog before applying the cap', () => {
    const stale = pair('BTC/USDC', [market('coinbase', 'BTC/USDC', 'BTC-USDC')]);
    const online = pair('ETH/USD', [market('coinbase', 'ETH/USD', 'ETH-USD')]);
    const selected = selectLiveUniverse(report([stale, online]), { maxPairs: 1, exchanges: ['coinbase'], requiredExchanges: ['coinbase'],
      eligibleSymbols: { coinbase: ['ETH-USD'] } });
    expect(selected.candidatePairCount).toBe(1);
    expect(selected.selectedPairCount).toBe(1);
    expect(selected.subscriptions).toMatchObject([{ exchange: 'coinbase', canonicalPair: 'ETH/USD', exchangeSymbol: 'ETH-USD' }]);
  });
  it('applies Coinbase online-product filtering to sparse selections without emptying other exchanges', () => {
    const staleOnly = pair('HYPE/USDT', [market('coinbase', 'HYPE/USDT', 'HYPE-USDT')]);
    const selected = selectLiveUniverse(report([staleOnly, sol]), { maxPairs: 10, exchanges: ['coinbase', 'mexc'], requiredExchanges: [],
      eligibleSymbols: { coinbase: ['ETH-USD'] } });
    expect(selected.candidatePairCount).toBe(1);
    expect(selected.selectedPairCount).toBe(1);
    expect(selected.subscriptions.map((item) => item.exchange)).toEqual(['mexc']);
  });
  it('rejects failed discovery reports and invalid caps', () => {
    expect(() => selectLiveUniverse({ ...report([btc]), status: 'FAILED' })).toThrow('invalid or failed');
    expect(() => selectLiveUniverse(report([btc]), { maxPairs: 0 })).toThrow('positive integer');
  });
});
