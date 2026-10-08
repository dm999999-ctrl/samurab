import { describe, expect, it } from 'vitest';
import type { ArbitrageOpportunity } from './detection';
import type { OrderBookState } from '../market-data/types';
import { estimateOpportunityProfitability, type ProfitabilityLimits } from './profitability';

const NOW = 1_700_000_000_000;
const candidate: ArbitrageOpportunity = {
  pair: 'BTC/USDT', asset: 'BTC', quote: 'USDT', buyExchange: 'binance', sellExchange: 'bybit',
  buyPrice: 100, buyVWAP: 100, sellPrice: 102, sellVWAP: 102,
  executableQuantity: 1, executableNotional: 100, grossSpread: 0.02,
  estimatedFees: 0, netSpread: 0.02, estimatedNetProfit: 2, score: 75, timestamp: NOW,
};
const config = { feeRates: { binance: 0, bybit: 0 }, maxBookAgeMs: 5_000, minimumNetSpread: 0,
  minimumExecutableNotional: 0, maxDepthLevels: 20 };
const limits: ProfitabilityLimits = { minimumEstimatedNetProfit: 0.01, maximumExecutableNotional: 10_000, maximumTotalSlippage: 0.5 };

function book(exchange: 'binance' | 'bybit', bids = [{ price: 99, quantity: 2 }], asks = [{ price: 100, quantity: 2 }]): OrderBookState {
  const asset = 'BTC', quote = 'USDT';
  return { exchange, canonicalAsset: asset, canonicalQuote: quote, canonicalPair: 'BTC/USDT', exchangeSymbol: 'BTCUSDT',
    bids, asks, bestBid: bids[0]?.price ?? null, bestBidQuantity: bids[0]?.quantity ?? null,
    bestAsk: asks[0]?.price ?? null, bestAskQuantity: asks[0]?.quantity ?? null,
    sequence: 1, sequenceValid: true, feedHealth: 'HEALTHY', lastFeedHeartbeatTimestamp: NOW,
    exchangeTimestamp: NOW, receivedTimestamp: NOW, processedTimestamp: NOW, status: 'SYNCHRONIZED',
    synchronized: true, stale: false, updateCount: 1 };
}
function evaluate(options: { buy?: OrderBookState; sell?: OrderBookState; route?: ArbitrageOpportunity;
  cfg?: typeof config; limits?: ProfitabilityLimits; now?: number } = {}) {
  return estimateOpportunityProfitability(options.route ?? candidate, options.buy ?? book('binance'),
    options.sell ?? book('bybit', [{ price: 102, quantity: 2 }], [{ price: 103, quantity: 2 }]),
    options.cfg ?? config, options.limits ?? limits, options.now ?? NOW);
}
function limited(changes: Partial<ProfitabilityLimits>): ProfitabilityLimits { return { ...limits, ...changes }; }

describe('Phase D estimated execution profitability', () => {
  it('accepts a profitable route after fees and slippage', () => {
    const result = evaluate({ cfg: { ...config, feeRates: { binance: 0.001, bybit: 0.001 } } }).opportunity!;
    expect(result.estimatedNetProfit).toBeCloseTo(1.798);
    expect(result.buySlippage).toBe(0);
    expect(result.sellSlippage).toBe(0);
  });
  it('rejects an apparent edge erased by fees', () => {
    const result = evaluate({ sell: book('bybit', [{ price: 100.1, quantity: 2 }], [{ price: 101, quantity: 2 }]),
      cfg: { ...config, feeRates: { binance: 0.001, bybit: 0.001 } } });
    expect(result.opportunity).toBeUndefined();
  });
  it('rejects an otherwise profitable route when depth slippage exceeds the limit', () => {
    const result = evaluate({ buy: book('binance', [{ price: 99, quantity: 2 }], [{ price: 100, quantity: 0.1 }, { price: 100.2, quantity: 2 }]),
      sell: book('bybit', [{ price: 105, quantity: 0.1 }, { price: 104.8, quantity: 2 }], [{ price: 106, quantity: 2 }]),
      limits: limited({ maximumTotalSlippage: 0.0001 }) });
    expect(result.rejectionReason).toBe('above_maximum_slippage');
  });
  it('walks multiple BUY levels and computes the buy VWAP', () => {
    const result = evaluate({ buy: book('binance', [{ price: 99, quantity: 1 }], [{ price: 100, quantity: 0.5 }, { price: 101, quantity: 1 }]) }).opportunity!;
    expect(result.buyQuoteCost).toBe(100.5);
    expect(result.buyVWAP).toBe(100.5);
  });
  it('walks multiple SELL levels and computes the sell VWAP', () => {
    const result = evaluate({ sell: book('bybit', [{ price: 102, quantity: 0.5 }, { price: 101, quantity: 1 }], [{ price: 103, quantity: 2 }]) }).opportunity!;
    expect(result.sellQuoteProceeds).toBe(101.5);
    expect(result.sellVWAP).toBe(101.5);
  });
  it('reports different BUY and SELL VWAPs', () => {
    const result = evaluate({ buy: book('binance', [{ price: 99, quantity: 2 }], [{ price: 100, quantity: 0.5 }, { price: 101, quantity: 1 }]),
      sell: book('bybit', [{ price: 102, quantity: 0.5 }, { price: 101, quantity: 1 }], [{ price: 103, quantity: 2 }]) }).opportunity!;
    expect(result.buyVWAP).toBe(100.5);
    expect(result.sellVWAP).toBe(101.5);
  });
  it('uses one common quantity limited by the thinner side', () => {
    const result = evaluate({ sell: book('bybit', [{ price: 102, quantity: 0.25 }], [{ price: 103, quantity: 2 }]) }).opportunity!;
    expect(result.executableQuantity).toBe(0.25);
    expect(result.buyQuoteCost).toBe(25);
    expect(result.sellQuoteProceeds).toBe(25.5);
  });
  it('rejects insufficient common depth when it cannot meet minimum notional', () => {
    const result = evaluate({ buy: book('binance', [{ price: 99, quantity: 2 }], [{ price: 100, quantity: 0.2 }]),
      sell: book('bybit', [{ price: 102, quantity: 0.1 }], [{ price: 103, quantity: 2 }]),
      cfg: { ...config, minimumExecutableNotional: 20 } });
    expect(result.rejectionReason).toBe('below_minimum_notional');
  });
  it('rejects executable notional below the configured minimum', () => {
    const result = evaluate({ cfg: { ...config, minimumExecutableNotional: 101 } });
    expect(result.rejectionReason).toBe('below_minimum_notional');
  });
  it('rejects a route above maximum slippage', () => {
    const result = evaluate({ buy: book('binance', [{ price: 99, quantity: 2 }], [{ price: 100, quantity: 0.5 }, { price: 101, quantity: 1 }]),
      limits: limited({ maximumTotalSlippage: 0 }) });
    expect(result.rejectionReason).toBe('above_maximum_slippage');
  });
  it('calculates the buy-side fee independently', () => {
    const result = evaluate({ cfg: { ...config, feeRates: { binance: 0.002, bybit: 0 } } }).opportunity!;
    expect(result.buyFee).toBeCloseTo(0.2);
  });
  it('calculates the sell-side fee independently', () => {
    const result = evaluate({ cfg: { ...config, feeRates: { binance: 0, bybit: 0.003 } } }).opportunity!;
    expect(result.sellFee).toBeCloseTo(0.306);
  });
  it('reports total fees as the sum of both legs', () => {
    const result = evaluate({ cfg: { ...config, feeRates: { binance: 0.001, bybit: 0.001 } } }).opportunity!;
    expect(result.totalFees).toBeCloseTo(0.202);
    expect(result.estimatedFees).toBeCloseTo(0.202);
  });
  it('calculates gross profit before fees', () => {
    expect(evaluate().opportunity!.grossProfit).toBe(2);
  });
  it('calculates net profit after both fees', () => {
    expect(evaluate({ cfg: { ...config, feeRates: { binance: 0.001, bybit: 0.001 } } }).opportunity!.estimatedNetProfit).toBeCloseTo(1.798);
  });
  it('calculates net spread against quote cost', () => {
    expect(evaluate({ cfg: { ...config, feeRates: { binance: 0.001, bybit: 0.001 } } }).opportunity!.netSpread).toBeCloseTo(0.01798);
  });
  it('rejects invalid or stale books', () => {
    const stale = { ...book('binance'), receivedTimestamp: NOW - 5_001 };
    expect(evaluate({ buy: stale }).rejectionReason).toBe('invalid_or_stale_book');
  });
  it('rejects quote mismatch', () => {
    const bad = { ...book('bybit', [{ price: 102, quantity: 2 }], [{ price: 103, quantity: 2 }]), canonicalQuote: 'USD', canonicalPair: 'BTC/USD' };
    expect(evaluate({ sell: bad }).rejectionReason).toBe('pair_or_quote_mismatch');
  });
  it('rejects stablecoin-to-stablecoin opportunities', () => {
    const stable = { ...candidate, asset: 'USDC', pair: 'USDC/USDT' };
    const buy = { ...book('binance'), canonicalAsset: 'USDC', canonicalPair: 'USDC/USDT' };
    const sell = { ...book('bybit', [{ price: 102, quantity: 2 }], [{ price: 103, quantity: 2 }]), canonicalAsset: 'USDC', canonicalPair: 'USDC/USDT' };
    expect(evaluate({ route: stable, buy, sell }).rejectionReason).toBe('stablecoin_base_asset');
  });
  it('rejects same-exchange routes', () => {
    const route = { ...candidate, sellExchange: 'binance' as const };
    const sell = { ...book('bybit', [{ price: 102, quantity: 2 }], [{ price: 103, quantity: 2 }]), exchange: 'binance' as const };
    expect(evaluate({ route, sell }).rejectionReason).toBe('same_exchange');
  });
  it('caps executable notional and recalculates the common quantity', () => {
    const result = evaluate({ limits: limited({ maximumExecutableNotional: 40 }) }).opportunity!;
    expect(result.executableNotional).toBeCloseTo(40);
    expect(result.executableQuantity).toBeCloseTo(0.4);
  });
  it('enforces the minimum estimated net profit', () => {
    const result = evaluate({ limits: limited({ minimumEstimatedNetProfit: 3 }) });
    expect(result.rejectionReason).toBe('below_minimum_net_profit');
  });
  it('retains book age and ACTIVE/QUIET freshness in the output', () => {
    const quiet = { ...book('binance'), status: 'QUIET' as const, receivedTimestamp: NOW - 100 };
    const result = evaluate({ buy: quiet }).opportunity!;
    expect(result).toMatchObject({ freshness: 'QUIET', buyBookAgeMs: 100, sellBookAgeMs: 0 });
  });
  it('preserves the Phase C score while enriching the route', () => {
    const result = evaluate().opportunity!;
    expect(result.score).toBe(candidate.score);
    expect(result.referenceQuantity).toBe(candidate.executableQuantity);
  });
});
