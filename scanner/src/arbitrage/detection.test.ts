import { describe, expect, it } from 'vitest';
import type { ExchangeId } from '../discovery/types';
import type { OrderBookState } from '../market-data/types';
import { detectArbitrageOpportunities, type ArbitrageConfig } from './detection';

const NOW = 10_000;
const config: ArbitrageConfig = { feeRates: {}, maxBookAgeMs: 1_000, minimumNetSpread: 0, minimumExecutableNotional: 0 };
function book(exchange: ExchangeId, bids: Array<[number, number]>, asks: Array<[number, number]>, overrides: Partial<OrderBookState> = {}): OrderBookState {
  const bidLevels = bids.map(([price, quantity]) => ({ price, quantity }));
  const askLevels = asks.map(([price, quantity]) => ({ price, quantity }));
  return {
    exchange, canonicalAsset: 'BTC', canonicalQuote: 'USDT', canonicalPair: 'BTC/USDT',
    exchangeSymbol: 'BTCUSDT', bids: bidLevels, asks: askLevels,
    bestBid: bidLevels[0]?.price ?? null, bestBidQuantity: bidLevels[0]?.quantity ?? null,
    bestAsk: askLevels[0]?.price ?? null, bestAskQuantity: askLevels[0]?.quantity ?? null,
    sequence: 1, sequenceValid: true, feedHealth: 'HEALTHY', lastFeedHeartbeatTimestamp: NOW,
    exchangeTimestamp: NOW, receivedTimestamp: NOW, processedTimestamp: NOW,
    status: 'SYNCHRONIZED', synchronized: true, stale: false, updateCount: 1,
    ...overrides,
  };
}
const detect = (books: OrderBookState[], overrides: Partial<ArbitrageConfig> = {}) =>
  detectArbitrageOpportunities(books, { ...config, ...overrides }, NOW);

describe('deterministic arbitrage detection', () => {
  it('detects a clearly profitable route and reports quote-currency economics', () => {
    const result = detect([book('binance', [[99, 1]], [[100, 1]]), book('bybit', [[105, 1]], [[106, 1]])]);
    expect(result).toHaveLength(1);
    expect(result[0]).toMatchObject({ buyExchange: 'binance', sellExchange: 'bybit', executableQuantity: 1, estimatedNetProfit: 4.795 });
  });
  it('returns no opportunity when the cross-exchange market has no spread', () => {
    expect(detect([book('binance', [[99, 1]], [[101, 1]]), book('bybit', [[100, 1]], [[102, 1]])])).toEqual([]);
  });
  it('rejects a gross edge when configured fees eliminate the net opportunity', () => {
    expect(detect([book('binance', [[99.8, 1]], [[100, 1]]), book('bybit', [[100.1, 1]], [[101, 1]])],
      { feeRates: { binance: 0.001, bybit: 0.001 } })).toEqual([]);
  });
  it('walks multiple levels and reflects depth in the executable VWAP', () => {
    const [result] = detect([
      book('binance', [[99, 1]], [[100, 0.2], [102, 0.8]]),
      book('bybit', [[105, 0.5], [103, 0.5]], [[106, 1]]),
    ]);
    expect(result.executableQuantity).toBe(1);
    expect(result.buyVWAP).toBeCloseTo(101.6);
    expect(result.sellVWAP).toBeCloseTo(104);
    expect(result.buyPrice).toBe(100);
    expect(result.sellPrice).toBe(105);
  });
  it('caps the common executable quantity at the shallower side', () => {
    const [result] = detect([
      book('binance', [[99, 1]], [[100, 0.4]]),
      book('bybit', [[105, 1]], [[106, 1]]),
    ]);
    expect(result.executableQuantity).toBeCloseTo(0.4);
  });
  it('computes buy/sell VWAP independently from consumed depth', () => {
    const [result] = detect([
      book('binance', [[99, 1]], [[100, 1], [102, 1]]),
      book('bybit', [[105, 1], [103, 1]], [[106, 1]]),
    ]);
    expect(result.buyVWAP).toBe(101);
    expect(result.sellVWAP).toBe(104);
  });
  it('rejects a route below the configured minimum executable notional', () => {
    expect(detect([book('binance', [[99, 1]], [[100, 0.1]]), book('bybit', [[105, 1]], [[106, 1]])],
      { minimumExecutableNotional: 20 })).toEqual([]);
  });
  it('rejects invalid or unsynchronized books', () => {
    expect(detect([book('binance', [[99, 1]], [[100, 1]]), book('bybit', [[105, 1]], [[106, 1]], { sequenceValid: false })])).toEqual([]);
  });
  it('rejects expired books even if their last state was synchronized', () => {
    expect(detect([book('binance', [[99, 1]], [[100, 1]]), book('bybit', [[105, 1]], [[106, 1]], { receivedTimestamp: 8_000 })])).toEqual([]);
  });
  it('does not compare mismatched quote currencies', () => {
    expect(detect([
      book('binance', [[99, 1]], [[100, 1]]),
      book('bybit', [[105, 1]], [[106, 1]], { canonicalQuote: 'USDC' }),
    ])).toEqual([]);
  });
  it('selects the most profitable route across several venues', () => {
    const [result] = detect([
      book('binance', [[99, 1]], [[100, 1]]),
      book('okx', [[99, 1]], [[101, 1]]),
      book('bybit', [[105, 1]], [[106, 1]]),
      book('gate', [[104, 1]], [[105, 1]]),
    ]);
    expect(result.buyExchange).toBe('binance');
    expect(result.sellExchange).toBe('bybit');
  });
  it('never compares a book against itself', () => {
    expect(detect([book('binance', [[99, 1]], [[100, 1]])])).toEqual([]);
  });
  it('removes the opportunity when a later book update makes the spread unprofitable', () => {
    const buy = book('binance', [[99, 1]], [[100, 1]]);
    const sell = book('bybit', [[105, 1]], [[106, 1]]);
    expect(detect([buy, sell])).toHaveLength(1);
    sell.bids = [{ price: 99.9, quantity: 1 }];
    sell.bestBid = 99.9;
    expect(detect([buy, sell])).toEqual([]);
  });
});
