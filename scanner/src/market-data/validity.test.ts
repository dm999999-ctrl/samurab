import { describe, expect, it } from 'vitest';
import { isUsableOrderBook } from './validity';
import type { OrderBookState } from './types';

function book(overrides: Partial<OrderBookState> = {}): OrderBookState {
  return {
    exchange: 'binance', canonicalAsset: 'BTC', canonicalQuote: 'USDT', canonicalPair: 'BTC/USDT',
    exchangeSymbol: 'BTCUSDT', bids: [{ price: 99, quantity: 2 }], asks: [{ price: 101, quantity: 3 }],
    bestBid: 99, bestBidQuantity: 2, bestAsk: 101, bestAskQuantity: 3, sequence: 10,
    sequenceValid: true, feedHealth: 'HEALTHY', lastFeedHeartbeatTimestamp: 980,
    exchangeTimestamp: 900, receivedTimestamp: 950, processedTimestamp: 955,
    status: 'SYNCHRONIZED', synchronized: true, stale: false, updateCount: 1, ...overrides,
  };
}

describe('central order-book validity gate', () => {
  const policy = { connected: true };
  it('accepts an active, connected, healthy, synchronized, sequenced, two-sided valid book', () => {
    expect(isUsableOrderBook(book(), policy)).toBe(true);
  });
  it.each([
    ['disconnected', { connected: false }],
    ['unhealthy feed', { book: { feedHealth: 'UNHEALTHY' as const, status: 'UNHEALTHY' as const } }],
    ['resynchronizing', { book: { status: 'RESYNCING' as const, synchronized: false } }],
    ['sequence invalid', { book: { sequenceValid: false } }],
    ['explicitly stale', { book: { stale: true, status: 'STALE' as const, synchronized: false } }],
    ['quiet book without synchronized state', { book: { status: 'QUIET' as const, synchronized: false } }],
    ['empty bids', { book: { bids: [], bestBid: null, bestBidQuantity: null } }],
    ['empty asks', { book: { asks: [], bestAsk: null, bestAskQuantity: null } }],
    ['invalid quantity', { book: { asks: [{ price: 101, quantity: 0 }] } }],
    ['crossed book', { book: { bestAsk: 98, asks: [{ price: 98, quantity: 1 }] } }],
    ['missing timestamp', { book: { exchangeTimestamp: null } }],
  ])('rejects %s', (_name, options) => {
    const value = options as { connected?: boolean; book?: Partial<OrderBookState> };
    expect(isUsableOrderBook(book(value.book), { ...policy, connected: value.connected ?? true })).toBe(false);
  });
  it('allows a synchronized quiet book while feed health remains good', () => {
    const quiet = book({ status: 'QUIET', synchronized: true, stale: false });
    expect(isUsableOrderBook(quiet, policy)).toBe(true);
  });
});
