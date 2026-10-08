import { describe, expect, it } from 'vitest';
import type { LiveMarketSubscription } from './universe';
import { classifyKucoinDelta, KUCOIN_FULL_ORDERBOOK_URL, KUCOIN_PUBLIC_TOKEN_URL, mapKucoinSpotSymbol, parseKucoinFrame, parseKucoinSnapshot } from './kucoin-protocol';

const item: LiveMarketSubscription = {
  exchange: 'kucoin', canonicalAsset: 'BTC', canonicalQuote: 'USDT', canonicalPair: 'BTC/USDT', exchangeSymbol: 'BTC-USDT',
};

describe('KuCoin Classic Spot Level 2 protocol', () => {
  it('maps Phase A Spot symbols and uses the official public token/snapshot endpoints', () => {
    expect(mapKucoinSpotSymbol(item)).toBe('BTC-USDT');
    expect(() => mapKucoinSpotSymbol({ ...item, exchangeSymbol: 'BTC-USDTM' })).toThrow('does not match');
    expect(KUCOIN_PUBLIC_TOKEN_URL).toBe('https://api.kucoin.com/api/v1/bullet-public');
    expect(KUCOIN_FULL_ORDERBOOK_URL).toBe('https://api.kucoin.com/api/v1/market/orderbook/level2_100');
  });

  it('parses Level 2 range bounds and ignores per-price sequence for continuity', () => {
    const frame = parseKucoinFrame(JSON.stringify({ type: 'message', topic: '/market/level2:BTC-USDT', subject: 'trade.l2update', data: {
      symbol: 'BTC-USDT', sequenceStart: '1001', sequenceEnd: '1003', time: 1_700_000_000_000,
      changes: { asks: [['101', '0', '9999']], bids: [['100', '4', '1003'], ['0', '3', '1004']] },
    } }));
    expect(frame.kind).toBe('delta');
    if (frame.kind !== 'delta') return;
    expect(frame.delta).toMatchObject({ sequenceStart: 1001, sequenceEnd: 1003, bids: [{ price: 100, quantity: 4 }], asks: [{ price: 101, quantity: 0 }] });
    expect(classifyKucoinDelta(1000, frame.delta)).toBe('APPLY');
  });

  it('allows overlapping sequence windows, discards old frames, and identifies real gaps', () => {
    const delta = { symbol: 'BTC-USDT', sequenceStart: 99, sequenceEnd: 102, exchangeTimestamp: 1, bids: [], asks: [] };
    expect(classifyKucoinDelta(100, delta)).toBe('APPLY');
    expect(classifyKucoinDelta(102, delta)).toBe('STALE');
    expect(classifyKucoinDelta(100, { ...delta, sequenceStart: 102, sequenceEnd: 103 })).toBe('GAP');
  });

  it('parses the REST snapshot and rejects malformed frames/snapshot mismatches', () => {
    expect(parseKucoinSnapshot({ code: '200000', data: { symbol: 'BTC-USDT', sequence: '1000', time: 1_700_000_000_000,
      bids: [['100', '2']], asks: [['101', '3']] } }, 'BTC-USDT')).toMatchObject({
      sequence: 1000, bids: [{ price: 100, quantity: 2 }], asks: [{ price: 101, quantity: 3 }],
    });
    expect(() => parseKucoinSnapshot({ code: '200000', data: { symbol: 'ETH-USDT', sequence: '1', time: 1, bids: [], asks: [] } }, 'BTC-USDT')).toThrow('symbol mismatch');
    expect(() => parseKucoinFrame('{broken')).toThrow('invalid KuCoin JSON');
  });
});
