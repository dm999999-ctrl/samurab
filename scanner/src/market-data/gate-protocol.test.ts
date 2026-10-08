import { describe, expect, it } from 'vitest';
import { classifyGateDelta, GATE_SPOT_ORDER_BOOK_UPDATE_CHANNEL, mapGateSpotSymbol, parseGateFrame, parseGateSnapshot, type GateBookDelta } from './gate-protocol';
import type { LiveMarketSubscription } from './universe';

describe('Gate Spot order-book protocol', () => {
  it('maps Phase A symbols and parses Gate U/u absolute-amount updates', () => {
    expect(mapGateSpotSymbol(subscription())).toBe('BTC_USDT');
    expect(mapGateSpotSymbol(subscription('btc_usdt'))).toBe('BTC_USDT');
    expect(() => mapGateSpotSymbol(subscription('XBT_USDT'))).toThrow('does not match');
    expect(parseGateFrame(JSON.stringify({ channel: GATE_SPOT_ORDER_BOOK_UPDATE_CHANNEL, event: 'update', result: {
      s: 'BTC_USDT', U: 101, u: 103, t: 1_800_000_000_000, b: [['100', '2.5']], a: [['101', '0']],
    } }))).toEqual({ kind: 'book', delta: { symbol: 'BTC_USDT', firstUpdateId: 101, finalUpdateId: 103,
      exchangeTimestamp: 1_800_000_000_000, bids: [{ price: 100, quantity: 2.5 }], asks: [{ price: 101, quantity: 0 }] } });
  });

  it('parses a full REST snapshot and rejects invalid snapshot data', () => {
    expect(parseGateSnapshot({ id: 100, current: 1_800_000_000_000, update: 1_800_000_000_000,
      bids: [['100', '2']], asks: [['101', '3']] }, 'BTC_USDT')).toMatchObject({ updateId: 100, symbol: 'BTC_USDT' });
    expect(() => parseGateSnapshot({ id: 1, bids: [], asks: [] }, 'BTC_USDT')).toThrow('snapshot timestamp');
    expect(() => parseGateSnapshot({ id: 1, update: 100, bids: [['100', '0']], asks: [['101', '2']] }, 'BTC_USDT')).toThrow();
  });

  it('classifies stale, bridging, overlapping contiguous, and gapped U/u ranges', () => {
    const delta = (firstUpdateId: number, finalUpdateId: number): GateBookDelta => ({ symbol: 'BTC_USDT', firstUpdateId,
      finalUpdateId, exchangeTimestamp: 1, bids: [], asks: [] });
    expect(classifyGateDelta(100, delta(95, 100))).toBe('STALE');
    expect(classifyGateDelta(100, delta(98, 101), true)).toBe('BRIDGE');
    expect(classifyGateDelta(100, delta(99, 102))).toBe('APPLY');
    expect(classifyGateDelta(100, delta(102, 103))).toBe('GAP');
  });

  it('parses subscription acknowledgements, ping replies, and server errors', () => {
    expect(parseGateFrame(JSON.stringify({ channel: GATE_SPOT_ORDER_BOOK_UPDATE_CHANNEL, event: 'subscribe', error: null,
      result: { status: 'success' } }))).toMatchObject({ kind: 'ack', event: 'subscribe', error: null });
    expect(parseGateFrame(JSON.stringify({ channel: 'spot.pong', event: '', error: null, result: null }))).toEqual({ kind: 'pong' });
    expect(parseGateFrame(JSON.stringify({ channel: GATE_SPOT_ORDER_BOOK_UPDATE_CHANNEL, event: 'subscribe',
      error: { code: 2, message: 'rejected' }, result: { status: 'fail' } }))).toMatchObject({ kind: 'ack', error: '2: rejected' });
  });
});

function subscription(exchangeSymbol = 'BTC_USDT'): LiveMarketSubscription {
  return { exchange: 'gate', exchangeSymbol, canonicalPair: 'BTC/USDT', canonicalAsset: 'BTC', canonicalQuote: 'USDT' };
}
