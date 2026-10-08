import { describe, expect, it } from 'vitest';
import { classifyCoinbaseSequence, mapCoinbaseSpotProduct, parseCoinbaseFrame } from './coinbase-protocol';

describe('Coinbase Spot WebSocket protocol', () => {
  it('maps Phase A product IDs and rejects non-product symbols', () => {
    expect(mapCoinbaseSpotProduct({ exchange: 'coinbase', exchangeSymbol: 'btc-usdt' } as never)).toBe('BTC-USDT');
    expect(() => mapCoinbaseSpotProduct({ exchange: 'coinbase', exchangeSymbol: 'BTCUSDT' } as never)).toThrow('BASE-QUOTE');
  });

  it('parses Advanced Trade L2 snapshots from l2_data envelopes', () => {
    expect(parseCoinbaseFrame(JSON.stringify({ channel: 'l2_data', sequence_num: 44, timestamp: '2026-10-08T00:00:00Z', events: [
      { type: 'snapshot', product_id: 'BTC-USD', updates: [{ side: 'bid', price_level: '100', new_quantity: '2' }, { side: 'offer', price_level: '101', new_quantity: '3' }] },
    ] }))).toMatchObject({ kind: 'batch', sequence: 44, frames: [{ productId: 'BTC-USD', type: 'snapshot',
      exchangeTimestamp: Date.parse('2026-10-08T00:00:00Z'), bids: [{ price: 100, quantity: 2 }], asks: [{ price: 101, quantity: 3 }] }] });
  });

  it('parses absolute L2 updates, both sides, and zero-size deletions', () => {
    expect(parseCoinbaseFrame(JSON.stringify({ channel: 'l2_data', sequence_num: 45, timestamp: '2026-10-08T00:00:01Z', events: [
      { type: 'update', product_id: 'BTC-USD', updates: [
        { side: 'bid', price_level: '100', new_quantity: '0' }, { side: 'bid', price_level: '99', new_quantity: '4.5' },
        { side: 'offer', price_level: '101', new_quantity: '2' },
      ] },
    ] }))).toMatchObject({ kind: 'batch', sequence: 45, frames: [{ type: 'update', productId: 'BTC-USD',
      bids: [{ price: 100, quantity: 0 }, { price: 99, quantity: 4.5 }], asks: [{ price: 101, quantity: 2 }] }] });
  });

  it('parses multiple product events in one envelope and preserves the shared sequence', () => {
    expect(parseCoinbaseFrame(JSON.stringify({ channel: 'l2_data', sequence_num: '46', events: [
      { type: 'update', product_id: 'BTC-USD', updates: [] }, { type: 'update', product_id: 'ETH-USD', updates: [] },
    ] }))).toMatchObject({ kind: 'batch', frames: [{ productId: 'BTC-USD', sequence: '46' }, { productId: 'ETH-USD', sequence: '46' }] });
  });

  it('preserves sequence numbers from valid empty L2 envelopes', () => {
    expect(parseCoinbaseFrame(JSON.stringify({ channel: 'l2_data', sequence_num: 47, events: [] })))
      .toEqual({ kind: 'batch', sequence: 47, frames: [] });
  });

  it('classifies exact per-product sequence continuity, duplicates, older messages, and gaps', () => {
    expect(classifyCoinbaseSequence(99, 100)).toBe('APPLY');
    expect(classifyCoinbaseSequence(100, 100)).toBe('STALE');
    expect(classifyCoinbaseSequence(100, 99)).toBe('STALE');
    expect(classifyCoinbaseSequence(100, 102)).toBe('GAP');
    expect(classifyCoinbaseSequence(null, 4)).toBe('BASELINE');
    expect(classifyCoinbaseSequence(4, null)).toBe('UNSEQUENCED');
    expect(classifyCoinbaseSequence('9007199254740993', '9007199254740994')).toBe('APPLY');
  });

  it('parses heartbeat/subscription replies and ignores future message types per Coinbase guidance', () => {
    expect(parseCoinbaseFrame(JSON.stringify({ channel: 'heartbeats', sequence_num: 123, events: [{ current_time: '2026-10-08T00:00:00Z' }] })))
      .toMatchObject({ kind: 'heartbeat', sequence: 123 });
    expect(parseCoinbaseFrame(JSON.stringify({ type: 'subscriptions', channels: [] }))).toEqual({ kind: 'subscriptions', sequence: null });
    expect(parseCoinbaseFrame(JSON.stringify({ channel: 'subscriptions', sequence_num: 4, events: [{}] })))
      .toEqual({ kind: 'subscriptions', sequence: 4 });
    expect(parseCoinbaseFrame(JSON.stringify({ type: 'new_future_event', product_id: 'BTC-USD' }))).toEqual({ kind: 'ignore' });
    expect(parseCoinbaseFrame(JSON.stringify({ type: 'error', message: 'bad subscription', reason: 'invalid product' })))
      .toMatchObject({ kind: 'error', message: 'bad subscription: invalid product' });
  });

  it('fails closed on malformed snapshots, changes, sequences, and product IDs', () => {
    expect(() => parseCoinbaseFrame(JSON.stringify({ channel: 'l2_data', sequence_num: 1, events: [{ type: 'snapshot', product_id: 'BTC-USD', updates: [{ side: 'bid', price_level: 'x', new_quantity: '1' }] }] }))).toThrow('invalid price');
    expect(() => parseCoinbaseFrame(JSON.stringify({ channel: 'l2_data', sequence_num: 1, events: [{ type: 'update', product_id: 'BTC-USD', updates: [{ side: 'sideways', price_level: '100', new_quantity: '1' }] }] }))).toThrow('bid|offer');
    expect(() => parseCoinbaseFrame(JSON.stringify({ channel: 'heartbeats', sequence_num: 1.5, events: [] }))).toThrow('invalid sequence');
    expect(() => parseCoinbaseFrame(JSON.stringify({ channel: 'l2_data', sequence_num: 1, events: [{ type: 'snapshot', product_id: 'BTCUSDT', updates: [] }] }))).toThrow('invalid product_id');
  });
});
