import { describe, expect, it } from 'vitest';
import { classifyBitgetUpdate, mapBitgetSpotSymbol, parseBitgetFrame } from './bitget-protocol';
import type { LiveMarketSubscription } from './universe';

const subscription: LiveMarketSubscription = {
  exchange: 'bitget', exchangeSymbol: 'BTCUSDT', canonicalAsset: 'BTC', canonicalQuote: 'USDT', canonicalPair: 'BTC/USDT',
};

describe('Bitget Spot order-book protocol', () => {
  it('maps the Phase A Spot instrument without guessing separators', () => {
    expect(mapBitgetSpotSymbol(subscription)).toBe('BTCUSDT');
    expect(() => mapBitgetSpotSymbol({ ...subscription, exchangeSymbol: 'BTC_USDT' })).toThrow(/Invalid Bitget/);
  });

  it('parses the full snapshot and preserves 64-bit sequence tokens exactly', () => {
    const frame = parseBitgetFrame('{"action":"snapshot","arg":{"instType":"spot","topic":"books","symbol":"BTCUSDT"},"data":[{"seq":1304314508780744705,"pseq":0,"ts":"1746698732562","b":[["100","2"]],"a":[["101","3"]]}]}');
    expect(frame).toMatchObject({ kind: 'book', book: { action: 'snapshot', symbol: 'BTCUSDT', sequence: '1304314508780744705', previousSequence: '0', bids: [{ price: 100, quantity: 2 }] } });
  });

  it('parses absolute-quantity updates including zero-quantity deletion levels', () => {
    const frame = parseBitgetFrame('{"action":"update","arg":{"instType":"spot","topic":"books","symbol":"BTCUSDT"},"data":[{"seq":"9007199254740995","pseq":"9007199254740994","ts":"1746698732563","b":[["100","0"]],"a":[["101","4"]]}]}');
    expect(frame).toMatchObject({ kind: 'book', book: { action: 'update', sequence: '9007199254740995', previousSequence: '9007199254740994', bids: [{ price: 100, quantity: 0 }], asks: [{ price: 101, quantity: 4 }] } });
  });

  it('classifies the snapshot bridge, exact pseq continuity, old messages, and gaps', () => {
    const update = (pseq: string, seq: string) => ({ action: 'update' as const, symbol: 'BTCUSDT', sequence: seq, previousSequence: pseq,
      exchangeTimestamp: 1746698732563, bids: [], asks: [] });
    expect(classifyBitgetUpdate('100', update('98', '103'), true)).toBe('BRIDGE');
    expect(classifyBitgetUpdate('100', update('101', '103'), true)).toBe('GAP');
    expect(classifyBitgetUpdate('103', update('103', '106'))).toBe('APPLY');
    expect(classifyBitgetUpdate('103', update('102', '106'))).toBe('GAP');
    expect(classifyBitgetUpdate('103', update('102', '103'))).toBe('STALE');
    expect(classifyBitgetUpdate('90071992547409930', update('0', '3'))).toBe('GAP');
  });

  it('parses acknowledgements and pong while failing closed on malformed book messages', () => {
    expect(parseBitgetFrame('pong')).toEqual({ kind: 'pong' });
    expect(parseBitgetFrame('{"event":"subscribe","arg":{"symbol":"BTCUSDT"},"code":"0"}')).toMatchObject({ kind: 'ack', event: 'subscribe', symbol: 'BTCUSDT', error: null });
    expect(parseBitgetFrame('{"event":"error","code":"30001","msg":"bad subscribe"}')).toMatchObject({ kind: 'ack', error: '30001: bad subscribe' });
    expect(() => parseBitgetFrame('{"action":"update","arg":{"instType":"spot","topic":"books","symbol":"BTCUSDT"},"data":[{"seq":9007199254740993,"ts":1746698732563,"b":[],"a":[]}]}')).toThrow(/missing pseq/);
  });
});
