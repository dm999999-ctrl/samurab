import { describe, expect, it } from 'vitest';
import { classifyMexcDelta, mexcDepthChannel, mapMexcSpotSymbol, parseMexcFrame, parseMexcSnapshot, type MexcDepthDelta } from './mexc-protocol';
import type { LiveMarketSubscription } from './universe';

describe('MEXC Spot protocol', () => {
  it('maps Phase A concatenated symbols and builds the official aggregate diff-depth channel', () => {
    expect(mapMexcSpotSymbol(subscription())).toBe('BTCUSDT');
    expect(mexcDepthChannel('BTCUSDT')).toBe('spot@public.aggre.depth.v3.api.pb@100ms@BTCUSDT');
    expect(() => mapMexcSpotSymbol(subscription('BTC-USDT'))).toThrow('Invalid MEXC Spot symbol mapping');
  });

  it('decodes the official protobuf wrapper, depth versions and absolute levels', () => {
    const raw = wrapper('BTCUSDT', deltaBody(100, 103, [['100.5', '0']], [['100', '2.25']]));
    expect(parseMexcFrame(raw, true)).toEqual({ kind: 'delta', delta: {
      symbol: 'BTCUSDT', fromVersion: 100, toVersion: 103, exchangeTimestamp: 1_800_000_000_000,
      bids: [{ price: 100, quantity: 2.25 }], asks: [{ price: 100.5, quantity: 0 }],
    } });
  });

  it('accepts int64 version fields encoded as protobuf varints', () => {
    const depth = join(field(4, 0, varint(7)), field(5, 0, varint(9)));
    const wrapperBytes = join(textField(3, 'BTCUSDT'), field(6, 0, varint(1_800_000_000_000)), field(313, 2, depth));
    expect(parseMexcFrame(wrapperBytes, true)).toMatchObject({ kind: 'delta', delta: { fromVersion: 7, toVersion: 9 } });
  });

  it('parses REST snapshots and rejects malformed levels and unsafe versions', () => {
    expect(parseMexcSnapshot({ lastUpdateId: 99, bids: [['100', '2']], asks: [['101', '3']] }, 'BTCUSDT'))
      .toMatchObject({ symbol: 'BTCUSDT', lastUpdateId: 99, bids: [{ price: 100, quantity: 2 }] });
    expect(() => parseMexcSnapshot({ lastUpdateId: Number.MAX_SAFE_INTEGER + 10, bids: [], asks: [] }, 'BTCUSDT')).toThrow();
    expect(() => parseMexcSnapshot({ lastUpdateId: 1, bids: [['0', '1']], asks: [] }, 'BTCUSDT')).toThrow();
  });

  it('handles acknowledgements and pong control frames and rejects failed subscriptions', () => {
    expect(parseMexcFrame(JSON.stringify({ id: 7, code: 0, msg: 'OK' }), false)).toEqual({ kind: 'ack', id: 7, code: 0, message: 'OK' });
    expect(parseMexcFrame(JSON.stringify({ msg: 'PONG' }), false)).toEqual({ kind: 'pong' });
  });

  it('classifies stale, snapshot-bridging, contiguous, and gapped version ranges', () => {
    const d = (fromVersion: number, toVersion: number): MexcDepthDelta => ({ symbol: 'BTCUSDT', fromVersion, toVersion,
      exchangeTimestamp: 1, bids: [], asks: [] });
    expect(classifyMexcDelta(100, d(95, 99), true)).toBe('STALE');
    expect(classifyMexcDelta(100, d(95, 100), true)).toBe('BRIDGE');
    expect(classifyMexcDelta(100, d(98, 102), true)).toBe('BRIDGE');
    expect(classifyMexcDelta(100, d(101, 103))).toBe('APPLY');
    expect(classifyMexcDelta(100, d(102, 103))).toBe('GAP');
  });
});

function subscription(exchangeSymbol = 'BTCUSDT'): LiveMarketSubscription {
  return { exchange: 'mexc', exchangeSymbol, canonicalPair: 'BTC/USDT', canonicalAsset: 'BTC', canonicalQuote: 'USDT',
    baseAsset: 'BTC', quoteAsset: 'USDT', quoteVolume24h: 1000, rank: 1 } as LiveMarketSubscription;
}
function varint(value: number): Uint8Array {
  const bytes: number[] = []; let n = BigInt(value);
  while (n >= 128n) { bytes.push(Number(n & 127n) | 128); n >>= 7n; }
  bytes.push(Number(n)); return Uint8Array.from(bytes);
}
function field(number: number, wire: 0 | 2, value: Uint8Array): Uint8Array {
  const tag = varint(number * 8 + wire);
  return wire === 0 ? join(tag, value) : join(tag, varint(value.length), value);
}
function textField(number: number, value: string) { return field(number, 2, new TextEncoder().encode(value)); }
function join(...values: Uint8Array[]) { const out = new Uint8Array(values.reduce((n, value) => n + value.length, 0)); let offset = 0;
  for (const value of values) { out.set(value, offset); offset += value.length; } return out; }
function level(price: string, quantity: string) { return join(textField(1, price), textField(2, quantity)); }
function deltaBody(from: number, to: number, asks: string[][], bids: string[][]) {
  return join(...asks.map((row) => field(1, 2, level(row[0], row[1]))), ...bids.map((row) => field(2, 2, level(row[0], row[1]))),
    textField(4, String(from)), textField(5, String(to)));
}
function wrapper(symbol: string, body: Uint8Array) {
  const timestamp = varint(1_800_000_000_000);
  return join(textField(1, 'spot@public.aggre.depth.v3.api.pb@100ms'), textField(3, symbol), field(6, 0, timestamp), field(313, 2, body));
}
