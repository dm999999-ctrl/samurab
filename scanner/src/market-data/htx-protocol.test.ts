import { gzipSync } from 'node:zlib';
import { describe, expect, it } from 'vitest';
import { classifyHtxDelta, decodeHtxFrame, htxMbpTopic, mapHtxSpotSymbol, parseHtxFrame } from './htx-protocol';
import type { LiveMarketSubscription } from './universe';

const sub: LiveMarketSubscription = { exchange: 'htx', exchangeSymbol: 'BTCUSDT', canonicalAsset: 'BTC', canonicalQuote: 'USDT', canonicalPair: 'BTC/USDT' };
const gzipJson = (value: unknown) => gzipSync(Buffer.from(JSON.stringify(value)));
const gzipText = (value: string) => gzipSync(Buffer.from(value));

describe('HTX Spot MBP protocol', () => {
  it('maps Phase A symbols and selects only documented MBP depths', () => {
    expect(mapHtxSpotSymbol(sub)).toBe('btcusdt');
    expect(htxMbpTopic('BTCUSDT', 150)).toBe('market.btcusdt.mbp.150');
    expect(() => htxMbpTopic('btcusdt', 10)).toThrow('5, 20, 150, or 400');
    expect(() => mapHtxSpotSymbol({ ...sub, exchangeSymbol: 'BTC-USDT' })).toThrow('Invalid HTX Spot symbol');
  });

  it('gzip-decodes market/control frames and responds to the documented heartbeat shape', () => {
    const compressed = gzipJson({ ping: 1_700_000_000_000 });
    expect(decodeHtxFrame(compressed)).toBe('{"ping":1700000000000}');
    expect(parseHtxFrame(compressed)).toEqual({ kind: 'heartbeat', ping: 1_700_000_000_000 });
    expect(parseHtxFrame(gzipJson({ id: 'sub1', status: 'ok', subbed: 'market.btcusdt.mbp.150' })))
      .toMatchObject({ kind: 'ack', id: 'sub1', error: null });
    expect(() => decodeHtxFrame(Uint8Array.from([1, 2, 3]))).toThrow('not gzip-compressed');
  });

  it('parses MBP refresh snapshots and absolute-size incrementals with exact sequence tokens', () => {
    const snapshot = parseHtxFrame(gzipText('{"id":"req1","rep":"market.btcusdt.mbp.150","status":"ok","data":{"seqNum":9007199254740993,"bids":[[100,2]],"asks":[[101,3]]}}'));
    expect(snapshot).toMatchObject({ kind: 'snapshot', snapshot: { sequence: '9007199254740993', bids: [{ price: 100, quantity: 2 }] } });
    const update = parseHtxFrame(gzipText('{"ch":"market.btcusdt.mbp.150","ts":1700000000001,"tick":{"seqNum":9007199254740995,"prevSeqNum":9007199254740993,"bids":[[100,0],[99,4]]}}'));
    expect(update).toMatchObject({ kind: 'delta', delta: { sequence: '9007199254740995', previousSequence: '9007199254740993',
      bids: [{ price: 100, quantity: 0 }, { price: 99, quantity: 4 }], asks: [] } });
  });

  it('requires the exact prevSeqNum bridge, classifies gaps and stale messages', () => {
    const delta = (sequence: string, previousSequence: string) => ({ symbol: 'btcusdt', sequence, previousSequence,
      exchangeTimestamp: 1_700_000_000_000, bids: [], asks: [] });
    expect(classifyHtxDelta('100', delta('101', '100'), true)).toBe('BRIDGE');
    expect(classifyHtxDelta('101', delta('102', '101'))).toBe('APPLY');
    expect(classifyHtxDelta('101', delta('102', '100'))).toBe('GAP');
    expect(classifyHtxDelta('101', delta('101', '100'))).toBe('STALE');
  });

  it('rejects malformed levels and sequence ranges', () => {
    expect(() => parseHtxFrame(gzipJson({ ch: 'market.btcusdt.mbp.20', ts: 1_700_000_000_000,
      tick: { seqNum: 5, prevSeqNum: 5, bids: [[100, 1]], asks: [] } }))).toThrow('seqNum must exceed prevSeqNum');
    expect(() => parseHtxFrame(gzipJson({ ch: 'market.btcusdt.mbp.20', ts: 1_700_000_000_000,
      tick: { seqNum: 6, prevSeqNum: 5, bids: [[0, 1]], asks: [] } }))).toThrow('price/size');
  });
});
