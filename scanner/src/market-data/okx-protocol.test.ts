import { describe, expect, it } from 'vitest';
import type { LiveMarketSubscription } from './universe';
import { classifyOkxUpdate, mapOkxSpotInstrument, OKX_PUBLIC_SPOT_WS_URL, parseOkxFrame } from './okx-protocol';

const subscription: LiveMarketSubscription = {
  exchange: 'okx', canonicalAsset: 'BTC', canonicalQuote: 'USDT', canonicalPair: 'BTC/USDT', exchangeSymbol: 'BTC-USDT',
};

describe('OKX public Spot books protocol', () => {
  it('uses the TLS public endpoint and maps Phase A Spot instruments', () => {
    expect(OKX_PUBLIC_SPOT_WS_URL).toBe('wss://ws.okx.com/ws/v5/public');
    expect(mapOkxSpotInstrument(subscription)).toBe('BTC-USDT');
    expect(mapOkxSpotInstrument({ ...subscription, exchangeSymbol: '' })).toBe('BTC-USDT');
    expect(() => mapOkxSpotInstrument({ ...subscription, exchangeSymbol: 'BTC-USDT-SWAP' })).toThrow('Invalid OKX Spot');
  });

  it('parses snapshots and 4-column price levels without relying on deprecated checksum', () => {
    const frame = parseOkxFrame(JSON.stringify({ arg: { channel: 'books', instId: 'BTC-USDT' }, action: 'snapshot', data: [{
      asks: [['101', '3', '3', '2']], bids: [['100', '2', '2', '1']], ts: '1700000000000', seqId: 20,
    }] }));
    expect(frame).toEqual({ kind: 'book', push: {
      instrumentId: 'BTC-USDT', action: 'snapshot', sequence: 20, previousSequence: null,
      exchangeTimestamp: 1_700_000_000_000, bids: [{ price: 100, quantity: 2 }], asks: [{ price: 101, quantity: 3 }],
    } });
  });

  it('validates prevSeqId continuity even when seqId values are not consecutive', () => {
    const frame = parseOkxFrame(JSON.stringify({ arg: { channel: 'books', instId: 'BTC-USDT' }, action: 'update', data: [{
      asks: [], bids: [['100', '4', '4', '1']], ts: '1700000000100', seqId: 25, prevSeqId: 20,
    }] }));
    if (frame.kind !== 'book') throw new Error('expected book push');
    expect(classifyOkxUpdate(20, frame.push)).toBe('CONTIGUOUS');
    expect(classifyOkxUpdate(19, frame.push)).toBe('GAP');
    expect(classifyOkxUpdate(25, frame.push)).toBe('GAP');
  });

  it('recognizes the documented no-change heartbeat and rejects malformed book data', () => {
    expect(parseOkxFrame('pong')).toEqual({ kind: 'pong' });
    const heartbeat = parseOkxFrame(JSON.stringify({ arg: { channel: 'books', instId: 'BTC-USDT' }, action: 'update', data: [{
      asks: [], bids: [], ts: '1700000000100', seqId: 20, prevSeqId: 20,
    }] }));
    if (heartbeat.kind !== 'book') throw new Error('expected book push');
    expect(classifyOkxUpdate(20, heartbeat.push)).toBe('HEARTBEAT');
    expect(() => parseOkxFrame('{bad')).toThrow('invalid OKX JSON');
    expect(() => parseOkxFrame(JSON.stringify({ arg: { channel: 'books', instId: 'BTC-USDT' }, action: 'update', data: [{
      asks: [], bids: [], ts: '1', seqId: 21, prevSeqId: 19,
    }] }))).not.toThrow();
  });
});
