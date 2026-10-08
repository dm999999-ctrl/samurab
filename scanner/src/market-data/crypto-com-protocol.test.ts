import { describe, expect, it } from 'vitest';
import { classifyCryptoComUpdate, mapCryptoComSpotInstrument, parseCryptoComFrame } from './crypto-com-protocol';

const snapshot = (sequence: number | string = 100) => JSON.stringify({ id: 1, method: 'subscribe', code: 0, result: {
  channel: 'book', instrument_name: 'BTC_USDT', data: [{ t: 1_700_000_000_000, tt: 1_700_000_000_001, u: sequence,
    bids: [['100', '2', '1']], asks: [['101', '3', '2']] }],
} });
const delta = (u: number | string, pu: number | string, bids: unknown[] = [['100', '4', '1']], asks: unknown[] = []) => JSON.stringify({
  id: -1, method: 'subscribe', code: 0, result: { channel: 'book.update', instrument_name: 'BTC_USDT', data: [{ t: 1_700_000_000_010,
    tt: 1_700_000_000_011, u, pu, update: { bids, asks } }] },
});

describe('Crypto.com Spot protocol', () => {
  it('maps Phase A BASE_QUOTE symbols without changing their spot spelling', () => {
    expect(mapCryptoComSpotInstrument({ exchange: 'crypto.com', exchangeSymbol: 'btc_usdt' } as never)).toBe('BTC_USDT');
    expect(() => mapCryptoComSpotInstrument({ exchange: 'crypto.com', exchangeSymbol: 'BTCUSDT' } as never)).toThrow('BASE_QUOTE');
  });

  it('parses a sequenced snapshot and absolute-quantity delta, including empty deltas', () => {
    expect(parseCryptoComFrame(snapshot())).toMatchObject({ kind: 'book', channel: 'book', instrumentName: 'BTC_USDT', sequence: 100,
      bids: [{ price: 100, quantity: 2 }], asks: [{ price: 101, quantity: 3 }] });
    const update = parseCryptoComFrame(delta(101, 100, [['100', '0']], []));
    expect(update).toMatchObject({ kind: 'book', channel: 'book.update', sequence: 101, previousSequence: 100,
      bids: [{ price: 100, quantity: 0 }], asks: [] });
    expect(parseCryptoComFrame(delta('9007199254740994', '9007199254740993', [], []))).toMatchObject({ kind: 'book', sequence: '9007199254740994' });
  });

  it('classifies linked, duplicate, and discontinuous deltas with exact integer comparison', () => {
    const linked = parseCryptoComFrame(delta('9007199254740994', '9007199254740993'));
    const duplicate = parseCryptoComFrame(delta(100, 99));
    const gap = parseCryptoComFrame(delta(103, 99));
    expect(linked.kind).toBe('book');
    if (linked.kind !== 'book' || duplicate.kind !== 'book' || gap.kind !== 'book') throw new Error('expected book messages');
    expect(classifyCryptoComUpdate('9007199254740993', linked)).toBe('APPLY');
    expect(classifyCryptoComUpdate(100, duplicate)).toBe('STALE');
    expect(classifyCryptoComUpdate(101, gap)).toBe('GAP');
  });

  it('responds to server heartbeat and ignores unrelated subscription acknowledgements', () => {
    expect(parseCryptoComFrame(JSON.stringify({ id: 123, method: 'public/heartbeat', code: 0 }))).toEqual({ kind: 'heartbeat', id: 123 });
    expect(parseCryptoComFrame(JSON.stringify({ id: 12, method: 'subscribe', code: 0, result: { channel: 'ticker' } }))).toEqual({ kind: 'ignore' });
    expect(parseCryptoComFrame(JSON.stringify({ id: 12, method: 'unsubscribe', code: 0 }))).toMatchObject({ kind: 'ack', code: 0 });
  });

  it('rejects malformed sequence, missing delta predecessor, and invalid book levels', () => {
    expect(() => parseCryptoComFrame(snapshot('not-a-sequence'))).toThrow('invalid u sequence');
    expect(() => parseCryptoComFrame(delta(101, null as never))).toThrow('invalid pu sequence');
    expect(() => parseCryptoComFrame(delta(101, 100, [['0', '1']]))).toThrow('invalid price/quantity');
  });
});
