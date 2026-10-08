import type { LiveMarketSubscription } from './universe';
import type { OrderBookLevel } from '../types';

export const CRYPTO_COM_SPOT_WS_URL = 'wss://stream.crypto.com/exchange/v1/market';
export const CRYPTO_COM_BOOK_DEPTH = 50;

export function mapCryptoComSpotInstrument(item: LiveMarketSubscription): string {
  const symbol = item.exchangeSymbol.trim().toUpperCase();
  if (!/^[A-Z0-9]+_[A-Z0-9]+$/.test(symbol)) {
    throw new Error(`Invalid Crypto.com Spot instrument ${item.exchangeSymbol}; expected BASE_QUOTE`);
  }
  return symbol;
}

export type CryptoComBookFrame = {
  kind: 'book';
  instrumentName: string;
  channel: 'book' | 'book.update';
  sequence: number | string;
  previousSequence?: number | string;
  bids?: OrderBookLevel[];
  asks?: OrderBookLevel[];
  exchangeTimestamp: number;
};
export type CryptoComFrame = CryptoComBookFrame
  | { kind: 'heartbeat'; id: number | string }
  | { kind: 'ack'; id?: number | string; code: number; method?: string; message?: string }
  | { kind: 'ignore' };

export function parseCryptoComFrame(raw: string): CryptoComFrame {
  let value: unknown;
  try { value = JSON.parse(raw); } catch { throw new Error('Crypto.com sent invalid JSON'); }
  if (!isRecord(value)) throw new Error('Crypto.com frame must be an object');
  if (value.method === 'public/heartbeat') {
    if (!isId(value.id)) throw new Error('Crypto.com heartbeat missing id');
    return { kind: 'heartbeat', id: value.id };
  }
  const result = isRecord(value.result) ? value.result : undefined;
  if (!result) {
    if (value.method === 'subscribe' || value.method === 'unsubscribe' || value.method === 'public/respond-heartbeat') {
      return { kind: 'ack', ...(isId(value.id) ? { id: value.id } : {}), code: typeof value.code === 'number' ? value.code : 0,
        method: value.method, ...(typeof value.message === 'string' ? { message: value.message } : {}) };
    }
    if (value.code !== undefined && value.code !== 0) throw new Error(`Crypto.com WebSocket error ${String(value.code)}: ${String(value.message ?? 'unknown error')}`);
    return { kind: 'ignore' };
  }
  const channel = result.channel;
  if (channel !== 'book' && channel !== 'book.update') return { kind: 'ignore' };
  if (!Array.isArray(result.data) && value.method === 'subscribe') {
    return { kind: 'ack', ...(isId(value.id) ? { id: value.id } : {}), code: typeof value.code === 'number' ? value.code : 0,
      method: value.method, ...(typeof value.message === 'string' ? { message: value.message } : {}) };
  }
  const instrumentName = typeof result.instrument_name === 'string' ? result.instrument_name.toUpperCase() : '';
  if (!instrumentName) throw new Error('Crypto.com book frame missing instrument_name');
  if (!Array.isArray(result.data) || result.data.length === 0 || !isRecord(result.data[0])) {
    throw new Error(`Crypto.com ${channel} frame missing data`);
  }
  const data = result.data[0];
  const sequence = parseSequence(data.u, 'u');
  const exchangeTimestamp = parseTimestamp(data.tt ?? data.t);
  if (channel === 'book') {
    if (!Array.isArray(data.bids) || !Array.isArray(data.asks)) throw new Error('Crypto.com snapshot missing bids/asks');
    return { kind: 'book', instrumentName, channel, sequence, bids: parseLevels(data.bids), asks: parseLevels(data.asks), exchangeTimestamp };
  }
  if (!isRecord(data.update) || !Array.isArray(data.update.bids) || !Array.isArray(data.update.asks)) {
    throw new Error('Crypto.com delta missing update.bids/update.asks');
  }
  return { kind: 'book', instrumentName, channel, sequence,
    previousSequence: parseSequence(data.pu, 'pu'), bids: parseLevels(data.update.bids), asks: parseLevels(data.update.asks), exchangeTimestamp };
}

export type CryptoComUpdateDisposition = 'APPLY' | 'STALE' | 'GAP';
export function classifyCryptoComUpdate(current: number | string, update: CryptoComBookFrame): CryptoComUpdateDisposition {
  if (update.channel !== 'book.update' || update.previousSequence === undefined) throw new Error('Crypto.com delta sequence fields are invalid');
  const last = sequenceBigInt(current), next = sequenceBigInt(update.sequence), previous = sequenceBigInt(update.previousSequence);
  if (next <= last) return 'STALE';
  return previous === last ? 'APPLY' : 'GAP';
}

function parseLevels(value: unknown[]): OrderBookLevel[] {
  return value.map((row) => {
    if (!Array.isArray(row) || row.length < 2) throw new Error('Crypto.com book level must contain price and quantity');
    const price = Number(row[0]), quantity = Number(row[1]);
    if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(quantity) || quantity < 0) throw new Error('Crypto.com book level has invalid price/quantity');
    return { price, quantity };
  });
}
function parseSequence(value: unknown, name: string): number | string {
  if (typeof value === 'string' && /^\d+$/.test(value)) return value;
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return value;
  throw new Error(`Crypto.com book frame has invalid ${name} sequence`);
}
function sequenceBigInt(value: number | string): bigint { return BigInt(value); }
function parseTimestamp(value: unknown): number {
  const timestamp = typeof value === 'string' || typeof value === 'number' ? Number(value) : NaN;
  if (!Number.isSafeInteger(timestamp) || timestamp <= 0) throw new Error('Crypto.com book frame has invalid timestamp');
  return timestamp;
}
function isRecord(value: unknown): value is Record<string, any> { return typeof value === 'object' && value !== null && !Array.isArray(value); }
function isId(value: unknown): value is number | string { return (typeof value === 'number' && Number.isSafeInteger(value)) || (typeof value === 'string' && value.length > 0); }
