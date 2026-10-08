import type { OrderBookLevel } from '../types';
import type { LiveMarketSubscription } from './universe';

export const BITGET_SPOT_WS_URL = 'wss://ws.bitget.com/v3/ws/public';
export const BITGET_SPOT_BOOK_TOPIC = 'books';

export type BitgetBookMessage = {
  action: 'snapshot' | 'update';
  symbol: string;
  sequence: string;
  previousSequence: string | null;
  exchangeTimestamp: number;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
};

export type BitgetFrame =
  | { kind: 'book'; book: BitgetBookMessage }
  | { kind: 'ack'; event: string; symbol: string | null; error: string | null }
  | { kind: 'pong' }
  | { kind: 'ignore' };

export type BitgetSequenceDisposition = 'STALE' | 'BRIDGE' | 'APPLY' | 'GAP';

/** Phase A's Spot instrument is concatenated base+quote; reject lossy guesses. */
export function mapBitgetSpotSymbol(subscription: LiveMarketSubscription): string {
  const expected = `${subscription.canonicalAsset}${subscription.canonicalQuote}`.toUpperCase();
  const symbol = (subscription.exchangeSymbol.trim() || expected).toUpperCase();
  if (!/^[A-Z0-9]+$/.test(symbol) || symbol !== expected) {
    throw new Error(`Invalid Bitget Spot symbol mapping for ${subscription.canonicalPair}: ${symbol}`);
  }
  return symbol;
}

/** Parse seq/pseq integer tokens losslessly; Bitget sequence values exceed Number.MAX_SAFE_INTEGER. */
export function parseBitgetFrame(raw: string): BitgetFrame {
  if (raw.trim() === 'pong') return { kind: 'pong' };
  let value: unknown;
  try { value = parseLosslessJson(raw); }
  catch (error) { throw new Error(`invalid Bitget Spot JSON: ${error instanceof Error ? error.message : String(error)}`); }
  if (!isRecord(value)) throw new Error('invalid Bitget Spot frame: expected object');
  const event = typeof value.event === 'string' ? value.event : '';
  if (event) {
    const symbol = isRecord(value.arg) && typeof value.arg.symbol === 'string' ? value.arg.symbol.toUpperCase() : null;
    const code = value.code == null || String(value.code) === '0' ? null : String(value.code);
    const detail = typeof value.msg === 'string' ? value.msg : null;
    return { kind: 'ack', event, symbol, error: code ? `${code}${detail ? `: ${detail}` : ''}` : event === 'error' ? detail ?? 'Bitget WebSocket error' : null };
  }
  if (typeof value.op === 'string' || typeof value.action !== 'string') return { kind: 'ignore' };
  if (value.action !== 'snapshot' && value.action !== 'update') return { kind: 'ignore' };
  if (!isRecord(value.arg) || value.arg.instType !== 'spot' || value.arg.topic !== BITGET_SPOT_BOOK_TOPIC) return { kind: 'ignore' };
  const symbol = typeof value.arg.symbol === 'string' ? value.arg.symbol.toUpperCase() : '';
  if (!/^[A-Z0-9]+$/.test(symbol)) throw new Error('invalid Bitget Spot order-book symbol');
  if (!Array.isArray(value.data) || value.data.length !== 1 || !isRecord(value.data[0])) {
    throw new Error('invalid Bitget Spot order-book data: expected one book entry');
  }
  const row = value.data[0];
  const sequence = parseSequence(row.seq, 'seq');
  const previousSequence = row.pseq == null ? null : parseSequence(row.pseq, 'pseq');
  if (value.action === 'update' && previousSequence === null) throw new Error('Bitget Spot update is missing pseq');
  if (previousSequence !== null && compareSequence(sequence, previousSequence) <= 0) {
    throw new Error('invalid Bitget Spot sequence: seq must be greater than pseq');
  }
  const exchangeTimestamp = safeTimestamp(row.ts ?? value.ts);
  return { kind: 'book', book: { action: value.action, symbol, sequence, previousSequence, exchangeTimestamp,
    bids: parseLevels(row.b, 'bids'), asks: parseLevels(row.a, 'asks') } };
}

/** The first delta must bracket the WS snapshot sequence; later pseq must equal the applied seq. */
export function classifyBitgetUpdate(currentSequence: string, update: BitgetBookMessage, awaitingSnapshotBridge = false): BitgetSequenceDisposition {
  const current = toBigInt(currentSequence), next = toBigInt(update.sequence);
  const previous = update.previousSequence === null ? null : toBigInt(update.previousSequence);
  if (!awaitingSnapshotBridge && previous === 0n && current > 0n) return 'GAP'; // Bitget resets sequence state on service restart.
  if (next <= current) return 'STALE';
  if (awaitingSnapshotBridge) {
    if (previous !== null && previous <= current && current <= next) return 'BRIDGE';
    return 'GAP';
  }
  return previous === current ? 'APPLY' : 'GAP';
}

function parseLosslessJson(raw: string): unknown {
  type Context = { source?: string };
  type Reviver = (this: unknown, key: string, value: unknown, context?: Context) => unknown;
  const parse = JSON.parse as unknown as (text: string, reviver: Reviver) => unknown;
  return parse(raw, function (key, value, context) {
    if ((key === 'seq' || key === 'pseq') && typeof value === 'number') {
      const source = context?.source;
      if (source && /^\d+$/.test(source)) return source;
      if (!Number.isSafeInteger(value) || value < 0) throw new Error(`unsafe Bitget ${key} number`);
      return String(value);
    }
    if ((key === 'seq' || key === 'pseq') && typeof value === 'string') return value;
    return value;
  });
}

function parseSequence(value: unknown, name: string): string {
  const sequence = typeof value === 'string' ? value : typeof value === 'number' && Number.isSafeInteger(value) ? String(value) : '';
  if (!/^\d+$/.test(sequence)) throw new Error(`invalid Bitget Spot ${name}`);
  return sequence;
}
function compareSequence(left: string, right: string) {
  const a = toBigInt(left), b = toBigInt(right);
  return a < b ? -1 : a > b ? 1 : 0;
}
function toBigInt(value: string) {
  if (!/^\d+$/.test(value)) throw new Error('invalid Bitget sequence integer');
  return BigInt(value);
}
function safeTimestamp(value: unknown) {
  const timestamp = typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : value;
  if (typeof timestamp !== 'number' || !Number.isSafeInteger(timestamp) || timestamp <= 0) throw new Error('invalid Bitget Spot order-book timestamp');
  return timestamp;
}
function parseLevels(value: unknown, name: string): OrderBookLevel[] {
  if (!Array.isArray(value)) throw new Error(`invalid Bitget Spot ${name}: expected array`);
  return value.map((row, index) => {
    if (!Array.isArray(row) || row.length < 2) throw new Error(`invalid Bitget Spot ${name}[${index}] level`);
    const price = Number(row[0]), quantity = Number(row[1]);
    if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(quantity) || quantity < 0) {
      throw new Error(`invalid Bitget Spot ${name}[${index}] price/quantity`);
    }
    return { price, quantity };
  });
}
function isRecord(value: unknown): value is Record<string, any> { return typeof value === 'object' && value !== null && !Array.isArray(value); }
