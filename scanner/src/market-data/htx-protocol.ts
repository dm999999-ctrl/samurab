import { gunzipSync } from 'node:zlib';
import type { OrderBookLevel } from '../types';
import type { LiveMarketSubscription } from './universe';

export const HTX_SPOT_MBP_WS_URL = 'wss://api.huobi.pro/feed';
export const HTX_DEFAULT_MBP_LEVELS = 150;

export type HtxBookDelta = {
  symbol: string;
  sequence: string;
  previousSequence: string;
  exchangeTimestamp: number;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
};
export type HtxBookSnapshot = {
  symbol: string;
  sequence: string;
  exchangeTimestamp: number | null;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
};
export type HtxFrame =
  | { kind: 'heartbeat'; ping: number }
  | { kind: 'ack'; id: string | null; topic: string | null; error: string | null }
  | { kind: 'snapshot'; id: string; topic: string; snapshot: HtxBookSnapshot; error: string | null }
  | { kind: 'delta'; delta: HtxBookDelta }
  | { kind: 'ignore' };
export type HtxSequenceDisposition = 'STALE' | 'BRIDGE' | 'APPLY' | 'GAP';

export function mapHtxSpotSymbol(subscription: LiveMarketSubscription): string {
  const expected = `${subscription.canonicalAsset}${subscription.canonicalQuote}`.toLowerCase();
  const symbol = (subscription.exchangeSymbol.trim() || expected).toLowerCase();
  if (!/^[a-z0-9]+$/.test(symbol) || symbol !== expected) {
    throw new Error(`Invalid HTX Spot symbol mapping for ${subscription.canonicalPair}: ${symbol}; expected ${expected}`);
  }
  return symbol;
}

export function htxMbpTopic(symbol: string, levels = HTX_DEFAULT_MBP_LEVELS): string {
  if (![5, 20, 150, 400].includes(levels)) throw new Error('HTX MBP levels must be 5, 20, 150, or 400');
  return `market.${symbol.toLowerCase()}.mbp.${levels}`;
}

/** HTX public market feed frames are gzip-compressed; text is accepted for control-frame tests. */
export function decodeHtxFrame(raw: string | Uint8Array | Uint8Array[]): string {
  if (typeof raw === 'string') return raw;
  const bytes = Array.isArray(raw) ? Buffer.concat(raw.map((part) => Buffer.from(part))) : Buffer.from(raw);
  if (bytes.length >= 2 && bytes[0] === 0x1f && bytes[1] === 0x8b) {
    try { return gunzipSync(bytes).toString('utf8'); }
    catch (error) { throw new Error(`invalid HTX gzip WebSocket frame: ${error instanceof Error ? error.message : String(error)}`); }
  }
  // A text JSON frame may arrive as a Buffer in local/proxy environments; compressed market data may not.
  const text = bytes.toString('utf8');
  if (text.startsWith('{') || text.startsWith('[')) return text;
  throw new Error('HTX WebSocket binary frame is not gzip-compressed JSON');
}

export function parseHtxFrame(raw: string | Uint8Array | Uint8Array[]): HtxFrame {
  const text = decodeHtxFrame(raw);
  let value: unknown;
  try { value = parseLosslessJson(text); }
  catch (error) { throw new Error(`invalid HTX Spot JSON: ${error instanceof Error ? error.message : String(error)}`); }
  if (!isRecord(value)) throw new Error('invalid HTX Spot frame: expected object');
  if (value.ping !== undefined) return { kind: 'heartbeat', ping: safeTimestamp(value.ping, 'ping') };
  if (value.pong !== undefined) return { kind: 'ignore' };

  if (typeof value.rep === 'string') {
    const id = value.id == null ? '' : String(value.id);
    const topic = value.rep;
    const error = value.status !== 'ok' ? String(value['err-msg'] ?? value['err-code'] ?? value.status ?? 'HTX refresh request failed') : null;
    if (!error && isRecord(value.data)) {
      const snapshot = parseSnapshot(value.data, topic, safeOptionalTimestamp(value.ts));
      return { kind: 'snapshot', id, topic, snapshot, error: null };
    }
    return { kind: 'snapshot', id, topic, snapshot: emptySnapshot(topic), error };
  }
  if (typeof value.subbed === 'string' || value.status !== undefined && value.id !== undefined) {
    const topic = typeof value.subbed === 'string' ? value.subbed : null;
    const error = value.status === 'ok' ? null : String(value['err-msg'] ?? value['err-code'] ?? value.status);
    return { kind: 'ack', id: value.id == null ? null : String(value.id), topic, error };
  }
  if (typeof value.ch !== 'string' || !isRecord(value.tick)) return { kind: 'ignore' };
  const match = /^market\.([a-z0-9]+)\.mbp\.(5|20|150|400)$/.exec(value.ch);
  if (!match) return { kind: 'ignore' };
  const symbol = match[1];
  const tick = value.tick;
  const sequence = parseSequence(tick.seqNum, 'seqNum');
  const previousSequence = parseSequence(tick.prevSeqNum, 'prevSeqNum');
  if (compareSequence(sequence, previousSequence) <= 0) throw new Error('invalid HTX MBP sequence: seqNum must exceed prevSeqNum');
  if (tick.bids === undefined && tick.asks === undefined) throw new Error('invalid HTX MBP delta: both sides are missing');
  return { kind: 'delta', delta: { symbol, sequence, previousSequence,
    exchangeTimestamp: safeTimestamp(value.ts, 'update timestamp'),
    bids: parseLevels(tick.bids ?? [], 'bids'), asks: parseLevels(tick.asks ?? [], 'asks') } };
}

export function classifyHtxDelta(currentSequence: string, delta: HtxBookDelta, awaitingSnapshotBridge = false): HtxSequenceDisposition {
  const current = BigInt(currentSequence), next = BigInt(delta.sequence), previous = BigInt(delta.previousSequence);
  if (next <= current) return 'STALE';
  if (awaitingSnapshotBridge) return previous === current ? 'BRIDGE' : 'GAP';
  return previous === current ? 'APPLY' : 'GAP';
}

function parseSnapshot(value: Record<string, any>, topic: string, timestamp: number | null): HtxBookSnapshot {
  const match = /^market\.([a-z0-9]+)\.mbp\.(5|20|150|400)$/.exec(topic);
  if (!match) throw new Error(`invalid HTX MBP refresh topic: ${topic}`);
  return { symbol: match[1], sequence: parseSequence(value.seqNum, 'snapshot seqNum'), exchangeTimestamp: timestamp,
    bids: parseLevels(value.bids, 'snapshot bids'), asks: parseLevels(value.asks, 'snapshot asks') };
}
function emptySnapshot(topic: string): HtxBookSnapshot {
  const symbol = /^market\.([a-z0-9]+)\.mbp\.(?:5|20|150|400)$/.exec(topic)?.[1] ?? '';
  return { symbol, sequence: '0', exchangeTimestamp: null, bids: [], asks: [] };
}
function parseLevels(value: unknown, name: string): OrderBookLevel[] {
  if (!Array.isArray(value)) throw new Error(`invalid HTX ${name}: expected array`);
  return value.map((row, index) => {
    if (!Array.isArray(row) || row.length < 2) throw new Error(`invalid HTX ${name}[${index}] level`);
    const price = Number(row[0]), quantity = Number(row[1]);
    if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(quantity) || quantity < 0) throw new Error(`invalid HTX ${name}[${index}] price/size`);
    return { price, quantity };
  });
}
function parseSequence(value: unknown, name: string): string {
  const sequence = typeof value === 'string' ? value : typeof value === 'number' && Number.isSafeInteger(value) ? String(value) : '';
  if (!/^\d+$/.test(sequence)) throw new Error(`invalid HTX ${name}`);
  return sequence;
}
function compareSequence(a: string, b: string) { const x = BigInt(a), y = BigInt(b); return x < y ? -1 : x > y ? 1 : 0; }
function safeTimestamp(value: unknown, name: string) {
  const parsed = typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : value;
  if (typeof parsed !== 'number' || !Number.isSafeInteger(parsed) || parsed <= 0) throw new Error(`invalid HTX ${name}`);
  return parsed;
}
function safeOptionalTimestamp(value: unknown): number | null { return value == null ? null : safeTimestamp(value, 'snapshot timestamp'); }
function parseLosslessJson(raw: string): unknown {
  type Context = { source?: string };
  type Reviver = (this: unknown, key: string, value: unknown, context?: Context) => unknown;
  const parse = JSON.parse as unknown as (text: string, reviver: Reviver) => unknown;
  return parse(raw, function (key, value, context) {
    if ((key === 'seqNum' || key === 'prevSeqNum') && typeof value === 'number') {
      const source = context?.source;
      if (source && /^\d+$/.test(source)) return source;
      if (!Number.isSafeInteger(value) || value < 0) throw new Error(`unsafe HTX ${key} number`);
      return String(value);
    }
    return value;
  });
}
function isRecord(value: unknown): value is Record<string, any> { return typeof value === 'object' && value !== null && !Array.isArray(value); }
