import type { OrderBookLevel } from '../types';
import type { LiveMarketSubscription } from './universe';

export const KUCOIN_PUBLIC_TOKEN_URL = 'https://api.kucoin.com/api/v1/bullet-public';
export const KUCOIN_FULL_ORDERBOOK_URL = 'https://api.kucoin.com/api/v1/market/orderbook/level2_100';
export const KUCOIN_LEVEL2_TOPIC = '/market/level2';

export type KucoinDelta = {
  symbol: string;
  sequenceStart: number;
  sequenceEnd: number;
  exchangeTimestamp: number;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
};
export type KucoinSnapshot = {
  symbol: string;
  sequence: number;
  exchangeTimestamp: number;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
};
export type KucoinFrame =
  | { kind: 'welcome'; pingIntervalMs?: number; pingTimeoutMs?: number; id?: string }
  | { kind: 'ack'; id?: string }
  | { kind: 'ping' | 'pong'; id?: string; timestamp?: number }
  | { kind: 'delta'; delta: KucoinDelta }
  | { kind: 'error'; message: string; id?: string }
  | { kind: 'ignore' };

export function mapKucoinSpotSymbol(subscription: LiveMarketSubscription): string {
  const symbol = subscription.exchangeSymbol.trim().toUpperCase();
  if (!/^[A-Z0-9]+-[A-Z0-9]+$/.test(symbol) || symbol.split('-')[0] !== subscription.canonicalAsset.toUpperCase()) {
    throw new Error(`Invalid KuCoin Spot symbol mapping for ${subscription.canonicalPair}: ${symbol}`);
  }
  const [, quote] = symbol.split('-');
  if (quote !== subscription.canonicalQuote.toUpperCase()) {
    throw new Error(`KuCoin Spot symbol ${symbol} does not match ${subscription.canonicalPair}`);
  }
  return symbol;
}

export function parseKucoinFrame(raw: string): KucoinFrame {
  let value: unknown;
  try { value = JSON.parse(raw); } catch (error) {
    throw new Error(`invalid KuCoin JSON: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!isRecord(value) || typeof value.type !== 'string') throw new Error('invalid KuCoin frame: missing type');
  const id = value.id === undefined ? undefined : String(value.id);
  if (value.type === 'welcome') return { kind: 'welcome', id };
  if (value.type === 'ack') return { kind: 'ack', id };
  if (value.type === 'ping' || value.type === 'pong') {
    return { kind: value.type, id, timestamp: optionalSequence(value.timestamp) };
  }
  if (value.type === 'error') return { kind: 'error', id, message: String(value.data ?? value.msg ?? value.code ?? 'KuCoin WebSocket error') };
  if (value.type !== 'message') return { kind: 'ignore' };
  if (typeof value.topic !== 'string' || !value.topic.startsWith(`${KUCOIN_LEVEL2_TOPIC}:`) || !isRecord(value.data)) return { kind: 'ignore' };
  const data = value.data;
  const symbol = typeof data.symbol === 'string' ? data.symbol.toUpperCase() : value.topic.slice(`${KUCOIN_LEVEL2_TOPIC}:`.length).toUpperCase();
  if (!/^[A-Z0-9]+-[A-Z0-9]+$/.test(symbol) || !isRecord(data.changes)) throw new Error('invalid KuCoin Level 2 update: symbol or changes missing');
  const sequenceStart = sequence(data.sequenceStart, 'sequenceStart');
  const sequenceEnd = sequence(data.sequenceEnd, 'sequenceEnd');
  if (sequenceStart > sequenceEnd) throw new Error('invalid KuCoin Level 2 update: sequenceStart exceeds sequenceEnd');
  const exchangeTimestamp = Number(data.time);
  if (!Number.isFinite(exchangeTimestamp) || exchangeTimestamp <= 0) throw new Error('invalid KuCoin Level 2 update: invalid time');
  return { kind: 'delta', delta: {
    symbol, sequenceStart, sequenceEnd, exchangeTimestamp,
    bids: parseChanges(data.changes.bids, 'bids'), asks: parseChanges(data.changes.asks, 'asks'),
  } };
}

export function parseKucoinSnapshot(raw: unknown, expectedSymbol: string): KucoinSnapshot {
  if (!isRecord(raw) || raw.code !== '200000' || !isRecord(raw.data)) {
    throw new Error(`KuCoin snapshot request failed: ${isRecord(raw) ? String(raw.msg ?? raw.code ?? 'invalid response') : 'invalid response'}`);
  }
  const data = raw.data;
  const symbol = typeof data.symbol === 'string' ? data.symbol.toUpperCase() : expectedSymbol.toUpperCase();
  if (symbol !== expectedSymbol.toUpperCase()) throw new Error(`KuCoin snapshot symbol mismatch: expected ${expectedSymbol}, got ${symbol}`);
  return {
    symbol,
    sequence: sequence(data.sequence, 'snapshot sequence'),
    exchangeTimestamp: positiveTime(data.time, 'snapshot time'),
    bids: parseSnapshotLevels(data.bids, 'bids'), asks: parseSnapshotLevels(data.asks, 'asks'),
  };
}

/** KuCoin permits overlapping ranges; only a start beyond oldEnd + 1 is a gap. */
export function classifyKucoinDelta(oldEnd: number, delta: KucoinDelta): 'STALE' | 'APPLY' | 'GAP' {
  if (delta.sequenceEnd <= oldEnd) return 'STALE';
  if (delta.sequenceStart > oldEnd + 1) return 'GAP';
  return 'APPLY';
}

function parseChanges(value: unknown, side: string): OrderBookLevel[] {
  if (!Array.isArray(value)) throw new Error(`invalid KuCoin Level 2 update: ${side} changes must be an array`);
  const changes: OrderBookLevel[] = [];
  for (const row of value) {
    if (!Array.isArray(row) || row.length < 3) throw new Error(`invalid KuCoin ${side} change`);
    const price = Number(row[0]), quantity = Number(row[1]);
    // KuCoin specifies price=0 changes as ignored while still advancing the message sequence.
    if (price === 0) continue;
    if (!Number.isFinite(price) || price < 0 || !Number.isFinite(quantity) || quantity < 0) throw new Error(`invalid KuCoin ${side} price/size`);
    sequence(row[2], 'price sequence'); // Validated for shape only; it is not the message continuity key.
    changes.push({ price, quantity });
  }
  return changes;
}
function parseSnapshotLevels(value: unknown, side: string): OrderBookLevel[] {
  if (!Array.isArray(value)) throw new Error(`invalid KuCoin snapshot: ${side} must be an array`);
  return value.map((row) => {
    if (!Array.isArray(row) || row.length < 2) throw new Error(`invalid KuCoin snapshot ${side} level`);
    const price = Number(row[0]), quantity = Number(row[1]);
    if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(quantity) || quantity <= 0) throw new Error(`invalid KuCoin snapshot ${side} price/size`);
    return { price, quantity };
  });
}
function sequence(value: unknown, field: string): number {
  const parsed = typeof value === 'number' ? value : typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : NaN;
  if (!Number.isSafeInteger(parsed) || parsed < 0) throw new Error(`invalid KuCoin ${field}`);
  return parsed;
}
function optionalSequence(value: unknown) {
  return value === undefined ? undefined : sequence(value, 'timestamp');
}
function positiveTime(value: unknown, field: string) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) throw new Error(`invalid KuCoin ${field}`);
  return parsed;
}
function isRecord(value: unknown): value is Record<string, any> { return typeof value === 'object' && value !== null && !Array.isArray(value); }
