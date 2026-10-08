import type { OrderBookLevel } from '../types';
import type { LiveMarketSubscription } from './universe';

export const COINBASE_SPOT_WS_URL = 'wss://advanced-trade-ws.coinbase.com';
export const COINBASE_PRODUCT_BATCH_SIZE = 100;

export function mapCoinbaseSpotProduct(item: LiveMarketSubscription): string {
  const product = item.exchangeSymbol.trim().toUpperCase();
  if (!/^[A-Z0-9]+-[A-Z0-9]+$/.test(product)) {
    throw new Error(`Invalid Coinbase Spot product ${item.exchangeSymbol}; expected BASE-QUOTE`);
  }
  return product;
}

export type CoinbaseBookFrame = {
  kind: 'book';
  productId: string;
  type: 'snapshot' | 'update';
  sequence: number | string;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
  exchangeTimestamp: number | null;
};
export type CoinbaseFrame = CoinbaseBookFrame
  | { kind: 'heartbeat'; sequence: number | string | null; exchangeTimestamp: number | null }
  | { kind: 'batch'; sequence: number | string; frames: CoinbaseBookFrame[] }
  | { kind: 'subscriptions'; sequence: number | string | null }
  | { kind: 'error'; message: string }
  | { kind: 'ignore' };

export type CoinbaseContinuity = 'APPLY' | 'UNSEQUENCED' | 'BASELINE' | 'STALE' | 'GAP';

export function parseCoinbaseFrame(raw: string): CoinbaseFrame {
  let value: unknown;
  try { value = JSON.parse(raw); } catch { throw new Error('Coinbase sent invalid JSON'); }
  if (!isRecord(value) || (typeof value.type !== 'string' && typeof value.channel !== 'string')) throw new Error('Coinbase WebSocket frame must include type or channel');
  if (value.type === 'subscriptions' || value.channel === 'subscriptions') {
    return { kind: 'subscriptions', sequence: parseOptionalSequence(value.sequence_num) };
  }
  if (value.type === 'error') {
    return { kind: 'error', message: [value.message, value.reason].filter((part) => typeof part === 'string').join(': ') || 'unknown Coinbase WebSocket error' };
  }
  if (value.channel === 'heartbeats') {
    const heartbeat = Array.isArray(value.events) ? value.events[0] : undefined;
    return { kind: 'heartbeat', sequence: parseOptionalSequence(value.sequence_num),
      exchangeTimestamp: isRecord(heartbeat) ? parseOptionalTimestamp(heartbeat.current_time) : parseOptionalTimestamp(value.timestamp) };
  }
  if (value.channel === 'l2_data') {
    if (!Array.isArray(value.events)) throw new Error('Coinbase l2_data envelope missing events');
    const sequence = parseRequiredSequence(value.sequence_num);
    const frames: CoinbaseBookFrame[] = [];
    for (const event of value.events) {
      if (!isRecord(event) || (event.type !== 'snapshot' && event.type !== 'update') || !Array.isArray(event.updates)) {
        throw new Error('Coinbase l2_data event must contain snapshot/update and updates');
      }
      const bids: OrderBookLevel[] = [], asks: OrderBookLevel[] = [];
      for (const update of event.updates) {
        if (!isRecord(update) || (update.side !== 'bid' && update.side !== 'offer') || typeof update.price_level !== 'string' || typeof update.new_quantity !== 'string') {
          throw new Error('Coinbase L2 update must contain bid|offer, price_level, and new_quantity');
        }
        const price = Number(update.price_level), quantity = Number(update.new_quantity);
        if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(quantity) || quantity < 0) throw new Error('Coinbase L2 update has invalid price/quantity');
        (update.side === 'bid' ? bids : asks).push({ price, quantity });
      }
      frames.push({ kind: 'book', type: event.type, productId: parseProductId(event.product_id), sequence,
        bids, asks, exchangeTimestamp: parseOptionalTimestamp(value.timestamp) });
    }
    return { kind: 'batch', sequence, frames };
  }
  // Coinbase documents that clients must ignore message types they do not support.
  return { kind: 'ignore' };
}

/** Coinbase Exchange sequence numbers are per product; gaps mean messages were dropped and lower values are old/out of order. */
export function classifyCoinbaseSequence(current: number | string | null, observed: number | string | null): CoinbaseContinuity {
  if (observed === null) return 'UNSEQUENCED';
  if (current === null) return 'BASELINE';
  const previous = BigInt(current), next = BigInt(observed);
  if (next <= previous) return 'STALE';
  return next === previous + 1n ? 'APPLY' : 'GAP';
}

function parseProductId(value: unknown): string {
  if (typeof value !== 'string' || !/^[A-Z0-9]+-[A-Z0-9]+$/i.test(value)) throw new Error('Coinbase frame has invalid product_id');
  return value.toUpperCase();
}
function parseOptionalSequence(value: unknown): number | string | null {
  if (value === undefined || value === null) return null;
  if (typeof value === 'string' && /^\d+$/.test(value)) return value;
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return value;
  throw new Error('Coinbase frame has invalid sequence');
}
function parseRequiredSequence(value: unknown): number | string {
  const sequence = parseOptionalSequence(value);
  if (sequence === null) throw new Error('Coinbase L2 envelope missing sequence_num');
  return sequence;
}
function parseOptionalTimestamp(value: unknown): number | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== 'string') throw new Error('Coinbase frame has invalid time');
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp) || timestamp <= 0) throw new Error('Coinbase frame has invalid time');
  return timestamp;
}
function isRecord(value: unknown): value is Record<string, unknown> { return typeof value === 'object' && value !== null && !Array.isArray(value); }
