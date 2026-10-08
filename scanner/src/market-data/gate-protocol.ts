import type { OrderBookLevel } from '../types';
import type { LiveMarketSubscription } from './universe';

export const GATE_SPOT_WS_URL = 'wss://api.gateio.ws/ws/v4/';
export const GATE_SPOT_ORDER_BOOK_URL = 'https://api.gateio.ws/api/v4/spot/order_book';
export const GATE_SPOT_ORDER_BOOK_UPDATE_CHANNEL = 'spot.order_book_update';

export type GateBookDelta = { symbol: string; firstUpdateId: number; finalUpdateId: number; exchangeTimestamp: number; bids: OrderBookLevel[]; asks: OrderBookLevel[] };
export type GateBookSnapshot = { symbol: string; updateId: number; exchangeTimestamp: number; bids: OrderBookLevel[]; asks: OrderBookLevel[] };
export type GateFrame =
  | { kind: 'book'; delta: GateBookDelta }
  | { kind: 'ack'; channel: string; event: string; id?: string | number; error: string | null }
  | { kind: 'pong' }
  | { kind: 'ignore' };

/** Use Phase A's Gate currency_pair mapping and validate it against the canonical pair. */
export function mapGateSpotSymbol(subscription: LiveMarketSubscription): string {
  const symbol = (subscription.exchangeSymbol.trim() || `${subscription.canonicalAsset}_${subscription.canonicalQuote}`).toUpperCase();
  if (!/^[A-Z0-9]+_[A-Z0-9]+$/.test(symbol)) throw new Error(`Invalid Gate Spot symbol mapping for ${subscription.canonicalPair}: ${symbol}`);
  const [base, quote] = symbol.split('_');
  if (!base || base !== subscription.canonicalAsset.toUpperCase() || quote !== subscription.canonicalQuote.toUpperCase()) {
    throw new Error(`Gate Spot symbol ${symbol} does not match ${subscription.canonicalPair}`);
  }
  return symbol;
}

export function parseGateFrame(raw: string): GateFrame {
  let value: unknown;
  try { value = JSON.parse(raw); } catch (error) { throw new Error(`invalid Gate Spot JSON: ${error instanceof Error ? error.message : String(error)}`); }
  if (!isRecord(value)) throw new Error('invalid Gate Spot frame: expected object');
  const channel = typeof value.channel === 'string' ? value.channel : '';
  const event = typeof value.event === 'string' ? value.event : '';
  if (channel === 'spot.pong') return { kind: 'pong' };
  if (event && event !== 'update') {
    const error = value.error == null ? null : isRecord(value.error)
      ? `${String(value.error.code ?? 'error')}: ${String(value.error.message ?? 'unknown error')}` : String(value.error);
    return { kind: 'ack', channel, event, ...(typeof value.id === 'string' || typeof value.id === 'number' ? { id: value.id } : {}), error };
  }
  if (channel !== GATE_SPOT_ORDER_BOOK_UPDATE_CHANNEL || event !== 'update') return { kind: 'ignore' };
  if (!isRecord(value.result)) throw new Error('invalid Gate Spot depth update: result must be an object');
  const result = value.result;
  const symbol = typeof result.s === 'string' ? result.s.toUpperCase() : '';
  if (!/^[A-Z0-9]+_[A-Z0-9]+$/.test(symbol)) throw new Error('invalid Gate Spot depth update symbol');
  const firstUpdateId = safeInteger(result.U, 'first update ID');
  const finalUpdateId = safeInteger(result.u, 'final update ID');
  if (firstUpdateId > finalUpdateId) throw new Error('invalid Gate Spot depth update range: U exceeds u');
  const exchangeTimestamp = safeInteger(result.t, 'update timestamp');
  if (exchangeTimestamp <= 0) throw new Error('invalid Gate Spot depth update timestamp');
  return { kind: 'book', delta: { symbol, firstUpdateId, finalUpdateId, exchangeTimestamp,
    bids: parseLevels(result.b, 'bids'), asks: parseLevels(result.a, 'asks') } };
}

export function parseGateSnapshot(raw: unknown, expectedSymbol: string): GateBookSnapshot {
  if (!isRecord(raw)) throw new Error('invalid Gate Spot REST order-book snapshot');
  const symbol = expectedSymbol.toUpperCase();
  const updateId = safeInteger(raw.id, 'snapshot ID');
  const exchangeTimestamp = safeInteger(raw.update ?? raw.current, 'snapshot timestamp');
  if (exchangeTimestamp <= 0) throw new Error('invalid Gate Spot snapshot timestamp');
  return { symbol, updateId, exchangeTimestamp, bids: parseLevels(raw.bids, 'snapshot bids', false), asks: parseLevels(raw.asks, 'snapshot asks', false) };
}

export type GateDeltaDisposition = 'STALE' | 'BRIDGE' | 'APPLY' | 'GAP';
/** Gate ranges bridge the next expected ID and may overlap; subsequent U must cover current+1. */
export function classifyGateDelta(currentUpdateId: number, delta: GateBookDelta, bootstrap = false): GateDeltaDisposition {
  const expected = currentUpdateId + 1;
  if (delta.finalUpdateId < expected) return 'STALE';
  if (delta.firstUpdateId > expected) return 'GAP';
  return bootstrap ? 'BRIDGE' : 'APPLY';
}

function parseLevels(value: unknown, name: string, allowZero = true): OrderBookLevel[] {
  if (!Array.isArray(value)) throw new Error(`invalid Gate Spot ${name}: expected array`);
  return value.map((row, index) => {
    if (!Array.isArray(row) || row.length < 2) throw new Error(`invalid Gate Spot ${name}[${index}] level`);
    const price = Number(row[0]), quantity = Number(row[1]);
    if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(quantity) || (allowZero ? quantity < 0 : quantity <= 0)) {
      throw new Error(`invalid Gate Spot ${name}[${index}] price/amount`);
    }
    return { price, quantity };
  });
}
function safeInteger(value: unknown, name: string) {
  const parsed = typeof value === 'number' ? value : typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : NaN;
  if (!Number.isSafeInteger(parsed) || parsed < 0) throw new Error(`invalid Gate Spot ${name}`);
  return parsed;
}
function isRecord(value: unknown): value is Record<string, any> { return typeof value === 'object' && value !== null && !Array.isArray(value); }
