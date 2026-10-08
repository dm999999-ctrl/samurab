import type { OrderBookLevel } from '../types';
import type { LiveMarketSubscription } from './universe';

export const OKX_PUBLIC_SPOT_WS_URL = 'wss://ws.okx.com/ws/v5/public';

export type OkxSequence = number;
export type OkxBookPush = {
  instrumentId: string;
  action: 'snapshot' | 'update';
  sequence: OkxSequence;
  previousSequence: OkxSequence | null;
  exchangeTimestamp: number;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
};
export type OkxFrame =
  | { kind: 'pong' }
  | { kind: 'event'; event: string; code?: string; message?: string; instrumentId?: string }
  | { kind: 'book'; push: OkxBookPush }
  | { kind: 'ignore' };

/** Prefer Phase A's exchange instrument mapping, with canonical mapping as a safe fallback. */
export function mapOkxSpotInstrument(subscription: LiveMarketSubscription): string {
  const candidate = subscription.exchangeSymbol.trim().toUpperCase() || `${subscription.canonicalAsset}-${subscription.canonicalQuote}`;
  if (!/^[A-Z0-9]+-[A-Z0-9]+$/.test(candidate)) {
    throw new Error(`Invalid OKX Spot instrument mapping for ${subscription.canonicalPair}: ${candidate}`);
  }
  const [base, quote] = candidate.split('-');
  if (quote !== subscription.canonicalQuote.toUpperCase() || !base) {
    throw new Error(`OKX Spot instrument ${candidate} does not match ${subscription.canonicalPair}`);
  }
  return candidate;
}

export function parseOkxFrame(raw: string): OkxFrame {
  if (raw === 'pong') return { kind: 'pong' };
  let value: unknown;
  try { value = JSON.parse(raw); } catch (error) {
    throw new Error(`invalid OKX JSON: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!isRecord(value)) throw new Error('invalid OKX frame: expected object');
  if (typeof value.event === 'string') {
    return { kind: 'event', event: value.event, code: stringField(value.code), message: stringField(value.msg),
      instrumentId: isRecord(value.arg) ? stringField(value.arg.instId) : undefined };
  }
  if (!isRecord(value.arg) || value.arg.channel !== 'books') return { kind: 'ignore' };
  if (!Array.isArray(value.data)) throw new Error('invalid OKX books frame: data must be an array');
  if (value.data.length !== 1 || !isRecord(value.data[0])) throw new Error('invalid OKX books frame: expected one data object');
  const data = value.data[0];
  if (typeof value.arg.instId !== 'string' || (value.action !== 'snapshot' && value.action !== 'update')) {
    throw new Error('invalid OKX books frame: missing instrument or action');
  }
  const sequence = sequenceField(data.seqId, 'seqId');
  const previousSequence = value.action === 'snapshot' ? null : sequenceField(data.prevSeqId, 'prevSeqId');
  const exchangeTimestamp = Number(data.ts);
  if (!Number.isFinite(exchangeTimestamp) || exchangeTimestamp <= 0) throw new Error('invalid OKX books frame: invalid ts');
  return { kind: 'book', push: {
    instrumentId: value.arg.instId.toUpperCase(), action: value.action, sequence, previousSequence,
    exchangeTimestamp, bids: parseLevels(data.bids, 'bids'), asks: parseLevels(data.asks, 'asks'),
  } };
}

/** OKX sequence IDs may skip values; continuity is prevSeqId === current seqId. */
export function classifyOkxUpdate(current: number, push: OkxBookPush): 'CONTIGUOUS' | 'HEARTBEAT' | 'GAP' {
  if (push.previousSequence !== current) return 'GAP';
  if (push.sequence === current && push.bids.length === 0 && push.asks.length === 0) return 'HEARTBEAT';
  if (push.sequence <= current) return 'GAP';
  return 'CONTIGUOUS';
}

function parseLevels(value: unknown, field: string): OrderBookLevel[] {
  if (!Array.isArray(value)) throw new Error(`invalid OKX books frame: ${field} must be an array`);
  return value.map((row, index) => {
    if (!Array.isArray(row) || row.length < 2) throw new Error(`invalid OKX ${field}[${index}] level`);
    const price = Number(row[0]), quantity = Number(row[1]);
    if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(quantity) || quantity < 0) {
      throw new Error(`invalid OKX ${field}[${index}] price/quantity`);
    }
    return { price, quantity };
  });
}
function sequenceField(value: unknown, name: string): number {
  const parsed = typeof value === 'number' ? value : typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : NaN;
  if (!Number.isSafeInteger(parsed) || parsed < 0) throw new Error(`invalid OKX books frame: invalid ${name}`);
  return parsed;
}
function stringField(value: unknown) { return typeof value === 'string' ? value : undefined; }
function isRecord(value: unknown): value is Record<string, any> { return typeof value === 'object' && value !== null && !Array.isArray(value); }
