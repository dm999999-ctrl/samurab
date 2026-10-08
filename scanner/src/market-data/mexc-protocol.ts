import type { OrderBookLevel } from '../types';
import type { LiveMarketSubscription } from './universe';

export const MEXC_SPOT_WS_URL = 'wss://wbs-api.mexc.com/ws';
export const MEXC_DEPTH_SNAPSHOT_URL = 'https://api.mexc.com/api/v3/depth';
export const MEXC_AGGREGATE_DEPTH_CHANNEL = 'spot@public.aggre.depth.v3.api.pb@100ms';
export const MEXC_MAX_SUBSCRIPTIONS_PER_SOCKET = 30;

export type MexcDepthDelta = {
  symbol: string;
  fromVersion: number;
  toVersion: number;
  exchangeTimestamp: number;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
};
export type MexcDepthSnapshot = {
  symbol: string;
  lastUpdateId: number;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
};
export type MexcFrame =
  | { kind: 'delta'; delta: MexcDepthDelta }
  | { kind: 'ack'; id: number; code: number; message: string }
  | { kind: 'pong'; id?: number }
  | { kind: 'ignore' };

/** Phase A publishes MEXC's native concatenated Spot symbols, e.g. BTCUSDT. */
export function mapMexcSpotSymbol(subscription: LiveMarketSubscription): string {
  const symbol = subscription.exchangeSymbol.trim().toUpperCase();
  const expected = `${subscription.canonicalAsset}${subscription.canonicalQuote}`.toUpperCase();
  if (!/^[A-Z0-9]+$/.test(symbol) || symbol !== expected) {
    throw new Error(`Invalid MEXC Spot symbol mapping for ${subscription.canonicalPair}: ${symbol}; expected ${expected}`);
  }
  return symbol;
}

export function mexcDepthChannel(symbol: string): string {
  return `${MEXC_AGGREGATE_DEPTH_CHANNEL}@${symbol.toUpperCase()}`;
}

/** MEXC sends market pushes as PushDataV3ApiWrapper protobuf binary frames. */
export function parseMexcFrame(raw: Uint8Array | string, isBinary = typeof raw !== 'string'): MexcFrame {
  if (!isBinary) {
    let value: unknown;
    try { value = JSON.parse(typeof raw === 'string' ? raw : new TextDecoder().decode(raw)); }
    catch (error) { throw new Error(`invalid MEXC control JSON: ${error instanceof Error ? error.message : String(error)}`); }
    if (!isRecord(value)) throw new Error('invalid MEXC control frame');
    const id = integer(value.id, 'control id', true);
    if (value.msg === 'PONG' || value.method === 'PONG') return { kind: 'pong', ...(id === null ? {} : { id }) };
    if (value.id !== undefined && value.code !== undefined) {
      return { kind: 'ack', id: id ?? 0, code: integer(value.code, 'response code')!, message: String(value.msg ?? '') };
    }
    return { kind: 'ignore' };
  }
  if (typeof raw === 'string') throw new Error('MEXC protobuf market frame must be binary');
  const wrapper = readFields(raw);
  const channel = stringField(wrapper, 1, 'channel', false) ?? '';
  const symbol = stringField(wrapper, 3, 'symbol', false)?.toUpperCase();
  const body = bytesField(wrapper, 313);
  if (!body) {
    if (channel.startsWith('spot@public.aggre.depth.v3.api.pb')) throw new Error('MEXC depth wrapper is missing publicAggreDepths protobuf body');
    return { kind: 'ignore' };
  }
  if (!symbol || !/^[A-Z0-9]+$/.test(symbol)) throw new Error('invalid MEXC depth wrapper symbol');
  const depthFields = readFields(body);
  const fromVersion = safeVersion(versionField(depthFields, 4, 'fromVersion'));
  const toVersion = safeVersion(versionField(depthFields, 5, 'toVersion'));
  if (fromVersion > toVersion) throw new Error('invalid MEXC depth range: fromVersion exceeds toVersion');
  const exchangeTimestamp = safeVersion(varintField(wrapper, 6) ?? varintField(wrapper, 5) ?? 0n);
  if (exchangeTimestamp <= 0) throw new Error('invalid MEXC depth push timestamp');
  return { kind: 'delta', delta: {
    symbol, fromVersion, toVersion, exchangeTimestamp,
    asks: levelList(depthFields, 1, 'asks'), bids: levelList(depthFields, 2, 'bids'),
  } };
}

export function parseMexcSnapshot(raw: unknown, expectedSymbol: string): MexcDepthSnapshot {
  if (!isRecord(raw)) throw new Error('invalid MEXC depth snapshot response');
  const symbol = String(raw.symbol ?? expectedSymbol).toUpperCase();
  if (symbol !== expectedSymbol.toUpperCase()) throw new Error(`MEXC snapshot symbol mismatch: expected ${expectedSymbol}, got ${symbol}`);
  const lastUpdateId = safeVersion(raw.lastUpdateId);
  return { symbol, lastUpdateId, bids: snapshotLevels(raw.bids, 'bids'), asks: snapshotLevels(raw.asks, 'asks') };
}

export type MexcDeltaDisposition = 'STALE' | 'BRIDGE' | 'APPLY' | 'GAP';

/**
 * Snapshot bootstrap permits one range containing snapshot.lastUpdateId.
 * Once live, MEXC requires each fromVersion to equal previous toVersion + 1.
 */
export function classifyMexcDelta(currentVersion: number, delta: MexcDepthDelta, bootstrap = false): MexcDeltaDisposition {
  if (bootstrap ? delta.toVersion < currentVersion : delta.toVersion <= currentVersion) return 'STALE';
  if (bootstrap) {
    if (delta.fromVersion <= currentVersion && currentVersion <= delta.toVersion) return 'BRIDGE';
    return delta.fromVersion > currentVersion ? 'GAP' : 'STALE';
  }
  return delta.fromVersion === currentVersion + 1 ? 'APPLY' : 'GAP';
}

function readFields(bytes: Uint8Array) {
  const fields: Array<{ number: number; wire: number; bytes?: Uint8Array; integer?: bigint }> = [];
  let offset = 0;
  while (offset < bytes.length) {
    const tag = readVarint(bytes, offset); offset = tag.offset;
    const number = Number(tag.value >> 3n), wire = Number(tag.value & 7n);
    if (number <= 0) throw new Error('invalid MEXC protobuf field tag');
    if (wire === 0) {
      const value = readVarint(bytes, offset); offset = value.offset;
      fields.push({ number, wire, integer: value.value });
    } else if (wire === 2) {
      const length = readVarint(bytes, offset); offset = length.offset;
      if (length.value > BigInt(bytes.length - offset)) throw new Error('truncated MEXC protobuf length-delimited field');
      const end = offset + Number(length.value);
      fields.push({ number, wire, bytes: bytes.subarray(offset, end) }); offset = end;
    } else if (wire === 1 || wire === 5) {
      const size = wire === 1 ? 8 : 4;
      if (offset + size > bytes.length) throw new Error('truncated MEXC protobuf fixed-width field');
      fields.push({ number, wire }); offset += size;
    } else throw new Error(`unsupported MEXC protobuf wire type ${wire}`);
  }
  return fields;
}

function readVarint(bytes: Uint8Array, start: number) {
  let value = 0n, shift = 0n, offset = start;
  while (offset < bytes.length && shift < 70n) {
    const byte = bytes[offset++];
    value |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) return { value, offset };
    shift += 7n;
  }
  throw new Error('invalid/truncated MEXC protobuf varint');
}

function bytesField(fields: ReturnType<typeof readFields>, field: number) { return fields.find((item) => item.number === field && item.wire === 2)?.bytes; }
function stringField(fields: ReturnType<typeof readFields>, field: number, name: string, required = true): string | undefined {
  const bytes = bytesField(fields, field);
  if (!bytes) { if (required) throw new Error(`MEXC protobuf missing ${name}`); return undefined; }
  try { return new TextDecoder('utf-8', { fatal: true }).decode(bytes); }
  catch { throw new Error(`invalid MEXC protobuf ${name} UTF-8`); }
}
function varintField(fields: ReturnType<typeof readFields>, field: number) { return fields.find((item) => item.number === field && item.wire === 0)?.integer; }
function versionField(fields: ReturnType<typeof readFields>, field: number, name: string): unknown {
  const entry = fields.find((item) => item.number === field);
  if (!entry) throw new Error(`MEXC protobuf missing ${name}`);
  if (entry.wire === 0 && entry.integer !== undefined) return entry.integer;
  if (entry.wire === 2 && entry.bytes) return stringField(fields, field, name);
  throw new Error(`invalid MEXC protobuf ${name} wire type`);
}
function levelList(fields: ReturnType<typeof readFields>, field: number, side: string): OrderBookLevel[] {
  return fields.filter((item) => item.number === field && item.wire === 2).map((item) => {
    const row = readFields(item.bytes!);
    const price = Number(stringField(row, 1, `${side} price`));
    const quantity = Number(stringField(row, 2, `${side} quantity`));
    if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(quantity) || quantity < 0) throw new Error(`invalid MEXC ${side} level`);
    return { price, quantity };
  });
}
function snapshotLevels(value: unknown, side: string): OrderBookLevel[] {
  if (!Array.isArray(value)) throw new Error(`invalid MEXC snapshot ${side}`);
  return value.map((row) => {
    if (!Array.isArray(row) || row.length < 2) throw new Error(`invalid MEXC snapshot ${side} level`);
    const price = Number(row[0]), quantity = Number(row[1]);
    if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(quantity) || quantity <= 0) throw new Error(`invalid MEXC snapshot ${side} price/quantity`);
    return { price, quantity };
  });
}
function safeVersion(value: unknown): number {
  let result: number;
  if (typeof value === 'bigint') {
    if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('MEXC version/timestamp exceeds safe integer range');
    result = Number(value);
  } else if (typeof value === 'number') result = value;
  else if (typeof value === 'string' && /^\d+$/.test(value)) result = Number(value);
  else throw new Error('invalid MEXC version/snapshot update ID');
  if (!Number.isSafeInteger(result) || result < 0) throw new Error('invalid MEXC version/snapshot update ID');
  return result;
}
function integer(value: unknown, name: string, optional = false): number | null {
  if (optional && value === undefined) return null;
  const parsed = typeof value === 'number' ? value : typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : NaN;
  if (!Number.isSafeInteger(parsed) || parsed < 0) throw new Error(`invalid MEXC ${name}`);
  return parsed;
}
function isRecord(value: unknown): value is Record<string, any> { return typeof value === 'object' && value !== null && !Array.isArray(value); }
