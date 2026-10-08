import type { ExchangeId, NormalizedSpotMarket } from './types';

export type MarketDraft = Omit<NormalizedSpotMarket, 'canonicalAssetId' | 'canonicalPair' | 'assetMapping'>;
export type TickerDraft = { symbol: string; volumeBase24h?: number; volumeQuote24h?: number; turnover24h?: number; lastPrice?: number; exchangeTimestamp?: number; };
export function object(value: unknown, label: string): Record<string, any> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`Malformed ${label}: expected object`);
  return value as Record<string, any>;
}
export function list(value: unknown, label: string): unknown[] {
  if (!Array.isArray(value)) throw new Error(`Malformed ${label}: expected array`);
  return value;
}
export function rowsAt(payload: unknown, path: string[], label: string): unknown[] {
  let value: any = payload;
  for (const part of path) value = value?.[part];
  return list(value, label);
}
export function text(value: unknown): string | undefined {
  if (typeof value === 'string' && value.trim()) return value.trim();
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  return undefined;
}
export function numeric(value: unknown): number | undefined {
  if (value === null || value === undefined || value === '') return undefined;
  const parsed = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}
export function precision(value: unknown): number | undefined {
  const raw = text(value);
  if (!raw) return undefined;
  if (raw.includes('e-')) return Number(raw.split('e-')[1]);
  const dot = raw.indexOf('.');
  return dot < 0 ? 0 : raw.length - dot - 1;
}
export function makeMarket(exchange: ExchangeId, symbol: unknown, base: unknown, quote: unknown, status: unknown, extra: Partial<MarketDraft> = {}): MarketDraft | undefined {
  const exchangeSymbol = text(symbol);
  const baseAsset = text(base)?.toUpperCase();
  const quoteAsset = text(quote)?.toUpperCase();
  if (!exchangeSymbol || !baseAsset || !quoteAsset || baseAsset === quoteAsset) return undefined;
  return { exchange, exchangeSymbol, baseAsset, quoteAsset, marketType: 'spot', status: text(status) ?? 'unknown', ...extra };
}
export function makeTickers(rows: unknown[], symbolKey: string, pick: (row: Record<string, any>) => Omit<TickerDraft, 'symbol'>): Map<string, TickerDraft> {
  const result = new Map<string, TickerDraft>();
  for (const item of rows) {
    const row = object(item, 'ticker row');
    const symbol = text(row[symbolKey]);
    if (symbol) result.set(symbol.toUpperCase(), { symbol, ...pick(row) });
  }
  return result;
}
export function mergeTicker(market: MarketDraft, tickers: Map<string, TickerDraft>): MarketDraft {
  const ticker = tickers.get(market.exchangeSymbol.toUpperCase());
  if (!ticker) return market;
  const { symbol: _symbol, ...values } = ticker;
  return { ...market, ...values };
}
