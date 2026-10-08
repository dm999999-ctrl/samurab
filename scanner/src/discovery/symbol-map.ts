import type { NormalizedSpotMarket } from './types';

export const STABLECOIN_SYMBOLS = new Set([
  'USDT','USDC','USD','FDUSD','DAI','TUSD','USDP','BUSD','USDE','USDS','PYUSD','GUSD','USDD','FRAX','LUSD','EURC','EURS','USDK','USTC','UST','USDJ','XAUT','PAXG',
]);
export const MAJOR_QUOTE_ASSETS = ['USDT','USDC','USD','FDUSD','DAI'] as const;
// Only aliases with a widely documented common asset identity are normalized here.
// Wrapped and bridged assets intentionally remain distinct canonical assets.
export const VERIFIED_ASSET_ALIASES: Readonly<Record<string, string>> = { XBT: 'BTC', BCC: 'BCH' };
export const AMBIGUOUS_BASE_SYMBOLS = new Set(['PAY','ONE','HOT','REN','FLOW','SNT','POLY']);

const leveragedSuffix = /(?:\d+[LS]|(?:UP|DOWN|BULL|BEAR|HALF|HEDGE))$/i;
export function isLeveragedOrSynthetic(symbol: string): boolean {
  return leveragedSuffix.test(symbol.replace(/[-_]/g, ''));
}
export function classifyAsset(base: string): { assetId: string; symbol: string; mapping: NormalizedSpotMarket['assetMapping'] } {
  const original = base.toUpperCase();
  if (AMBIGUOUS_BASE_SYMBOLS.has(original)) return { assetId: `ambiguous:${original}`, symbol: original, mapping: 'ambiguous' };
  const canonical = VERIFIED_ASSET_ALIASES[original] ?? original;
  return { assetId: `asset:${canonical}`, symbol: canonical, mapping: canonical === original ? 'exact-symbol' : 'known-alias' };
}
export function canonicalizeMarket(market: Omit<NormalizedSpotMarket, 'canonicalAssetId' | 'canonicalPair' | 'assetMapping'>): NormalizedSpotMarket {
  const mapped = classifyAsset(market.baseAsset);
  return { ...market, canonicalAssetId: mapped.assetId, canonicalPair: `${mapped.symbol}/${market.quoteAsset}`, assetMapping: mapped.mapping };
}
export function isStablecoin(symbol: string): boolean { return STABLECOIN_SYMBOLS.has(symbol.toUpperCase()); }
