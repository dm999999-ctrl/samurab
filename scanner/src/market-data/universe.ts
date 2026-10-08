import { readFile } from 'node:fs/promises';
import type { DiscoveryReport, EligiblePair, ExchangeId, NormalizedSpotMarket } from '../discovery/types';

export type LiveMarketSubscription = {
  exchange: ExchangeId;
  canonicalAsset: string;
  canonicalQuote: string;
  canonicalPair: string;
  exchangeSymbol: string;
};

export type LiveUniverse = {
  sourceRunId: string;
  sourceStatus: DiscoveryReport['status'];
  candidatePairCount: number;
  selectedPairCount: number;
  subscriptions: LiveMarketSubscription[];
};

export type LiveUniverseOptions = {
  maxPairs?: number;
  pairs?: string[];
  exchanges?: ExchangeId[];
  requiredExchanges?: ExchangeId[];
  eligibleSymbols?: Partial<Record<ExchangeId, readonly string[]>>;
};

export function selectLiveUniverse(report: DiscoveryReport, options: LiveUniverseOptions = {}): LiveUniverse {
  if (report.schemaVersion !== 1 || report.status === 'FAILED') throw new Error('Phase A discovery report is invalid or failed');
  const requested = options.pairs?.length ? new Set(options.pairs.map(normalizePair)) : undefined;
  const requestedExchanges = options.exchanges ? new Set(options.exchanges) : undefined;
  const requiredExchanges = options.requiredExchanges ? new Set(options.requiredExchanges) : requestedExchanges;
  const eligibleSymbols = new Map(Object.entries(options.eligibleSymbols ?? {}).map(([exchange, symbols]) => [exchange, new Set(symbols)]));
  const availableMarkets = (pair: EligiblePair) => pair.markets.filter((market) => {
    const eligible = eligibleSymbols.get(market.exchange);
    return (!requestedExchanges || requestedExchanges.has(market.exchange))
      && (!eligible || eligible.has(market.exchangeSymbol));
  });
  const stableAssets = new Set(report.quoteAssets.filter((asset) => asset.isStablecoin).map((asset) => asset.symbol));
  const assetRank = new Map(report.assets.map((asset) => [asset.symbol, asset.rank]));
  const candidates = report.eligiblePairs
    .filter((pair) => pair.status === 'ELIGIBLE' || pair.status === 'PARTIAL_DISCOVERY')
    .filter((pair) => !requested || requested.has(pair.canonicalPair))
    .filter((pair) => !requiredExchanges || [...requiredExchanges].every((exchange) => {
      const market = pair.markets.find((candidate) => candidate.exchange === exchange);
      const eligible = eligibleSymbols.get(exchange);
      return !!market && (!eligible || eligible.has(market.exchangeSymbol));
    }))
    .filter((pair) => !requestedExchanges || !stableAssets.has(pair.baseAsset))
    .filter((pair) => availableMarkets(pair).length > 0)
    .sort((a, b) => {
      if (!requestedExchanges) return a.canonicalPair.localeCompare(b.canonicalPair);
      const quoteRank = (quote: string) => quote === 'USDT' ? 0 : quote === 'USDC' ? 1 : 2;
      const assetOrder = (asset: string) => assetRank.get(asset) ?? Number.MAX_SAFE_INTEGER;
      return quoteRank(a.quoteAsset) - quoteRank(b.quoteAsset)
        || assetOrder(a.baseAsset) - assetOrder(b.baseAsset)
        || a.canonicalPair.localeCompare(b.canonicalPair);
    });
  if (requested) {
    const absent = [...requested].filter((pair) => !candidates.some((candidate) => candidate.canonicalPair === pair));
    if (absent.length) throw new Error(`Requested pairs absent from Phase A eligible-pair report: ${absent.join(', ')}`);
  }
  const maxPairs = options.maxPairs ?? 3;
  if (!Number.isInteger(maxPairs) || maxPairs < 1) throw new Error('MAX_LIVE_PAIRS must be a positive integer');
  const selected = candidates.slice(0, maxPairs);
  const subscriptions = selected.flatMap((pair) => availableMarkets(pair)
    .map((market) => subscription(pair.canonicalPair, market)))
    .sort((a, b) => a.exchange.localeCompare(b.exchange) || a.canonicalPair.localeCompare(b.canonicalPair) || a.exchangeSymbol.localeCompare(b.exchangeSymbol));
  return {
    sourceRunId: report.runId,
    sourceStatus: report.status,
    candidatePairCount: candidates.length,
    selectedPairCount: selected.length,
    subscriptions,
  };
}

export async function loadLiveUniverse(path: string, options: LiveUniverseOptions = {}): Promise<LiveUniverse> {
  const report = JSON.parse(await readFile(path, 'utf8')) as DiscoveryReport;
  return selectLiveUniverse(report, options);
}

function subscription(canonicalPair: string, market: NormalizedSpotMarket): LiveMarketSubscription {
  const [canonicalAsset, canonicalQuote] = canonicalPair.split('/');
  if (!canonicalAsset || !canonicalQuote || market.canonicalPair !== canonicalPair) throw new Error(`Invalid Phase A market mapping for ${canonicalPair}`);
  return { exchange: market.exchange, canonicalAsset, canonicalQuote, canonicalPair, exchangeSymbol: market.exchangeSymbol };
}
function normalizePair(pair: string): string {
  const normalized = pair.trim().toUpperCase().replace('-', '/').replace('_', '/');
  if (!/^[A-Z0-9.]+\/[A-Z0-9.]+$/.test(normalized)) throw new Error(`Invalid canonical pair: ${pair}`);
  return normalized;
}
