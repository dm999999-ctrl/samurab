import { canonicalizeMarket, isLeveragedOrSynthetic, isStablecoin, classifyAsset, MAJOR_QUOTE_ASSETS } from './symbol-map';
import type { DiscoveryConfig, DiscoveryReport, ExchangeDiscoveryResult, EligiblePair, NormalizedSpotMarket, RankedAsset, SelectedTokenMarket, MarketPruningTier } from './types';

export function buildDiscoveryReport(
  exchanges: ExchangeDiscoveryResult[],
  config: DiscoveryConfig,
  startedAt: number,
  completedAt: number,
  runId: string,
): DiscoveryReport {
  const coverageComplete = exchanges.every((result) => result.status === 'AVAILABLE' || result.status === 'EMPTY_RESULT');
  const successful = exchanges.flatMap((result) => result.markets);
  const markets = successful
    .filter((market) => !isLeveragedOrSynthetic(market.baseAsset))
    .map((market) => canonicalizeMarket(market))
    .sort((a, b) => a.exchange.localeCompare(b.exchange) || a.exchangeSymbol.localeCompare(b.exchangeSymbol));

  const quoteSymbols = [...new Set(markets.map((market) => market.quoteAsset))].sort();
  const quotes = quoteSymbols.map((symbol) => ({
    symbol,
    isStablecoin: isStablecoin(symbol),
    isMajorQuote: (config.majorQuoteAssets.length ? config.majorQuoteAssets : [...MAJOR_QUOTE_ASSETS]).includes(symbol),
  }));

  const pairsById = groupBy(markets, (market) => market.canonicalPair);
  const eligiblePairs: EligiblePair[] = [];
  for (const [canonicalPair, listings] of pairsById) {
    const exchangesForPair = [...new Set(listings.map((market) => market.exchange))].sort();
    if (exchangesForPair.length < config.minExchangeCount) continue;
    const [baseAsset, quoteAsset] = canonicalPair.split('/');
    eligiblePairs.push({
      canonicalPair, baseAsset, quoteAsset, exchangeCount: exchangesForPair.length,
      exchanges: exchangesForPair, markets: listings,
      status: coverageComplete ? 'ELIGIBLE' : 'PARTIAL_DISCOVERY',
    });
  }
  eligiblePairs.sort((a, b) => a.canonicalPair.localeCompare(b.canonicalPair));

  const pairCounts = new Map<string, number>();
  for (const pair of eligiblePairs) {
    const base = classifyAsset(pair.baseAsset);
    pairCounts.set(base.assetId, (pairCounts.get(base.assetId) ?? 0) + 1);
  }
  const assetGroups = groupBy(markets, (market) => market.canonicalAssetId);
  const volumePercentiles = computeVolumePercentiles(markets);
  const candidates: RankedAsset[] = [];
  for (const [assetId, listings] of assetGroups) {
    const first = listings[0];
    const canonicalSymbol = assetId.startsWith("asset:") ? assetId.slice(6) : first.baseAsset;
    if (isStablecoin(canonicalSymbol) || first.assetMapping === 'ambiguous') continue;
    const exchangesForAsset = [...new Set(listings.map((market) => market.exchange))].sort();
    if (exchangesForAsset.length < config.minExchangeCount) continue;
    const volumeByQuote: Record<string, number> = {};
    const quoteValues = new Map<string, number[]>();
    for (const market of listings) {
      if (market.volumeQuote24h === undefined || market.volumeQuote24h <= 0) continue;
      volumeByQuote[market.quoteAsset] = (volumeByQuote[market.quoteAsset] ?? 0) + market.volumeQuote24h;
      const values = quoteValues.get(market.quoteAsset) ?? [];
      values.push(market.volumeQuote24h);
      quoteValues.set(market.quoteAsset, values);
    }
    const oneQuote = Object.keys(volumeByQuote).length === 1 && listings.every((market) => market.volumeQuote24h !== undefined);
    const totalVolume = oneQuote ? Object.values(volumeByQuote)[0] : null;
    const median = oneQuote ? medianOf(listings.map((market) => market.volumeQuote24h!)) : null;
    const liquidExchanges = new Set(listings.filter((market) => (market.volumeQuote24h ?? 0) > config.meaningfulVolumeMinimum).map((market) => market.exchange));
    const majorQuoteCoverage = new Set(listings.filter((market) => config.majorQuoteAssets.includes(market.quoteAsset)).map((market) => market.quoteAsset)).size;
    const scores: Record<string, number | null> = {
      coverage: exchangesForAsset.length / config.supportedExchanges.length,
      liquidity: mean(listings.map((market) => volumePercentiles.get(marketKey(market))).filter((value): value is number => value !== undefined)),
      liquidExchanges: liquidExchanges.size / config.supportedExchanges.length,
      quoteAvailability: config.majorQuoteAssets.length ? majorQuoteCoverage / config.majorQuoteAssets.length : null,
      activity: listings.filter((market) => (market.lastPrice ?? 0) > 0 || (market.volumeBase24h ?? 0) > 0 || (market.volumeQuote24h ?? 0) > 0).length / listings.length,
    };
    const weighted: Array<[keyof DiscoveryConfig['scoreWeights'], number | null]> = [
      ['coverage', scores.coverage], ['liquidity', scores.liquidity],
      ['liquidExchanges', scores.liquidExchanges], ['quoteAvailability', scores.quoteAvailability], ['activity', scores.activity],
    ];
    const available = weighted.filter(([, value]) => value !== null) as Array<[keyof DiscoveryConfig['scoreWeights'], number]>;
    const totalWeight = available.reduce((sum, [key]) => sum + config.scoreWeights[key], 0);
    const score = totalWeight > 0 ? 100 * available.reduce((sum, [key, value]) => sum + value * config.scoreWeights[key], 0) / totalWeight : 0;
    const missing = weighted.filter(([, value]) => value === null).map(([key]) => key);
    const eligiblePairCount = pairCounts.get(assetId) ?? 0;
    candidates.push({
      rank: 0, assetId, symbol: canonicalSymbol, isStablecoin: false,
      isQuoteAsset: quoteSymbols.includes(canonicalSymbol),
      exchangeCount: exchangesForAsset.length, exchanges: exchangesForAsset,
      eligiblePairCount, liquidExchangeCount: liquidExchanges.size,
      totalVolume24h: totalVolume,
      totalVolume24hByQuoteAsset: Object.fromEntries(Object.entries(volumeByQuote).sort(([a], [b]) => a.localeCompare(b))),
      medianVolume24h: median,
      liquidityScore: scores.liquidity === null ? null : scores.liquidity * 100,
      arbitrageRelevanceScore: score,
      scoreComponents: Object.fromEntries(Object.entries(scores).map(([key, value]) => [key, value === null ? null : value * 100])),
      scoreMissingComponents: missing,
      status: coverageComplete ? 'ELIGIBLE' : 'PARTIAL_DISCOVERY',
      selectionReason: `${exchangesForAsset.length} known exchanges; ${eligiblePairCount} eligible pairs; score uses configured weighted coverage, within-exchange/quote volume percentile, liquid venue count, major-quote coverage, and activity.`,
    });
  }

  candidates.sort((a, b) => b.arbitrageRelevanceScore - a.arbitrageRelevanceScore
    || b.exchangeCount - a.exchangeCount
    || b.eligiblePairCount - a.eligiblePairCount
    || (b.liquidityScore ?? -1) - (a.liquidityScore ?? -1)
    || a.symbol.localeCompare(b.symbol)
    || a.assetId.localeCompare(b.assetId));
  const assets = candidates.slice(0, Math.max(0, config.target)).map((asset, index) => ({ ...asset, rank: index + 1 }));
  const explicitSelection = config.selectedTokenSymbols?.map((value) => value.trim().toUpperCase().replace(/^ASSET:/, '')).filter(Boolean);
  const selectedAssets = explicitSelection?.length
    ? explicitSelection.map((symbol) => classifyAsset(symbol)).filter((asset) => asset.mapping !== 'ambiguous')
    : assets.slice(0, config.selectedTokenLimit ?? 25).map((asset) => ({ assetId: asset.assetId, symbol: asset.symbol, mapping: 'exact-symbol' as const }));
  const selectedById = new Map(selectedAssets.map((asset) => [asset.assetId, asset.symbol] as const));
  const quotePriority = new Map((config.supportedMarketQuotes ?? config.majorQuoteAssets).map((quote, index) => [quote, index]));
  const selectedTokenGroups = groupBy(markets.filter((market) => selectedTokenRelevance(market, selectedById) > 0
    && market.assetMapping !== 'ambiguous'
    && (!config.supportedMarketQuotes?.length || quotePriority.has(market.quoteAsset))), (market) => market.canonicalPair);
  const rankedSelectedMarkets = [...selectedTokenGroups.entries()].map(([canonicalPair, listings]) => {
    const [baseAsset, quoteAsset] = canonicalPair.split('/');
    const exchangesForMarket = [...new Set(listings.map((market) => market.exchange))].sort();
    const volumeByExchange: Partial<Record<NormalizedSpotMarket['exchange'], number>> = {};
    for (const market of listings) {
      if (market.volumeQuote24h !== undefined && market.volumeQuote24h > 0) {
        volumeByExchange[market.exchange] = (volumeByExchange[market.exchange] ?? 0) + market.volumeQuote24h;
      }
    }
    const totalQuoteVolume = listings.every((market) => market.volumeQuote24h !== undefined)
      ? listings.reduce((sum, market) => sum + (market.volumeQuote24h ?? 0), 0) : null;
    const activityScore = listings.filter((market) => (market.lastPrice ?? 0) > 0 || (market.volumeBase24h ?? 0) > 0 || (market.volumeQuote24h ?? 0) > 0).length / listings.length;
    const relevance = Math.max(...listings.map((market) => selectedTokenRelevance(market, selectedById))) as 1 | 2;
    const selectedAssetIds = [classifyAsset(baseAsset), classifyAsset(quoteAsset)]
      .filter((asset) => selectedById.has(asset.assetId)).map((asset) => asset.assetId).sort();
    const liquidityPercentiles = listings.map((market) => volumePercentiles.get(marketKey(market))).filter((value): value is number => value !== undefined);
    const liquidityScore = liquidityPercentiles.length ? mean(liquidityPercentiles) : null;
    return {
      canonicalPair, baseAsset, quoteAsset, selectedAssetIds, selectedTokenRelevance: relevance,
      exchangeCount: exchangesForMarket.length, exchanges: exchangesForMarket, totalQuoteVolume24hByExchange: volumeByExchange,
      totalQuoteVolume24h: totalQuoteVolume, liquidityScore, quotePriority: quotePriority.get(quoteAsset) ?? Number.MAX_SAFE_INTEGER,
      activityScore, rank: 0, markets: listings,
    } satisfies Omit<SelectedTokenMarket, 'rank'> & { rank: number };
  }).sort((a, b) => Number(b.exchangeCount >= 2) - Number(a.exchangeCount >= 2)
    || b.selectedTokenRelevance - a.selectedTokenRelevance
    || b.exchangeCount - a.exchangeCount
    || (b.liquidityScore ?? -1) - (a.liquidityScore ?? -1)
    || b.activityScore - a.activityScore
    || a.quotePriority - b.quotePriority
    || a.canonicalPair.localeCompare(b.canonicalPair));
  const selectedTokenMarketLimit = config.maxSelectedTokenMarkets ?? 0;
  const selectedTokenMarkets: SelectedTokenMarket[] = rankedSelectedMarkets
    .slice(0, selectedTokenMarketLimit > 0 ? selectedTokenMarketLimit : undefined)
    .map((market, index) => ({ ...market, rank: index + 1 }));
  const subscriptionsProjected = selectedTokenMarkets.reduce((sum, market) => sum + market.markets.length, 0);
  const routeMatches = selectedTokenMarkets.filter((market) => market.exchangeCount >= 2).length;
  const marketPruningTiers = buildMarketPruningTiers(selectedTokenMarkets);
  const failure = exchanges.some((result) => !['AVAILABLE','EMPTY_RESULT'].includes(result.status));
  return {
    schemaVersion: 1, runId, startedAt, completedAt,
    status: failure ? (successful.length ? 'PARTIAL' : 'FAILED') : 'COMPLETE',
    config, exchanges, marketsDiscovered: successful.length,
    uniqueCanonicalAssetsDiscovered: new Set(markets.map((market) => market.canonicalAssetId)).size,
    nonStableAssetsEligible: candidates.length, selectedAssetCount: assets.length,
    quoteAssets: quotes, assets, eligiblePairs,
    pairCount: eligiblePairs.length, coverageComplete,
    selectedTokenIds: [...selectedById.keys()],
    eligibleMarketCount: rankedSelectedMarkets.length,
    selectedTokenMarkets,
    selectedTokenMarketCount: selectedTokenMarkets.length,
    projectedExchangeBookCount: subscriptionsProjected,
    exactCrossExchangeMarketMatchCount: routeMatches,
    marketPruningTiers,
    methodology: {
      scoring: 'Score is a 0–100 weighted mean. Default weights: coverage 35%, exchange-native liquidity percentile 35%, liquid exchange count 15%, major quote availability 10%, activity 5%. Configured weights are renormalized only across available components.',
      missingData: 'Missing exchange fields remain omitted. Missing volume is excluded from liquidity ranking; unavailable components are omitted and weights renormalized, and discovery failures are reported rather than interpreted as zero markets.',
      volumeAggregation: 'Quote volumes are ranked only within the same exchange and quote currency. Totals are summed by quote currency; totalVolume24h and medianVolume24h are null unless all available listings use one quote currency with reported volume. No cross-currency conversion is assumed.',
      stablecoinClassification: 'Known stablecoin/fiat quote symbols are classified separately; all discovered quote currencies are retained. Stablecoin base assets are excluded from ranked assets.',
      marketLiquidityDisclaimer: '24-hour exchange-reported volume is a discovery-ranking signal, not executable order-book liquidity or a guarantee of arbitrage execution.',
    },
  };
}

function selectedTokenRelevance(market: NormalizedSpotMarket, selected: Map<string, string>): 0 | 1 | 2 {
  const base = selected.has(market.canonicalAssetId);
  const quote = selected.has(classifyAsset(market.quoteAsset).assetId);
  return Number(base) + Number(quote) as 0 | 1 | 2;
}

/** Turns ranked real listings into cumulative, deterministic arbitrage-oriented tiers. */
export function buildMarketPruningTiers(markets: SelectedTokenMarket[]): MarketPruningTier[] {
  const crossExchange = markets.filter((market) => market.exchangeCount >= 2);
  // Tier A is the compact high-confidence core; B broadens depth/venue coverage.
  const tierA = crossExchange.slice(0, Math.min(20, crossExchange.length));
  const tierB = crossExchange.slice(0, Math.min(40, crossExchange.length));
  return [
    makeTier('A', 'Top 20 ranked exact markets with at least two exchange listings; compact initial live candidate.', tierA),
    makeTier('B', 'Top 40 ranked exact markets with at least two exchange listings; broader candidate.', tierB),
    makeTier('C', 'All discovered catalog markets, including single-exchange listings; research/catalog only, not all cross-exchange eligible.', markets),
  ];
}

function makeTier(tier: MarketPruningTier['tier'], description: string, markets: SelectedTokenMarket[]): MarketPruningTier {
  const quoteDistribution: Record<string, number> = {};
  const exchangesByMarket: Record<string, NormalizedSpotMarket['exchange'][]> = {};
  let books = 0, comparisons = 0;
  for (const market of markets) {
    quoteDistribution[market.quoteAsset] = (quoteDistribution[market.quoteAsset] ?? 0) + 1;
    exchangesByMarket[market.canonicalPair] = [...market.exchanges];
    books += market.markets.length;
    comparisons += market.exchangeCount * (market.exchangeCount - 1) / 2;
  }
  return { tier, description, markets, marketCount: markets.length, exchangeBookCount: books,
    exchangesByMarket, quoteDistribution, possibleExactCrossExchangeComparisons: comparisons };
}

function groupBy<T>(values: T[], keyFn: (value: T) => string): Map<string, T[]> {
  const groups = new Map<string, T[]>();
  for (const value of values) {
    const key = keyFn(value);
    const group = groups.get(key) ?? [];
    group.push(value);
    groups.set(key, group);
  }
  return groups;
}
function marketKey(market: NormalizedSpotMarket) { return `${market.exchange}:${market.exchangeSymbol.toUpperCase()}`; }
function computeVolumePercentiles(markets: NormalizedSpotMarket[]): Map<string, number> {
  const buckets = groupBy(markets.filter((market) => market.volumeQuote24h !== undefined && market.volumeQuote24h > 0), (market) => `${market.exchange}:${market.quoteAsset}`);
  const scores = new Map<string, number>();
  for (const group of buckets.values()) {
    const sorted = [...group].sort((a, b) => b.volumeQuote24h! - a.volumeQuote24h! || marketKey(a).localeCompare(marketKey(b)));
    let index = 0;
    while (index < sorted.length) {
      let end = index + 1;
      while (end < sorted.length && sorted[end].volumeQuote24h === sorted[index].volumeQuote24h) end += 1;
      const averageRank = (index + end - 1) / 2;
      const percentile = sorted.length <= 1 ? 1 : 1 - averageRank / (sorted.length - 1);
      for (let cursor = index; cursor < end; cursor += 1) scores.set(marketKey(sorted[cursor]), percentile);
      index = end;
    }
  }
  return scores;
}
function mean(values: number[]): number | null { return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null; }
function medianOf(values: number[]): number | null {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b), middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}
