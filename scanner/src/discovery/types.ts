export type ExchangeId = 'binance' | 'bybit' | 'okx' | 'gate' | 'mexc' | 'bitget' | 'kucoin' | 'htx' | 'crypto.com' | 'coinbase';
export type DiscoveryStatus = 'AVAILABLE' | 'UNAVAILABLE' | 'RATE_LIMITED' | 'ERROR' | 'EMPTY_RESULT';
export type SourceHealth = { status: DiscoveryStatus; endpoint: string; error?: string; httpStatus?: number };
export type NormalizedSpotMarket = {
  exchange: ExchangeId;
  exchangeSymbol: string;
  baseAsset: string;
  quoteAsset: string;
  canonicalAssetId: string;
  canonicalPair: string;
  marketType: 'spot';
  status: string;
  pricePrecision?: number;
  quantityPrecision?: number;
  tickSize?: number;
  lotSize?: number;
  minimumQuantity?: number;
  minimumNotional?: number;
  volumeBase24h?: number;
  volumeQuote24h?: number;
  turnover24h?: number;
  lastPrice?: number;
  exchangeTimestamp?: number;
  assetMapping: 'exact-symbol' | 'known-alias' | 'ambiguous';
};
export type ExchangeDiscoveryResult = {
  exchange: ExchangeId;
  status: DiscoveryStatus;
  discoveredAt: number;
  marketCount: number;
  markets: NormalizedSpotMarket[];
  marketSource: SourceHealth;
  tickerSource?: SourceHealth;
  warnings: string[];
  durationMs: number;
};
export type DiscoveryConfig = {
  target: number;
  minExchangeCount: number;
  supportedExchanges: ExchangeId[];
  majorQuoteAssets: string[];
  /** Quotes retained by the selected-token market view; empty means no quote restriction. */
  supportedMarketQuotes?: string[];
  /** Deterministic cap for the ranked selected-token market view; zero means uncapped. */
  maxSelectedTokenMarkets?: number;
  /** Optional explicit canonical symbols/asset IDs for a reproducible F2 market report. */
  selectedTokenSymbols?: string[];
  /** Default number of ranked Phase A assets used by the separate F2 market view. */
  selectedTokenLimit?: number;
  meaningfulVolumeMinimum: number;
  scoreWeights: { coverage: number; liquidity: number; liquidExchanges: number; quoteAvailability: number; activity: number };
};
export type RankedAsset = {
  rank: number;
  assetId: string;
  symbol: string;
  name?: string;
  isStablecoin: false;
  isQuoteAsset: boolean;
  exchangeCount: number;
  exchanges: ExchangeId[];
  eligiblePairCount: number;
  liquidExchangeCount: number;
  totalVolume24h: number | null;
  totalVolume24hByQuoteAsset: Record<string, number>;
  medianVolume24h: number | null;
  liquidityScore: number | null;
  arbitrageRelevanceScore: number;
  scoreComponents: Record<string, number | null>;
  scoreMissingComponents: string[];
  status: 'ELIGIBLE' | 'PARTIAL_DISCOVERY';
  selectionReason: string;
};
export type EligiblePair = {
  canonicalPair: string;
  baseAsset: string;
  quoteAsset: string;
  exchangeCount: number;
  exchanges: ExchangeId[];
  markets: NormalizedSpotMarket[];
  status: 'ELIGIBLE' | 'PARTIAL_DISCOVERY';
};
/** A real canonical spot market touching a selected asset; never a generated pair. */
export type SelectedTokenMarket = {
  canonicalPair: string;
  baseAsset: string;
  quoteAsset: string;
  selectedAssetIds: string[];
  /** Number of selected assets appearing on either side of this exact market. */
  selectedTokenRelevance: 0 | 1 | 2;
  exchangeCount: number;
  exchanges: ExchangeId[];
  totalQuoteVolume24hByExchange: Partial<Record<ExchangeId, number>>;
  totalQuoteVolume24h: number | null;
  /** Mean venue-local volume percentile; avoids comparing raw BTC/ETH/stablecoin units. */
  liquidityScore: number | null;
  quotePriority: number;
  activityScore: number;
  rank: number;
  markets: NormalizedSpotMarket[];
};
export type MarketPruningTier = {
  tier: 'A' | 'B' | 'C';
  description: string;
  markets: SelectedTokenMarket[];
  marketCount: number;
  exchangeBookCount: number;
  exchangesByMarket: Record<string, ExchangeId[]>;
  quoteDistribution: Record<string, number>;
  possibleExactCrossExchangeComparisons: number;
};
export type DiscoveryReport = {
  /** Additive F2 fields preserve v1 compatibility for the existing Phase B loader. */
  schemaVersion: 1;
  runId: string;
  startedAt: number;
  completedAt: number;
  status: 'COMPLETE' | 'PARTIAL' | 'FAILED';
  config: DiscoveryConfig;
  exchanges: ExchangeDiscoveryResult[];
  marketsDiscovered: number;
  uniqueCanonicalAssetsDiscovered: number;
  nonStableAssetsEligible: number;
  selectedAssetCount: number;
  quoteAssets: Array<{ symbol: string; isStablecoin: boolean; isMajorQuote: boolean }>;
  assets: RankedAsset[];
  eligiblePairs: EligiblePair[];
  pairCount: number;
  /** Additive F2 selected-token view; existing v1 fields retain their original semantics. */
  selectedTokenIds?: string[];
  eligibleMarketCount?: number;
  selectedTokenMarkets?: SelectedTokenMarket[];
  selectedTokenMarketCount?: number;
  projectedExchangeBookCount?: number;
  exactCrossExchangeMarketMatchCount?: number;
  marketPruningTiers?: MarketPruningTier[];
  coverageComplete: boolean;
  methodology: {
    scoring: string;
    missingData: string;
    volumeAggregation: string;
    stablecoinClassification: string;
    marketLiquidityDisclaimer: string;
  };
};
