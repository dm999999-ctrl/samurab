import { createDiscoveryAdapters, type SpotDiscoveryAdapter } from './adapters';
import { buildDiscoveryReport } from './universe';
import type { DiscoveryConfig, DiscoveryReport, ExchangeDiscoveryResult, ExchangeId } from './types';

export const SUPPORTED_EXCHANGES: ExchangeId[] = ['binance','bybit','okx','gate','mexc','bitget','kucoin','htx','crypto.com','coinbase'];
const defaultWeights = { coverage: 35, liquidity: 35, liquidExchanges: 15, quoteAvailability: 10, activity: 5 };
function positiveInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : fallback;
}
function weight(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}
export function discoveryConfig(env: Record<string, string | undefined> = process.env): DiscoveryConfig {
  return {
    target: positiveInteger(env.MASTER_UNIVERSE_TARGET, 1000),
    minExchangeCount: Math.max(1, positiveInteger(env.MIN_EXCHANGE_COUNT, 2)),
    supportedExchanges: [...SUPPORTED_EXCHANGES],
    majorQuoteAssets: (env.DISCOVERY_MAJOR_QUOTES ?? 'USDT,USDC,USD,FDUSD,DAI').split(',').map((item) => item.trim().toUpperCase()).filter(Boolean),
    meaningfulVolumeMinimum: weight(env.DISCOVERY_MIN_QUOTE_VOLUME, 0),
    scoreWeights: {
      coverage: weight(env.SCORE_WEIGHT_COVERAGE, defaultWeights.coverage),
      liquidity: weight(env.SCORE_WEIGHT_LIQUIDITY, defaultWeights.liquidity),
      liquidExchanges: weight(env.SCORE_WEIGHT_LIQUID_EXCHANGES, defaultWeights.liquidExchanges),
      quoteAvailability: weight(env.SCORE_WEIGHT_QUOTES, defaultWeights.quoteAvailability),
      activity: weight(env.SCORE_WEIGHT_ACTIVITY, defaultWeights.activity),
    },
  };
}
export async function runDiscovery(options: {
  adapters?: SpotDiscoveryAdapter[];
  config?: DiscoveryConfig;
  now?: () => number;
  runId?: string;
} = {}): Promise<DiscoveryReport> {
  const now = options.now ?? Date.now;
  const startedAt = now();
  const adapters = options.adapters ?? createDiscoveryAdapters();
  const config = options.config ?? discoveryConfig();
  const settled = await Promise.all(adapters.map(async (adapter): Promise<ExchangeDiscoveryResult> => {
    try { return await adapter.discover(); }
    catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return {
        exchange: adapter.exchange, status: 'ERROR', discoveredAt: now(), marketCount: 0, markets: [],
        marketSource: { status: 'ERROR', endpoint: 'adapter', error: message }, warnings: [message], durationMs: 0,
      };
    }
  }));
  const completedAt = now();
  return buildDiscoveryReport(settled, config, startedAt, completedAt, options.runId ?? `discovery-${completedAt}`);
}
