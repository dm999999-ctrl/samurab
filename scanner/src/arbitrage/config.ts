import type { ExchangeId } from '../discovery/types';
import type { ArbitrageConfig } from './detection';

const EXCHANGES: ExchangeId[] = ['binance', 'bybit', 'okx', 'kucoin', 'gate', 'bitget', 'htx', 'crypto.com', 'coinbase', 'mexc'];
export const DEFAULT_ARBITRAGE_CONFIG: ArbitrageConfig = {
  feeRates: Object.fromEntries(EXCHANGES.map((exchange) => [exchange, 0.001])) as Partial<Record<ExchangeId, number>>,
  maxBookAgeMs: 60_000,
  minimumNetSpread: 0,
  minimumExecutableNotional: 10,
  maxDepthLevels: 20,
};

export function arbitrageConfigFromEnv(env: NodeJS.ProcessEnv): ArbitrageConfig {
  const feeRates = { ...DEFAULT_ARBITRAGE_CONFIG.feeRates };
  for (const exchange of EXCHANGES) {
    const name = `ARBITRAGE_FEE_${exchange.toUpperCase().replace(/[^A-Z0-9]/g, '_')}`;
    if (env[name] !== undefined) feeRates[exchange] = boundedNumber(env[name], 0, 0.999_999, name);
  }
  return {
    feeRates,
    maxBookAgeMs: boundedNumber(env.ARBITRAGE_MAX_BOOK_AGE_MS, 1, Number.MAX_SAFE_INTEGER, 'ARBITRAGE_MAX_BOOK_AGE_MS', 60_000),
    minimumNetSpread: boundedNumber(env.ARBITRAGE_MIN_NET_SPREAD, 0, Number.MAX_SAFE_INTEGER, 'ARBITRAGE_MIN_NET_SPREAD', 0),
    minimumExecutableNotional: boundedNumber(env.ARBITRAGE_MIN_NOTIONAL, 0, Number.MAX_SAFE_INTEGER, 'ARBITRAGE_MIN_NOTIONAL', 10),
    maxDepthLevels: positiveInteger(env.ARBITRAGE_DEPTH_LEVELS ?? '20', 'ARBITRAGE_DEPTH_LEVELS'),
    score: {
      weights: {
        netSpread: boundedNumber(env.ARBITRAGE_SCORE_WEIGHT_SPREAD, 0, Number.MAX_SAFE_INTEGER, 'ARBITRAGE_SCORE_WEIGHT_SPREAD', 0.4),
        executableNotional: boundedNumber(env.ARBITRAGE_SCORE_WEIGHT_NOTIONAL, 0, Number.MAX_SAFE_INTEGER, 'ARBITRAGE_SCORE_WEIGHT_NOTIONAL', 0.2),
        estimatedNetProfit: boundedNumber(env.ARBITRAGE_SCORE_WEIGHT_PROFIT, 0, Number.MAX_SAFE_INTEGER, 'ARBITRAGE_SCORE_WEIGHT_PROFIT', 0.3),
        bookQuality: boundedNumber(env.ARBITRAGE_SCORE_WEIGHT_QUALITY, 0, Number.MAX_SAFE_INTEGER, 'ARBITRAGE_SCORE_WEIGHT_QUALITY', 0.1),
      },
      notionalScale: boundedNumber(env.ARBITRAGE_SCORE_NOTIONAL_SCALE, 0.000_001, Number.MAX_SAFE_INTEGER, 'ARBITRAGE_SCORE_NOTIONAL_SCALE', 10_000),
      profitScale: boundedNumber(env.ARBITRAGE_SCORE_PROFIT_SCALE, 0.000_001, Number.MAX_SAFE_INTEGER, 'ARBITRAGE_SCORE_PROFIT_SCALE', 10),
    },
  };
}
function boundedNumber(raw: string | undefined, min: number, max: number, name: string, fallback?: number): number {
  if (raw === undefined && fallback !== undefined) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < min || value > max) throw new Error(`${name} must be between ${min} and ${max}`);
  return value;
}

function positiveInteger(value: string, name: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 65_535) throw new Error(`${name} must be an integer from 1 to 65535`);
  return parsed;
}
