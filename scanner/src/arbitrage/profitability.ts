import type { ArbitrageOpportunity } from './detection';
import type { ArbitrageConfig } from './detection';
import type { OrderBookState } from '../market-data/types';

export type ProfitabilityLimits = {
  minimumEstimatedNetProfit: number;
  maximumExecutableNotional: number;
  maximumTotalSlippage: number;
};

export const DEFAULT_PROFITABILITY_LIMITS: ProfitabilityLimits = {
  minimumEstimatedNetProfit: 0.01,
  maximumExecutableNotional: 10_000,
  maximumTotalSlippage: 0.005,
};

export type ProfitabilityOpportunity = ArbitrageOpportunity & {
  referenceQuantity: number;
  bestBuyAsk: number;
  bestSellBid: number;
  buySlippage: number;
  sellSlippage: number;
  totalEstimatedSlippage: number;
  estimatedSlippageCost: number;
  buyQuoteCost: number;
  sellQuoteProceeds: number;
  grossProfit: number;
  buyFee: number;
  sellFee: number;
  totalFees: number;
  estimatedNetProfit: number;
  buyBookAgeMs: number;
  sellBookAgeMs: number;
  freshness: 'ACTIVE' | 'QUIET';
};

export type ProfitabilityResult = {
  opportunity?: ProfitabilityOpportunity;
  rejectionReason?: string;
};

const STABLECOINS = new Set(['USDT', 'USDC', 'BUSD', 'DAI', 'TUSD', 'FDUSD', 'USDP', 'USDE', 'UST', 'USTC', 'PYUSD', 'EURC']);

/** Reprices a Phase C route against both live books at one common executable quantity. */
export function estimateOpportunityProfitability(
  candidate: ArbitrageOpportunity,
  buyBook: OrderBookState,
  sellBook: OrderBookState,
  config: ArbitrageConfig,
  limits: ProfitabilityLimits = DEFAULT_PROFITABILITY_LIMITS,
  now = Date.now(),
): ProfitabilityResult {
  validateLimits(limits);
  const maxAge = config.maxBookAgeMs ?? 60_000;
  if (candidate.buyExchange === candidate.sellExchange || buyBook.exchange === sellBook.exchange) return reject('same_exchange');
  if (candidate.quote !== buyBook.canonicalQuote || candidate.quote !== sellBook.canonicalQuote
    || candidate.asset !== buyBook.canonicalAsset || candidate.asset !== sellBook.canonicalAsset
    || buyBook.canonicalPair !== candidate.pair || sellBook.canonicalPair !== candidate.pair) return reject('pair_or_quote_mismatch');
  if (STABLECOINS.has(candidate.asset.toUpperCase())) return reject('stablecoin_base_asset');
  if (buyBook.exchange !== candidate.buyExchange || sellBook.exchange !== candidate.sellExchange) return reject('route_book_mismatch');
  if (!isValidBook(buyBook, now, maxAge) || !isValidBook(sellBook, now, maxAge)) return reject('invalid_or_stale_book');
  if (!(candidate.executableQuantity > 0) || !Number.isFinite(candidate.executableQuantity)) return reject('invalid_reference_quantity');

  const depth = config.maxDepthLevels ?? 20;
  const initialBuy = walkBuy(buyBook.asks, candidate.executableQuantity, limits.maximumExecutableNotional, depth);
  if (!(initialBuy.quantity > 0)) return reject('insufficient_buy_depth');
  const initialSell = walkSell(sellBook.bids, initialBuy.quantity, depth);
  if (!(initialSell.quantity > 0)) return reject('insufficient_sell_depth');
  const quantity = Math.min(initialBuy.quantity, initialSell.quantity);
  const buy = walkBuy(buyBook.asks, quantity, limits.maximumExecutableNotional, depth);
  const sell = walkSell(sellBook.bids, quantity, depth);
  const commonQuantity = Math.min(buy.quantity, sell.quantity);
  if (!(commonQuantity > 0)) return reject('insufficient_common_depth');
  const finalBuy = walkBuy(buyBook.asks, commonQuantity, limits.maximumExecutableNotional, depth);
  const finalSell = walkSell(sellBook.bids, commonQuantity, depth);
  if (finalBuy.quantity + 1e-12 < commonQuantity || finalSell.quantity + 1e-12 < commonQuantity) return reject('insufficient_common_depth');

  const buyVWAP = finalBuy.quote / commonQuantity;
  const sellVWAP = finalSell.quote / commonQuantity;
  const buyFeeRate = config.feeRates[buyBook.exchange] ?? 0.001;
  const sellFeeRate = config.feeRates[sellBook.exchange] ?? 0.001;
  const buyFee = finalBuy.quote * buyFeeRate;
  const sellFee = finalSell.quote * sellFeeRate;
  const grossProfit = finalSell.quote - finalBuy.quote;
  const totalFees = buyFee + sellFee;
  const netProfit = grossProfit - totalFees;
  const netSpread = netProfit / finalBuy.quote;
  const buySlippage = Math.max(0, (buyVWAP - buyBook.asks[0].price) / buyBook.asks[0].price);
  const sellSlippage = Math.max(0, (sellBook.bids[0].price - sellVWAP) / sellBook.bids[0].price);
  const totalEstimatedSlippage = buySlippage + sellSlippage;
  const estimatedSlippageCost = Math.max(0, finalBuy.quote - commonQuantity * buyBook.asks[0].price)
    + Math.max(0, commonQuantity * sellBook.bids[0].price - finalSell.quote);
  const executableNotional = finalBuy.quote;
  if (executableNotional < (config.minimumExecutableNotional ?? 0)) return reject('below_minimum_notional');
  if (netSpread < (config.minimumNetSpread ?? 0)) return reject('below_minimum_net_spread');
  if (netProfit < limits.minimumEstimatedNetProfit) return reject('below_minimum_net_profit');
  if (executableNotional > limits.maximumExecutableNotional + 1e-9) return reject('above_maximum_notional');
  if (totalEstimatedSlippage > limits.maximumTotalSlippage) return reject('above_maximum_slippage');

  const buyBookAgeMs = now - buyBook.receivedTimestamp;
  const sellBookAgeMs = now - sellBook.receivedTimestamp;
  return { opportunity: {
    ...candidate,
    buyPrice: buyBook.asks[0].price,
    buyVWAP,
    sellPrice: sellBook.bids[0].price,
    sellVWAP,
    referenceQuantity: candidate.executableQuantity,
    executableQuantity: commonQuantity,
    executableNotional,
    grossSpread: grossProfit / finalBuy.quote,
    grossProfit,
    estimatedFees: totalFees,
    buyFee,
    sellFee,
    totalFees,
    netSpread,
    estimatedNetProfit: netProfit,
    bestBuyAsk: buyBook.asks[0].price,
    bestSellBid: sellBook.bids[0].price,
    buySlippage,
    sellSlippage,
    totalEstimatedSlippage,
    estimatedSlippageCost,
    buyQuoteCost: finalBuy.quote,
    sellQuoteProceeds: finalSell.quote,
    buyBookAgeMs,
    sellBookAgeMs,
    freshness: buyBook.status === 'QUIET' || sellBook.status === 'QUIET' ? 'QUIET' : 'ACTIVE',
    timestamp: now,
  } };
}

export function profitabilityLimitsFromEnv(env: NodeJS.ProcessEnv): ProfitabilityLimits {
  return {
    minimumEstimatedNetProfit: readNumber(env.ARBITRAGE_MIN_NET_PROFIT, DEFAULT_PROFITABILITY_LIMITS.minimumEstimatedNetProfit, 'ARBITRAGE_MIN_NET_PROFIT'),
    maximumExecutableNotional: readNumber(env.ARBITRAGE_MAX_NOTIONAL, DEFAULT_PROFITABILITY_LIMITS.maximumExecutableNotional, 'ARBITRAGE_MAX_NOTIONAL'),
    maximumTotalSlippage: readNumber(env.ARBITRAGE_MAX_SLIPPAGE, DEFAULT_PROFITABILITY_LIMITS.maximumTotalSlippage, 'ARBITRAGE_MAX_SLIPPAGE'),
  };
}

function walkBuy(levels: OrderBookState['asks'], target: number, maxNotional: number, maxDepth: number) {
  let quantity = 0, quote = 0;
  for (const level of levels.slice(0, maxDepth)) {
    const byQuantity = Math.min(level.quantity, target - quantity);
    const take = Math.min(byQuantity, Math.max(0, (maxNotional - quote) / level.price));
    if (take > 0) { quantity += take; quote += take * level.price; }
    if (quantity + 1e-12 >= target || take + 1e-12 < byQuantity) break;
  }
  return { quantity, quote };
}

function walkSell(levels: OrderBookState['bids'], target: number, maxDepth: number) {
  let quantity = 0, quote = 0;
  for (const level of levels.slice(0, maxDepth)) {
    const take = Math.min(level.quantity, target - quantity);
    if (take > 0) { quantity += take; quote += take * level.price; }
    if (quantity + 1e-12 >= target) break;
  }
  return { quantity, quote };
}

function isValidBook(book: OrderBookState, now: number, maxAge: number): boolean {
  const age = now - book.receivedTimestamp;
  if (!book.synchronized || book.stale || !book.sequenceValid || book.feedHealth !== 'HEALTHY'
    || (book.status !== 'SYNCHRONIZED' && book.status !== 'QUIET') || !Number.isFinite(age) || age < 0 || age > maxAge
    || !Number.isFinite(book.exchangeTimestamp) || !Number.isFinite(book.processedTimestamp)
    || !book.bids.length || !book.asks.length) return false;
  const valid = (level: { price: number; quantity: number }) => Number.isFinite(level.price) && level.price > 0 && Number.isFinite(level.quantity) && level.quantity > 0;
  if (!book.bids.every(valid) || !book.asks.every(valid) || book.bids[0].price >= book.asks[0].price) return false;
  for (let i = 1; i < book.bids.length; i++) if (book.bids[i - 1].price < book.bids[i].price) return false;
  for (let i = 1; i < book.asks.length; i++) if (book.asks[i - 1].price > book.asks[i].price) return false;
  return book.bestBid === book.bids[0].price && book.bestAsk === book.asks[0].price
    && book.bestBidQuantity === book.bids[0].quantity && book.bestAskQuantity === book.asks[0].quantity;
}

function validateLimits(limits: ProfitabilityLimits): void {
  if (!Number.isFinite(limits.minimumEstimatedNetProfit) || limits.minimumEstimatedNetProfit < 0
    || !Number.isFinite(limits.maximumExecutableNotional) || limits.maximumExecutableNotional <= 0
    || !Number.isFinite(limits.maximumTotalSlippage) || limits.maximumTotalSlippage < 0 || limits.maximumTotalSlippage > 2) {
    throw new Error('Invalid estimated-profitability limits');
  }
}

function readNumber(raw: string | undefined, fallback: number, name: string): number {
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) throw new Error(`${name} must be a finite non-negative number`);
  return value;
}

function reject(rejectionReason: string): ProfitabilityResult { return { rejectionReason }; }
