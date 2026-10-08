import type { ExchangeId } from '../discovery/types';
import type { OrderBookState } from '../market-data/types';

export type ArbitrageOpportunity = {
  pair: string;
  asset: string;
  quote: string;
  buyExchange: ExchangeId;
  sellExchange: ExchangeId;
  buyPrice: number;
  buyVWAP: number;
  sellPrice: number;
  sellVWAP: number;
  executableQuantity: number;
  executableNotional: number;
  grossSpread: number;
  estimatedFees: number;
  netSpread: number;
  estimatedNetProfit: number;
  score: number;
  timestamp: number;
};

export type ArbitrageConfig = {
  feeRates: Partial<Record<ExchangeId, number>>;
  maxBookAgeMs?: number;
  minimumNetSpread?: number;
  minimumExecutableNotional?: number;
  maxDepthLevels?: number;
  score?: {
    weights?: { netSpread?: number; executableNotional?: number; estimatedNetProfit?: number; bookQuality?: number };
    notionalScale?: number;
    profitScale?: number;
  };
};

const DEFAULT_FEE = 0.001;
const STABLECOINS = new Set(['USDT', 'USDC', 'BUSD', 'DAI', 'TUSD', 'FDUSD', 'USDP', 'USDE', 'UST', 'USTC', 'PYUSD', 'EURC']);

/** Returns the single best executable route for each exact canonical spot pair. */
export function detectArbitrageOpportunities(books: OrderBookState[], config: ArbitrageConfig, now = Date.now()): ArbitrageOpportunity[] {
  const maxAge = config.maxBookAgeMs ?? 60_000;
  const maxDepth = config.maxDepthLevels ?? 20;
  const minimumSpread = config.minimumNetSpread ?? 0;
  const minimumNotional = config.minimumExecutableNotional ?? 0;
  if (!Number.isFinite(maxAge) || maxAge <= 0 || !Number.isInteger(maxDepth) || maxDepth < 1
    || !Number.isFinite(minimumSpread) || minimumSpread < 0
    || !Number.isFinite(minimumNotional) || minimumNotional < 0) throw new Error('Invalid arbitrage detector limits');
  for (const rate of Object.values(config.feeRates)) {
    if (rate !== undefined && (!Number.isFinite(rate) || rate < 0 || rate >= 1)) throw new Error('Fee rates must be finite values in [0, 1)');
  }

  const groups = new Map<string, OrderBookState[]>();
  for (const book of books) {
    if (book.canonicalAsset === book.canonicalQuote || STABLECOINS.has(book.canonicalAsset.toUpperCase())) continue;
    if (!isValidBook(book, now, maxAge)) continue;
    const group = groups.get(book.canonicalPair) ?? [];
    group.push(book);
    groups.set(book.canonicalPair, group);
  }

  const best: ArbitrageOpportunity[] = [];
  for (const group of groups.values()) {
    let selected: ArbitrageOpportunity | undefined;
    for (const buy of group) for (const sell of group) {
      if (buy.exchange === sell.exchange || buy.canonicalAsset !== sell.canonicalAsset || buy.canonicalQuote !== sell.canonicalQuote) continue;
      const candidate = calculateRoute(buy, sell, config, now, maxAge);
      if (!candidate || candidate.netSpread < minimumSpread || candidate.executableNotional < minimumNotional) continue;
      if (!selected || candidate.estimatedNetProfit > selected.estimatedNetProfit
        || (candidate.estimatedNetProfit === selected.estimatedNetProfit && candidate.netSpread > selected.netSpread)
        || (candidate.estimatedNetProfit === selected.estimatedNetProfit && candidate.netSpread === selected.netSpread && candidate.score > selected.score)) selected = candidate;
    }
    if (selected) best.push(selected);
  }
  return best.sort((a, b) => b.score - a.score || b.estimatedNetProfit - a.estimatedNetProfit || a.pair.localeCompare(b.pair));
}

function calculateRoute(buy: OrderBookState, sell: OrderBookState, config: ArbitrageConfig, now: number, maxAge: number): ArbitrageOpportunity | undefined {
  let askIndex = 0, bidIndex = 0;
  let askRemaining = buy.asks[0]?.quantity ?? 0, bidRemaining = sell.bids[0]?.quantity ?? 0;
  let quantity = 0, buyCost = 0, sellProceeds = 0;
  const buyFeeRate = config.feeRates[buy.exchange] ?? DEFAULT_FEE;
  const sellFeeRate = config.feeRates[sell.exchange] ?? DEFAULT_FEE;
  const askLimit = Math.min(config.maxDepthLevels ?? 20, buy.asks.length);
  const bidLimit = Math.min(config.maxDepthLevels ?? 20, sell.bids.length);

  while (askIndex < askLimit && bidIndex < bidLimit) {
    const ask = buy.asks[askIndex], bid = sell.bids[bidIndex];
    const marginalNetPerUnit = bid.price * (1 - sellFeeRate) - ask.price * (1 + buyFeeRate);
    if (marginalNetPerUnit <= 0) break;
    const take = Math.min(askRemaining, bidRemaining);
    if (!(take > 0)) break;
    quantity += take;
    buyCost += take * ask.price;
    sellProceeds += take * bid.price;
    askRemaining -= take;
    bidRemaining -= take;
    if (askRemaining <= Number.EPSILON) { askIndex += 1; askRemaining = buy.asks[askIndex]?.quantity ?? 0; }
    if (bidRemaining <= Number.EPSILON) { bidIndex += 1; bidRemaining = sell.bids[bidIndex]?.quantity ?? 0; }
  }
  if (!(quantity > 0) || !(buyCost > 0)) return undefined;

  const estimatedFees = buyCost * buyFeeRate + sellProceeds * sellFeeRate;
  const estimatedNetProfit = sellProceeds - buyCost - estimatedFees;
  const buyVWAP = buyCost / quantity, sellVWAP = sellProceeds / quantity;
  const grossSpread = (sellVWAP - buyVWAP) / buyVWAP;
  const netSpread = estimatedNetProfit / buyCost;
  const ageQuality = Math.max(0, 1 - Math.max(now - buy.receivedTimestamp, now - sell.receivedTimestamp) / maxAge);
  const score = calculateScore(netSpread, buyCost, estimatedNetProfit, ageQuality, config);
  return {
    pair: buy.canonicalPair, asset: buy.canonicalAsset, quote: buy.canonicalQuote,
    buyExchange: buy.exchange, sellExchange: sell.exchange,
    buyPrice: buy.asks[0].price, buyVWAP, sellPrice: sell.bids[0].price, sellVWAP,
    executableQuantity: quantity, executableNotional: buyCost, grossSpread,
    estimatedFees, netSpread, estimatedNetProfit, score, timestamp: now,
  };
}

function calculateScore(netSpread: number, notional: number, profit: number, quality: number, config: ArbitrageConfig): number {
  const weights = config.score?.weights ?? {};
  const parts = [
    { weight: weights.netSpread ?? 0.4, value: Math.min(100, Math.max(0, netSpread * 10_000)) },
    { weight: weights.executableNotional ?? 0.2, value: 100 * (1 - Math.exp(-notional / (config.score?.notionalScale ?? 10_000))) },
    { weight: weights.estimatedNetProfit ?? 0.3, value: 100 * (1 - Math.exp(-profit / (config.score?.profitScale ?? 10))) },
    { weight: weights.bookQuality ?? 0.1, value: quality * 100 },
  ];
  const weightTotal = parts.reduce((sum, part) => sum + part.weight, 0);
  if (parts.some((part) => !Number.isFinite(part.weight) || part.weight < 0) || weightTotal <= 0) throw new Error('Scoring weights must be non-negative with a positive total');
  return Math.round(parts.reduce((sum, part) => sum + part.weight * part.value, 0) / weightTotal * 100) / 100;
}

function isValidBook(book: OrderBookState, now: number, maxAge: number): boolean {
  const age = now - book.receivedTimestamp;
  if (!book.synchronized || book.stale || !book.sequenceValid || book.feedHealth !== 'HEALTHY'
    || (book.status !== 'SYNCHRONIZED' && book.status !== 'QUIET')
    || !Number.isFinite(book.receivedTimestamp) || age < 0 || age > maxAge
    || !Number.isFinite(book.processedTimestamp) || !Number.isFinite(book.exchangeTimestamp) || book.exchangeTimestamp === null || book.exchangeTimestamp <= 0
    || !book.bids.length || !book.asks.length) return false;
  if (!book.bids.every(validLevel) || !book.asks.every(validLevel)) return false;
  if (book.bestBid !== book.bids[0].price || book.bestBidQuantity !== book.bids[0].quantity
    || book.bestAsk !== book.asks[0].price || book.bestAskQuantity !== book.asks[0].quantity) return false;
  if (book.bestBid! >= book.bestAsk!) return false;
  for (let index = 1; index < book.bids.length; index += 1) if (book.bids[index - 1].price < book.bids[index].price) return false;
  for (let index = 1; index < book.asks.length; index += 1) if (book.asks[index - 1].price > book.asks[index].price) return false;
  return true;
}

function validLevel(level: { price: number; quantity: number }): boolean {
  return Number.isFinite(level.price) && level.price > 0 && Number.isFinite(level.quantity) && level.quantity > 0;
}
