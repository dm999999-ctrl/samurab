import type { ExchangeId } from '../discovery/types';
import type { MarketDataCoordinator } from '../market-data/coordinator';
import type { LiveMarketSubscription } from '../market-data/universe';
import type { OrderBookState } from '../market-data/types';
import { detectArbitrageOpportunities, type ArbitrageConfig, type ArbitrageOpportunity } from './detection';

type CachedOpportunity = { opportunity: ArbitrageOpportunity; expiresAt: number };

export class ArbitrageMonitor {
  private readonly cache = new Map<string, CachedOpportunity>();
  private readonly subscriptionsByMarket = new Map<string, LiveMarketSubscription>();
  private readonly subscriptionsByPair = new Map<string, LiveMarketSubscription[]>();
  private readonly unsubscribe: () => void;
  private readonly config: ArbitrageConfig;
  private bookChanges = 0;
  private pairRecalculations = 0;
  private usableBookInputs = 0;
  private latestUsableBooks = 0;
  private readonly usableBooksSeen = new Set<string>();
  private readonly pendingPairs = new Set<string>();
  private flushTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(
    private readonly coordinator: MarketDataCoordinator,
    subscriptions: LiveMarketSubscription[],
    config: ArbitrageConfig,
  ) {
    this.config = {
      ...config,
      feeRates: { ...config.feeRates },
      maxBookAgeMs: config.maxBookAgeMs ?? 60_000,
      maxDepthLevels: config.maxDepthLevels ?? 20,
      minimumNetSpread: config.minimumNetSpread ?? 0,
      minimumExecutableNotional: config.minimumExecutableNotional ?? 10,
    };
    detectArbitrageOpportunities([], this.config, Date.now());
    for (const subscription of subscriptions) {
      this.subscriptionsByMarket.set(marketKey(subscription.exchange, subscription.exchangeSymbol), subscription);
      const pair = this.subscriptionsByPair.get(subscription.canonicalPair) ?? [];
      pair.push(subscription);
      this.subscriptionsByPair.set(subscription.canonicalPair, pair);
    }
    this.unsubscribe = coordinator.onBookChange((exchange, symbol) => this.onBookChange(exchange, symbol));
  }

  snapshot(now = Date.now()) {
    for (const [pair, cached] of this.cache) if (cached.expiresAt <= now) this.cache.delete(pair);
    const opportunities = [...this.cache.values()].map((cached) => cached.opportunity)
      .sort((a, b) => b.score - a.score || b.estimatedNetProfit - a.estimatedNetProfit);
    return {
      config: this.config,
      booksConsumedInLatestEvaluation: this.latestUsableBooks,
      cumulativeUsableBookInputs: this.usableBookInputs,
      distinctUsableBooksConsumed: this.usableBooksSeen.size,
      bookChanges: this.bookChanges,
      pairRecalculations: this.pairRecalculations,
      qualifyingOpportunityCount: opportunities.length,
      opportunities,
    };
  }

  stop(): void {
    this.unsubscribe();
    if (this.flushTimer !== undefined) clearTimeout(this.flushTimer);
    this.pendingPairs.clear();
  }

  private onBookChange(exchange: ExchangeId, symbol: string): void {
    const subscription = this.subscriptionsByMarket.get(marketKey(exchange, symbol));
    if (!subscription) return;
    this.bookChanges += 1;
    const pair = subscription.canonicalPair;
    const cached = this.cache.get(pair);
    if (cached && !this.coordinator.isUsable(exchange, symbol)
      && (cached.opportunity.buyExchange === exchange || cached.opportunity.sellExchange === exchange)) this.cache.delete(pair);
    this.pendingPairs.add(pair);
    if (this.flushTimer === undefined) {
      this.flushTimer = setTimeout(() => this.flushPendingPairs(), 25);
      this.flushTimer.unref?.();
    }
  }

  private flushPendingPairs(): void {
    this.flushTimer = undefined;
    const pairs = [...this.pendingPairs];
    this.pendingPairs.clear();
    for (const pair of pairs) this.recalculatePair(pair, Date.now());
  }

  private recalculatePair(pair: string, now: number): void {
    const subscriptions = this.subscriptionsByPair.get(pair) ?? [];
    this.pairRecalculations += 1;
    const books = subscriptions.map((subscription) =>
      this.coordinator.getBook(subscription.exchange, subscription.exchangeSymbol, now));
    this.latestUsableBooks = books.filter((book) =>
      this.coordinator.isUsable(book.exchange, book.exchangeSymbol, now)).length;
    this.usableBookInputs += this.latestUsableBooks;
    for (const book of books) {
      if (this.coordinator.isUsable(book.exchange, book.exchangeSymbol, now)) this.usableBooksSeen.add(marketKey(book.exchange, book.exchangeSymbol));
    }

    const [opportunity] = detectArbitrageOpportunities(books, this.config, now);
    if (!opportunity) { this.cache.delete(pair); return; }
    const buyBook = books.find((book) => book.exchange === opportunity.buyExchange);
    const sellBook = books.find((book) => book.exchange === opportunity.sellExchange);
    if (!buyBook || !sellBook) { this.cache.delete(pair); return; }
    const expiresAt = Math.min(buyBook.receivedTimestamp, sellBook.receivedTimestamp) + (this.config.maxBookAgeMs ?? 60_000);
    this.cache.set(pair, { opportunity, expiresAt });
  }
}
function marketKey(exchange: ExchangeId, symbol: string): string {
  return `${exchange}:${symbol.toUpperCase()}`;
}

export type { ArbitrageConfig, ArbitrageOpportunity, OrderBookState };
