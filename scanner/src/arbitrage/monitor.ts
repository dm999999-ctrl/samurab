import type { ExchangeId } from '../discovery/types';
import type { MarketDataCoordinator } from '../market-data/coordinator';
import type { LiveMarketSubscription } from '../market-data/universe';
import type { OrderBookState } from '../market-data/types';
import { performance } from 'node:perf_hooks';
import { isUsableOrderBook } from '../market-data/validity';

import { detectArbitrageOpportunities, type ArbitrageConfig, type ArbitrageOpportunity } from './detection';
import { DEFAULT_PROFITABILITY_LIMITS, estimateOpportunityProfitability, type ProfitabilityLimits, type ProfitabilityOpportunity } from './profitability';

const PAIR_RECALCULATION_INTERVAL_MS = 50;
type TimingName = 'bookSnapshots' | 'bookValidation' | 'phaseCDetection' | 'phaseDProfitability' | 'pairRecalculation';
type TimingMetric = { count: number; totalMs: number; maxMs: number };
type PendingPair = { changes: number; lastChangedAt: number };
type CachedOpportunity = { opportunity: ProfitabilityOpportunity; expiresAt: number };

export class ArbitrageMonitor {
  private readonly cache = new Map<string, CachedOpportunity>();
  private profitabilityEvaluations = 0;
  private readonly profitabilityRejections: Record<string, number> = {};
  private readonly subscriptionsByMarket = new Map<string, LiveMarketSubscription>();
  private readonly subscriptionsByPair = new Map<string, LiveMarketSubscription[]>();
  private readonly unsubscribe: () => void;
  private readonly config: ArbitrageConfig;
  private readonly profitabilityLimits: ProfitabilityLimits;
  private bookChanges = 0;
  private pairRecalculations = 0;
  private usableBookInputs = 0;
  private latestUsableBooks = 0;
  private readonly usableBooksSeen = new Set<string>();
  private readonly pendingPairs = new Map<string, PendingPair>();
  private coalescedBookChanges = 0;
  private recalculationFlushes = 0;
  private coalescingDelayTotalMs = 0;
  private maxCoalescingDelayMs = 0;
  private readonly timings: Record<TimingName, TimingMetric> = {
    bookSnapshots: { count: 0, totalMs: 0, maxMs: 0 }, bookValidation: { count: 0, totalMs: 0, maxMs: 0 },
    phaseCDetection: { count: 0, totalMs: 0, maxMs: 0 }, phaseDProfitability: { count: 0, totalMs: 0, maxMs: 0 },
    pairRecalculation: { count: 0, totalMs: 0, maxMs: 0 },
  };
  private flushTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(
    private readonly coordinator: MarketDataCoordinator,
    subscriptions: LiveMarketSubscription[],
    config: ArbitrageConfig,
    profitabilityLimits: ProfitabilityLimits = DEFAULT_PROFITABILITY_LIMITS,
  ) {
    this.profitabilityLimits = { ...profitabilityLimits };
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
      profitabilityEvaluations: this.profitabilityEvaluations,
      profitabilityRejections: { ...this.profitabilityRejections },
      profitabilityLimits: this.profitabilityLimits,
      processing: {
        coalescingIntervalMs: PAIR_RECALCULATION_INTERVAL_MS,
        coalescedBookChanges: this.coalescedBookChanges,
        recalculationFlushes: this.recalculationFlushes,
        averageCoalescingDelayMs: this.pairRecalculations ? round3(this.coalescingDelayTotalMs / this.pairRecalculations) : 0,
        maxCoalescingDelayMs: round3(this.maxCoalescingDelayMs),
        stages: Object.fromEntries(Object.entries(this.timings).map(([name, timing]) => [name, {
          count: timing.count, totalMs: round3(timing.totalMs), averageMs: timing.count ? round3(timing.totalMs / timing.count) : 0, maxMs: round3(timing.maxMs),
        }])),
      },
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
    if (cached && (cached.opportunity.buyExchange === exchange || cached.opportunity.sellExchange === exchange)) {
      const book = this.coordinator.getBook(exchange, symbol);
      if (!isUsableOrderBook(book, { connected: book.feedHealth === 'HEALTHY' })) this.cache.delete(pair);
    }
    const pending = this.pendingPairs.get(pair);
    if (pending) { pending.changes += 1; pending.lastChangedAt = performance.now(); this.coalescedBookChanges += 1; }
    else this.pendingPairs.set(pair, { changes: 1, lastChangedAt: performance.now() });
    if (this.flushTimer === undefined) {
      this.flushTimer = setTimeout(() => this.flushPendingPairs(), PAIR_RECALCULATION_INTERVAL_MS);
      this.flushTimer.unref?.();
    }
  }

  private flushPendingPairs(): void {
    this.flushTimer = undefined;
    const pendingPairs = [...this.pendingPairs.entries()];
    this.pendingPairs.clear();
    this.recalculationFlushes += 1;
    const flushTime = performance.now();
    for (const [pair, pending] of pendingPairs) {
      const delay = Math.max(0, flushTime - pending.lastChangedAt);
      this.coalescingDelayTotalMs += delay;
      this.maxCoalescingDelayMs = Math.max(this.maxCoalescingDelayMs, delay);
      this.recalculatePair(pair, Date.now());
    }
  }

  private recalculatePair(pair: string, now: number): void {
    const started = performance.now();
    try { this.recalculatePairNow(pair, now); }
    finally { this.recordTiming('pairRecalculation', performance.now() - started); }
  }

  private recalculatePairNow(pair: string, now: number): void {
    const subscriptions = this.subscriptionsByPair.get(pair) ?? [];
    this.pairRecalculations += 1;
    const snapshotStarted = performance.now();
    const books = subscriptions.map((subscription) =>
      this.coordinator.getBook(subscription.exchange, subscription.exchangeSymbol, now));
    this.recordTiming('bookSnapshots', performance.now() - snapshotStarted);
    let usableBooks = 0;
    for (const book of books) {
      const validationStarted = performance.now();
      const usable = isUsableOrderBook(book, { connected: book.feedHealth === 'HEALTHY' });
      this.recordTiming('bookValidation', performance.now() - validationStarted);
      if (usable) {
        usableBooks += 1;
        this.usableBooksSeen.add(marketKey(book.exchange, book.exchangeSymbol));
      }
    }
    this.latestUsableBooks = usableBooks;
    this.usableBookInputs += this.latestUsableBooks;

    const detectionStarted = performance.now();
    const [opportunity] = detectArbitrageOpportunities(books, this.config, now);
    this.recordTiming('phaseCDetection', performance.now() - detectionStarted);
    if (!opportunity) { this.cache.delete(pair); return; }
    const buyBook = books.find((book) => book.exchange === opportunity.buyExchange);
    const sellBook = books.find((book) => book.exchange === opportunity.sellExchange);
    if (!buyBook || !sellBook) { this.cache.delete(pair); return; }
    this.profitabilityEvaluations += 1;
    const profitabilityStarted = performance.now();
    const estimate = estimateOpportunityProfitability(opportunity, buyBook, sellBook, this.config, this.profitabilityLimits, now);
    this.recordTiming('phaseDProfitability', performance.now() - profitabilityStarted);
    if (!estimate.opportunity) {
      const reason = estimate.rejectionReason ?? 'unknown';
      this.profitabilityRejections[reason] = (this.profitabilityRejections[reason] ?? 0) + 1;
      this.cache.delete(pair);
      return;
    }
    const expiresAt = Math.min(buyBook.receivedTimestamp, sellBook.receivedTimestamp) + (this.config.maxBookAgeMs ?? 60_000);
    this.cache.set(pair, { opportunity: estimate.opportunity, expiresAt });
  }
  private recordTiming(name: TimingName, durationMs: number): void {
    const metric = this.timings[name];
    const duration = Math.max(0, durationMs);
    metric.count += 1;
    metric.totalMs += duration;
    metric.maxMs = Math.max(metric.maxMs, duration);
  }
}
function marketKey(exchange: ExchangeId, symbol: string): string {
  return `${exchange}:${symbol.toUpperCase()}`;
}

function round3(value: number): number { return Math.round(value * 1_000) / 1_000; }
export type { ArbitrageConfig, ArbitrageOpportunity, OrderBookState };
