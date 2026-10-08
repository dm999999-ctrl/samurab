import { LocalOrderBook, type OrderBookDelta } from '../orderbook';
import type { ExchangeId } from '../discovery/types';
import type { AdapterStatus, OrderBookLevel } from '../types';
import { isUsableOrderBook } from './validity';
import type { ExchangeMarketDataTelemetry, MarketBookStatus, OrderBookState } from './types';
import type { LiveMarketSubscription } from './universe';

type Entry = {
  subscription: LiveMarketSubscription;
  book: LocalOrderBook;
  connected: boolean;
  status: AdapterStatus | 'UNHEALTHY';
  sequenceValid: boolean;
  updateCount: number;
  sequenceGaps: number;
  resynchronizations: number;
  errors: string[];
  messagesReceived: number;
  updatesApplied: number;
  lastMessageReceivedTimestamp: number | null;
  lastAppliedUpdateTimestamp: number | null;
  synchronizedAt: number | null;
  everSynchronized: boolean;
  staleSince: number | null;
  staleTransitions: number;
  recoveries: number;
  quietSince: number | null;
  quietTransitions: number;
  quietRecoveries: number;
  lastExpectedSequence: number | string | null;
  lastObservedSequence: number | string | null;
  lastFailureReason: string | null;
  feedHealthy: boolean;
  lastFeedHeartbeatTimestamp: number | null;
};

export class MarketDataCoordinator {
  private readonly entries = new Map<string, Entry>();
  private readonly exchangeMessages = new Map<ExchangeId, number>();
  private readonly exchangeReconnects = new Map<ExchangeId, number>();
  private readonly exchangeLastMessage = new Map<ExchangeId, number>();
  private readonly processingSamples = new Map<ExchangeId, { values: number[]; next: number }>();
  private readonly recentMessages = new Map<ExchangeId, { timestamps: number[]; head: number }>();
  private readonly maxDepth: number;

  constructor(subscriptions: LiveMarketSubscription[], options: { depthLevels?: number } = {}) {
    this.maxDepth = options.depthLevels ?? 20;
    if (!Number.isInteger(this.maxDepth) || this.maxDepth < 1) throw new Error('ORDERBOOK_DEPTH_LEVELS must be a positive integer');
    for (const subscription of subscriptions) {
      const key = marketKey(subscription.exchange, subscription.exchangeSymbol);
      if (this.entries.has(key)) continue;
      this.entries.set(key, {
        subscription, book: new LocalOrderBook(subscription.exchange, subscription.canonicalPair),
        connected: false, status: 'STARTING', sequenceValid: false, updateCount: 0,
        sequenceGaps: 0, resynchronizations: 0, errors: [],
        messagesReceived: 0, updatesApplied: 0, lastMessageReceivedTimestamp: null,
        lastAppliedUpdateTimestamp: null, synchronizedAt: null, everSynchronized: false,
        staleSince: null, staleTransitions: 0, recoveries: 0,
        quietSince: null, quietTransitions: 0, quietRecoveries: 0,
        lastExpectedSequence: null, lastObservedSequence: null, lastFailureReason: null,
        feedHealthy: false, lastFeedHeartbeatTimestamp: null,
      });
    }
  }

  setConnection(exchange: ExchangeId, status: AdapterStatus, reconnects?: number): void {
    for (const entry of this.entries.values()) {
      if (entry.subscription.exchange !== exchange) continue;
      entry.status = status;
      entry.connected = status === 'CONNECTED' || status === 'SYNCING' || status === 'LIVE';
      if (!entry.connected) entry.feedHealthy = false;
      else if (status === 'CONNECTED') {
        entry.feedHealthy = true;
        entry.lastFeedHeartbeatTimestamp ??= Date.now();
      }
      if (status === 'STARTING' || status === 'DISCONNECTED') entry.sequenceValid = false;
    }
    if (reconnects !== undefined) this.exchangeReconnects.set(exchange, reconnects);
  }

  setFeedHealth(exchange: ExchangeId, healthy: boolean, timestamp = Date.now()): void {
    for (const entry of this.entries.values()) {
      if (entry.subscription.exchange !== exchange) continue;
      if (!healthy && entry.feedHealthy && entry.sequenceValid) {
        entry.sequenceValid = false;
        entry.status = 'UNHEALTHY';
        entry.resynchronizations += 1;
        entry.lastFailureReason = 'exchange feed health lost; book requires resynchronization';
        this.recordError(entry, entry.lastFailureReason);
      }
      entry.feedHealthy = healthy;
      if (healthy) entry.lastFeedHeartbeatTimestamp = timestamp;
      if (healthy && entry.status === 'UNHEALTHY') entry.status = 'RESYNCING';
    }
  }

  applySnapshot(exchange: ExchangeId, exchangeSymbol: string, levels: { bids: OrderBookLevel[]; asks: OrderBookLevel[]; sequence: number | string | null; exchangeTimestamp: number | null; receivedTimestamp?: number }): boolean {
    const entry = this.getEntry(exchange, exchangeSymbol);
    const started = performance.now();
    if (!validSide(levels.bids) || !validSide(levels.asks) || !validTimestamp(levels.exchangeTimestamp) || !validSequence(levels.sequence)) {
      this.recordError(entry, 'invalid snapshot fields (empty/invalid levels, timestamp, or sequence)');
      entry.lastFailureReason = 'invalid snapshot fields (empty/invalid levels, timestamp, or sequence)';
      entry.status = 'ERROR'; entry.sequenceValid = false;
      this.recordMessage(entry.subscription.exchange, performance.now() - started);
      return false;
    }
    entry.book.loadSnapshot(levels.sequence, trim(levels.bids, this.maxDepth, true), trim(levels.asks, this.maxDepth, false), levels.receivedTimestamp ?? Date.now(), levels.exchangeTimestamp);
    entry.book.markLive();
    entry.sequenceValid = true;
    entry.status = 'LIVE';
    entry.connected = true;
    entry.updateCount += 1;
    entry.errors = [];
    entry.lastAppliedUpdateTimestamp = levels.receivedTimestamp ?? Date.now();
    if (!entry.everSynchronized || entry.quietSince !== null) entry.synchronizedAt = Date.now();
    if (entry.staleSince !== null) { entry.recoveries += 1; entry.staleSince = null; }
    if (entry.quietSince !== null) { entry.quietRecoveries += 1; entry.quietSince = null; }
    entry.everSynchronized = true;
    entry.lastFailureReason = null;
    this.recordMessage(exchange, performance.now() - started);
    return true;
  }

  applyDelta(exchange: ExchangeId, exchangeSymbol: string, delta: OrderBookDelta): boolean {
    const entry = this.getEntry(exchange, exchangeSymbol);
    const started = performance.now();
    if (!validTimestamp(delta.exchangeTimestamp) || !validSequence(delta.firstUpdateId) || !validSequence(delta.finalUpdateId)
      || !validSide(delta.bids, true) || !validSide(delta.asks, true)) {
      this.recordError(entry, 'invalid delta fields');
      entry.lastFailureReason = 'invalid delta fields';
      entry.status = 'ERROR'; entry.sequenceValid = false;
      this.recordMessage(exchange, performance.now() - started);
      return false;
    }
    if (!entry.sequenceValid) {
      this.recordError(entry, 'delta rejected while sequence is invalid; snapshot recovery required');
      entry.lastFailureReason = 'delta rejected while sequence is invalid; snapshot recovery required';
      entry.status = 'RESYNCING';
      this.recordMessage(exchange, performance.now() - started);
      return false;
    }
    const priorSequence = entry.book.sequence;
    entry.lastExpectedSequence = priorSequence === null ? null : incrementSequence(priorSequence);
    entry.lastObservedSequence = delta.finalUpdateId;
    const ok = entry.book.apply(delta);
    if (!ok) {
      entry.sequenceGaps += 1;
      entry.sequenceValid = false;
      entry.status = 'RESYNCING';
      entry.resynchronizations += 1;
      this.recordError(entry, `sequence gap: update ${delta.firstUpdateId}-${delta.finalUpdateId} after ${priorSequence}`);
      entry.lastFailureReason = `sequence gap: update ${delta.firstUpdateId}-${delta.finalUpdateId} after ${priorSequence}`;
      this.recordMessage(exchange, performance.now() - started);
      return false;
    }
    if (priorSequence === null) {
      entry.sequenceValid = false;
      entry.status = 'RESYNCING';
      entry.resynchronizations += 1;
      this.recordError(entry, 'delta received before snapshot');
      entry.lastFailureReason = 'delta received before snapshot';
      this.recordMessage(exchange, performance.now() - started);
      return false;
    }
    entry.sequenceValid = true;
    entry.status = 'LIVE';
    entry.connected = true;
    entry.updateCount += 1;
    entry.updatesApplied += 1;
    entry.lastAppliedUpdateTimestamp = Date.now();
    entry.errors = [];
    if (!entry.everSynchronized || entry.quietSince !== null) entry.synchronizedAt = Date.now();
    if (entry.staleSince !== null) { entry.recoveries += 1; entry.staleSince = null; }
    if (entry.quietSince !== null) { entry.quietRecoveries += 1; entry.quietSince = null; }
    entry.everSynchronized = true;
    entry.lastFailureReason = null;
    this.recordMessage(exchange, performance.now() - started);
    return true;
  }

  markResynchronizing(exchange: ExchangeId, exchangeSymbol: string, reason: string): void {
    const entry = this.getEntry(exchange, exchangeSymbol);
    entry.status = 'RESYNCING'; entry.sequenceValid = false; entry.resynchronizations += 1;
    this.recordError(entry, reason);
    entry.lastFailureReason = reason;
  }

  markSequenceGap(exchange: ExchangeId, exchangeSymbol: string, expected: number | string, observed: number | string, reason: string): void {
    const entry = this.getEntry(exchange, exchangeSymbol);
    entry.status = 'RESYNCING'; entry.sequenceValid = false; entry.sequenceGaps += 1; entry.resynchronizations += 1;
    entry.lastExpectedSequence = expected; entry.lastObservedSequence = observed;
    entry.book.markResyncing();
    this.recordError(entry, reason);
    entry.lastFailureReason = reason;
  }

  getBook(exchange: ExchangeId, exchangeSymbol: string, now = Date.now(), maxAgeMs = configuredAge(exchange)): OrderBookState {
    const entry = this.getEntry(exchange, exchangeSymbol);
    const source = entry.book.snapshot;
    const bids = source.bids.slice(0, this.maxDepth), asks = source.asks.slice(0, this.maxDepth);
    const status: MarketBookStatus = entry.status === 'STARTING' ? 'STARTING'
      : !entry.connected || entry.status === 'DISCONNECTED' ? 'DISCONNECTED'
      : entry.status === 'ERROR' ? 'ERROR'
      : entry.status === 'RESYNCING' || entry.status === 'SYNCING' ? entry.status
      : !entry.feedHealthy ? 'UNHEALTHY'
      : source.sequence === null ? (entry.status === 'CONNECTED' ? 'CONNECTED' : 'SYNCHRONIZING')
      : !entry.sequenceValid ? 'RESYNCING'
      : now - source.receivedTimestamp > maxAgeMs ? 'QUIET'
      : 'SYNCHRONIZED';
    if (status === 'QUIET' && entry.quietSince === null) { entry.quietSince = source.receivedTimestamp + maxAgeMs; entry.quietTransitions += 1; }
    else if (status === 'SYNCHRONIZED' && entry.quietSince !== null) { entry.quietRecoveries += 1; entry.quietSince = null; }
    return {
      exchange, canonicalAsset: entry.subscription.canonicalAsset, canonicalQuote: entry.subscription.canonicalQuote,
      canonicalPair: entry.subscription.canonicalPair, exchangeSymbol: entry.subscription.exchangeSymbol,
      bids, asks, bestBid: bids[0]?.price ?? null, bestBidQuantity: bids[0]?.quantity ?? null,
      bestAsk: asks[0]?.price ?? null, bestAskQuantity: asks[0]?.quantity ?? null,
      sequence: source.sequence ?? null, sequenceValid: entry.sequenceValid,
      feedHealth: !entry.connected ? 'DISCONNECTED' : entry.feedHealthy ? 'HEALTHY' : 'UNHEALTHY',
      lastFeedHeartbeatTimestamp: entry.lastFeedHeartbeatTimestamp,
      exchangeTimestamp: source.exchangeTimestamp, receivedTimestamp: source.receivedTimestamp,
      processedTimestamp: source.processedTimestamp, status, synchronized: status === 'SYNCHRONIZED' || status === 'QUIET',
      stale: false, updateCount: entry.updateCount,
    };
  }

  isUsable(exchange: ExchangeId, exchangeSymbol: string, now = Date.now(), maxAgeMs = configuredAge(exchange)): boolean {
    const book = this.getBook(exchange, exchangeSymbol, now, maxAgeMs);
      const entry = this.getEntry(exchange, exchangeSymbol);
    return isUsableOrderBook(book, { connected: entry.connected });
  }

  getExchangeTelemetry(exchange: ExchangeId, now = Date.now(), maxAgeMs = configuredAge(exchange)): ExchangeMarketDataTelemetry {
    const entries = [...this.entries.values()].filter((entry) => entry.subscription.exchange === exchange);
    const latencyRing = this.processingSamples.get(exchange);
    const latencies = latencyRing?.values ?? [];
    const synced = entries.filter((entry) => this.getBook(exchange, entry.subscription.exchangeSymbol, now, maxAgeMs).synchronized).length;
    const quiet = entries.filter((entry) => this.getBook(exchange, entry.subscription.exchangeSymbol, now, maxAgeMs).status === 'QUIET').length;
    const stale = entries.filter((entry) => this.getBook(exchange, entry.subscription.exchangeSymbol, now, maxAgeMs).status === 'STALE').length;
    const disconnected = entries.filter((entry) => entry.status === 'DISCONNECTED' || entry.status === 'STARTING').length;
    const errors = entries.flatMap((entry) => entry.errors);
    const messages = this.exchangeMessages.get(exchange) ?? 0;
    return {
      exchange, connections: entries.length ? Number(entries.some((entry) => entry.connected)) : 0,
      connectedStreams: entries.filter((entry) => entry.connected).length, subscriptions: entries.length,
      synchronizedBooks: synced, staleBooks: stale, disconnectedBooks: disconnected,
      quietBooks: quiet, unhealthyBooks: entries.filter((entry) => this.getBook(exchange, entry.subscription.exchangeSymbol, now, maxAgeMs).status === 'UNHEALTHY').length,
      errorBooks: entries.filter((entry) => entry.status === 'ERROR').length,
      reconnects: this.exchangeReconnects.get(exchange) ?? 0, messages,
      messagesPerSecond: this.messagesPerSecond(exchange, now), lastMessageTimestamp: this.exchangeLastMessage.get(exchange) ?? null,
      averageProcessingLatencyMs: latencies.length ? latencies.reduce((a, b) => a + b, 0) / latencies.length : null,
      maxProcessingLatencyMs: latencies.length ? maxValue(latencies) : null,
      sequenceGaps: entries.reduce((sum, entry) => sum + entry.sequenceGaps, 0),
      resynchronizations: entries.reduce((sum, entry) => sum + entry.resynchronizations, 0),
      errors: [...new Set(errors)],
    };
  }

  subscriptions(): LiveMarketSubscription[] { return [...this.entries.values()].map((entry) => entry.subscription); }
  counts() { return { books: this.entries.size, depthLevels: this.maxDepth }; }
  sequence(exchange: ExchangeId, exchangeSymbol: string): number | string | null {
    return this.getEntry(exchange, exchangeSymbol).book.sequence;
  }

  recordExternalMessage(exchange: ExchangeId, latencyMs = 0, exchangeSymbol?: string): void {
    if (latencyMs > 0) this.recordMessage(exchange, latencyMs);
    const now = Date.now();
    this.exchangeMessages.set(exchange, (this.exchangeMessages.get(exchange) ?? 0) + 1);
    this.exchangeLastMessage.set(exchange, now);
    this.recordRecentMessage(exchange, now);
    if (exchangeSymbol) {
      const entry = this.getEntry(exchange, exchangeSymbol);
      entry.messagesReceived += 1;
      entry.lastMessageReceivedTimestamp = Date.now();
    }
  }

  symbolDiagnostics(exchange: ExchangeId, exchangeSymbol: string, now = Date.now(), maxAgeMs = configuredAge(exchange)) {
    const entry = this.getEntry(exchange, exchangeSymbol);
    const book = this.getBook(exchange, exchangeSymbol, now, maxAgeMs);
    const snapshot = entry.book.snapshot;
    return {
      exchange, symbol: entry.subscription.exchangeSymbol, canonicalPair: entry.subscription.canonicalPair,
      subscriptionState: entry.connected ? 'SUBSCRIBED' : entry.status,
      feedHealth: !entry.connected ? 'DISCONNECTED' : entry.feedHealthy ? 'HEALTHY' : 'UNHEALTHY',
      feedHealthy: entry.connected && entry.feedHealthy,
      lastFeedHeartbeatTimestamp: entry.lastFeedHeartbeatTimestamp,
      snapshotState: exchange === 'binance' ? (snapshot.sequence === null ? 'PENDING' : 'RECEIVED') : 'STREAM_SNAPSHOT',
      synchronizationState: book.status, pending: snapshot.sequence === null && entry.status !== 'ERROR',
      snapshotting: false, synchronizing: ['STARTING', 'CONNECTED', 'SYNCING', 'RESYNCING', 'SYNCHRONIZING'].includes(book.status),
      synchronized: book.synchronized, quiet: book.status === 'QUIET', stale: book.stale, failed: book.status === 'ERROR',
      everSynchronized: entry.everSynchronized,
      lastExchangeUpdateReceivedAt: entry.lastMessageReceivedTimestamp,
      lastSuccessfullyAppliedUpdateAt: entry.lastAppliedUpdateTimestamp,
      bookAgeMs: snapshot.sequence === null ? null : Math.max(0, now - snapshot.receivedTimestamp), synchronizedAt: entry.synchronizedAt,
      staleSince: entry.staleSince, staleTransitions: entry.staleTransitions, recoveries: entry.recoveries,
      quietSince: entry.quietSince, quietTransitions: entry.quietTransitions, quietRecoveries: entry.quietRecoveries,
      lastSequence: snapshot.sequence, expectedSequence: entry.lastExpectedSequence,
      observedSequence: entry.lastObservedSequence, messagesReceived: entry.messagesReceived,
      updatesApplied: entry.updatesApplied, sequenceGaps: entry.sequenceGaps,
      resynchronizations: entry.resynchronizations, reconnectCount: this.exchangeReconnects.get(exchange) ?? 0,
      lastFailureReason: entry.lastFailureReason,
    };
  }

  private recordMessage(exchange: ExchangeId, latency: number) {
    // This is processing-cost telemetry only. Transport message counts are recorded once
    // by recordExternalMessage, so snapshots/deltas are not double-counted here.
    let ring = this.processingSamples.get(exchange);
    if (!ring) { ring = { values: [], next: 0 }; this.processingSamples.set(exchange, ring); }
    if (ring.values.length < 1000) ring.values.push(latency);
    else { ring.values[ring.next] = latency; ring.next = (ring.next + 1) % 1000; }
  }
  private messagesPerSecond(exchange: ExchangeId, now: number) {
    const recent = this.recentMessages.get(exchange);
    if (!recent) return 0;
    this.pruneRecentMessages(recent, now);
    return recent.timestamps.length - recent.head;
  }
  private recordRecentMessage(exchange: ExchangeId, now: number) {
    let recent = this.recentMessages.get(exchange);
    if (!recent) { recent = { timestamps: [], head: 0 }; this.recentMessages.set(exchange, recent); }
    recent.timestamps.push(now);
    this.pruneRecentMessages(recent, now);
  }
  private pruneRecentMessages(recent: { timestamps: number[]; head: number }, now: number) {
    while (recent.head < recent.timestamps.length && now - recent.timestamps[recent.head] > 1_000) recent.head += 1;
    if (recent.head > 1024 && recent.head * 2 > recent.timestamps.length) {
      recent.timestamps = recent.timestamps.slice(recent.head);
      recent.head = 0;
    }
  }
  private getEntry(exchange: ExchangeId, symbol: string): Entry {
    const entry = this.entries.get(marketKey(exchange, symbol));
    if (!entry) throw new Error(`Unconfigured market subscription ${exchange}:${symbol}`);
    return entry;
  }
  private recordError(entry: Entry, message: string) {
    entry.errors = [message, ...entry.errors].slice(0, 20);
  }
}

function maxValue(values: number[]) {
  let maximum = -Infinity;
  for (const value of values) if (value > maximum) maximum = value;
  return maximum;
}

export function configuredAge(exchange: ExchangeId): number {
  const variable = `MAX_BOOK_AGE_MS_${exchange.toUpperCase().replace(/[^A-Z0-9]/g, '_')}`;
  const raw = process.env[variable] ?? process.env.MAX_BOOK_AGE_MS;
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 2_000;
}

function marketKey(exchange: ExchangeId, symbol: string) { return `${exchange}:${symbol.toUpperCase()}`; }
function validSequence(value: number | string | null): value is number | string {
  if (typeof value === 'number') return Number.isSafeInteger(value) && value >= 0;
  return typeof value === 'string' && /^\d+$/.test(value);
}
function incrementSequence(value: number | string): number | string {
  return typeof value === 'number' ? value + 1 : (BigInt(value) + 1n).toString();
}
function validTimestamp(value: number | null): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value > 0;
}
function validSide(levels: OrderBookLevel[], allowEmpty = false) {
  return Array.isArray(levels) && (allowEmpty || levels.length > 0)
    && levels.every((level) => Number.isFinite(level.price) && level.price > 0 && Number.isFinite(level.quantity) && level.quantity >= 0);
}
function trim(levels: OrderBookLevel[], limit: number, bids: boolean) {
  return [...levels].sort((a, b) => bids ? b.price - a.price : a.price - b.price).slice(0, limit);
}
