import WebSocket from 'ws';
import { randomBytes } from 'node:crypto';
import type { ExchangeId } from '../discovery/types';
import type { OrderBookLevel } from '../types';
import type { LiveMarketSubscription } from './universe';
import { MarketDataCoordinator } from './coordinator';
import { BinanceSnapshotScheduler, type SnapshotResponse } from './binance-snapshot-scheduler';
import { classifyOkxUpdate, mapOkxSpotInstrument, OKX_PUBLIC_SPOT_WS_URL, parseOkxFrame } from './okx-protocol';
import { classifyKucoinDelta, KUCOIN_FULL_ORDERBOOK_URL, KUCOIN_LEVEL2_TOPIC, KUCOIN_PUBLIC_TOKEN_URL, mapKucoinSpotSymbol, parseKucoinFrame, parseKucoinSnapshot, type KucoinDelta } from './kucoin-protocol';
import { classifyMexcDelta, MEXC_DEPTH_SNAPSHOT_URL, MEXC_MAX_SUBSCRIPTIONS_PER_SOCKET, MEXC_SPOT_WS_URL, mexcDepthChannel, mapMexcSpotSymbol, parseMexcFrame, parseMexcSnapshot, type MexcDepthDelta } from './mexc-protocol';
import { classifyGateDelta, GATE_SPOT_ORDER_BOOK_URL, GATE_SPOT_ORDER_BOOK_UPDATE_CHANNEL, GATE_SPOT_WS_URL, mapGateSpotSymbol, parseGateFrame, parseGateSnapshot, type GateBookDelta } from './gate-protocol';
import { BITGET_SPOT_BOOK_TOPIC, BITGET_SPOT_WS_URL, classifyBitgetUpdate, mapBitgetSpotSymbol, parseBitgetFrame, type BitgetBookMessage } from './bitget-protocol';
import { classifyHtxDelta, htxMbpTopic, HTX_SPOT_MBP_WS_URL, mapHtxSpotSymbol, parseHtxFrame, type HtxBookDelta, type HtxBookSnapshot } from './htx-protocol';
import { classifyCryptoComUpdate, CRYPTO_COM_BOOK_DEPTH, CRYPTO_COM_SPOT_WS_URL, mapCryptoComSpotInstrument, parseCryptoComFrame } from './crypto-com-protocol';
import { classifyCoinbaseSequence, COINBASE_PRODUCT_BATCH_SIZE, COINBASE_SPOT_WS_URL, mapCoinbaseSpotProduct, parseCoinbaseFrame } from './coinbase-protocol';

type SessionStats = { exchange: ExchangeId; connections: number; reconnects: number; errors: string[]; feedHealthy: boolean; lastFeedHeartbeatTimestamp: number | null };
type BinanceDelta = { U: number; u: number; E?: number; b: string[][]; a: string[][] };
type BootstrapSymbol = { state: string; startedAt: number; completedAt: number | null; lastFailure: string | null };
type KucoinBootstrap = { state: string; startedAt: number; completedAt: number | null; snapshotSequence: number | null; pendingDeltas: number; lastFailure: string | null };
type MexcBootstrap = { state: string; startedAt: number; completedAt: number | null; snapshotVersion: number | null; pendingDeltas: number; lastFailure: string | null };
type GateBootstrap = { state: string; startedAt: number; completedAt: number | null; snapshotId: number | null; pendingDeltas: number; lastFailure: string | null };
type BitgetBootstrap = { state: string; startedAt: number; completedAt: number | null; sequence: string | null; pendingDeltas: number; lastFailure: string | null };
type BitgetChannelError = { at: number; event: string; symbol: string | null; message: string };
type BitgetConnectionDiagnostic = {
  connectionId: number; attemptNumber: number; reasonForConnect: string | null; startedAt: number;
  openedAt: number | null; closedAt: number | null; closeCode: number | null; closeReason: string | null;
  phase: 'CONNECTING' | 'SUBSCRIBING' | 'SNAPSHOT_BOOTSTRAP' | 'PARTIALLY_SYNCHRONIZED' | 'SYNCHRONIZED' | 'CLOSED';
  subscriptionRequest: { status: 'NOT_SENT' | 'SENT' | 'ACKNOWLEDGED' | 'ERROR'; sentAt: number | null;
    requestedChannels: number; requestedSymbols: string[]; acknowledgedChannels: number; acknowledgedSymbols: string[]; ackResponses: number; responseAt: number | null;
    channelErrors: BitgetChannelError[] };
  receivedMessages: number; receivedBookMessages: number; firstBookUpdateAt: number | null;
  disconnectAfterFirstBookUpdate: boolean | null; synchronizedBooksAtDisconnect: number | null;
  disconnectPhase: 'CONNECTING' | 'SUBSCRIBING' | 'SNAPSHOT_BOOTSTRAP' | 'PARTIALLY_SYNCHRONIZED' | 'AFTER_SYNCHRONIZATION' | null;
  transportPingFrames: number; transportPongFrames: number; textPongMessages: number;
  lastError: string | null; triggerReason: string | null; reconnectAttempt: number | null;
  reconnectReason: string | null; reconnectScheduledAt: number | null;
  sequenceStateAtDisconnect: Record<string, { bootstrapState: string; sequence: string | null;
    pendingDeltas: number; synchronized: boolean; sequenceGaps: number; resynchronizations: number;
    lastFailure: string | null }> | null;
  errors: string[];
};
type HtxBootstrap = { state: string; startedAt: number; completedAt: number | null; sequence: string | null; pendingDeltas: number; lastFailure: string | null };
type CryptoComBootstrap = { state: string; startedAt: number; completedAt: number | null; sequence: number | string | null; lastFailure: string | null };
type CoinbaseBootstrap = { state: string; startedAt: number; completedAt: number | null; localSequence: string; exchangeSequence: number | string | null; sequenceSource: 'LOCAL_ORDER' | 'EXCHANGE'; lastFailure: string | null };

/** One multiplexed public-market socket per exchange, isolated from legacy port 4000. */
export class LiveExchangeSessions {
  private readonly sessions = new Map<ExchangeId, SessionStats>();
  private readonly sockets = new Map<ExchangeId, WebSocket>();
  private readonly timers = new Map<string, NodeJS.Timeout>();
  private readonly feedHealthTimers = new Map<ExchangeId, NodeJS.Timeout>();
  private readonly okxHeartbeatTimers = new Map<ExchangeId, NodeJS.Timeout>();
  private readonly okxPingSentAt = new Map<ExchangeId, number>();
  private readonly kucoinHeartbeatTimers = new Map<ExchangeId, NodeJS.Timeout>();
  private readonly kucoinWelcomeTimers = new Map<ExchangeId, NodeJS.Timeout>();
  private readonly kucoinPingSentAt = new Map<ExchangeId, { id: string; sentAt: number }>();
  private readonly kucoinPingConfig = new Map<ExchangeId, { intervalMs: number; timeoutMs: number }>();
  private readonly kucoinLastPingAt = new Map<ExchangeId, number>();
  private readonly lastFeedHeartbeat = new Map<ExchangeId, number>();
  private readonly stopped = new Set<ExchangeId>();
  private readonly reconnectAttempts = new Map<ExchangeId, number>();
  private readonly binanceBuffers = new Map<string, BinanceDelta[]>();
  private readonly binanceSnapshots = new Set<string>();
  private readonly binanceSnapshotCandidates = new Map<string, { lastUpdateId: number; bids?: string[][]; asks?: string[][] }>();
  private readonly binanceSnapshotScheduler = new BinanceSnapshotScheduler();
  private readonly binanceBootstrap = new Map<string, BootstrapSymbol>();
  private binanceBootstrapStartedAt: number | null = null;
  private binanceBootstrapCompletedAt: number | null = null;
  private readonly bybitTopics = new Map<string, LiveMarketSubscription>();
  private readonly okxTopics = new Map<string, LiveMarketSubscription>();
  private readonly okxResubscribing = new Set<string>();
  private readonly kucoinTopics = new Map<string, LiveMarketSubscription>();
  private readonly kucoinBuffers = new Map<string, KucoinDelta[]>();
  private readonly kucoinReady = new Set<string>();
  private readonly kucoinBootstrap = new Map<string, KucoinBootstrap>();
  private readonly kucoinSubscribeRequests = new Map<string, string[]>();
  private readonly kucoinSnapshotGeneration = new Map<string, number>();
  private readonly kucoinSnapshotScheduled = new Map<string, WebSocket>();
  private kucoinSnapshotQueue: Promise<void> = Promise.resolve();
  private readonly mexcTopics = new Map<string, LiveMarketSubscription>();
  private readonly mexcBuffers = new Map<string, MexcDepthDelta[]>();
  private readonly mexcReady = new Set<string>();
  private readonly mexcBootstrap = new Map<string, MexcBootstrap>();
  private readonly mexcSubscribeRequests = new Map<number, string>();
  private readonly mexcSnapshotGeneration = new Map<string, number>();
  private readonly mexcSnapshotCandidates = new Map<string, ReturnType<typeof parseMexcSnapshot>>();
  private readonly mexcSnapshotScheduled = new Map<string, WebSocket>();
  private readonly mexcHeartbeatTimers = new Map<ExchangeId, NodeJS.Timeout>();
  private readonly mexcPingSentAt = new Map<ExchangeId, number>();
  private mexcNextRequestId = 1;
  private mexcSnapshotQueue: Promise<void> = Promise.resolve();
  private readonly gateTopics = new Map<string, LiveMarketSubscription>();
  private readonly gateBuffers = new Map<string, GateBookDelta[]>();
  private readonly gateReady = new Set<string>();
  private readonly gateBootstrap = new Map<string, GateBootstrap>();
  private readonly gateSubscribeRequests = new Map<number, string>();
  private readonly gateSnapshotGeneration = new Map<string, number>();
  private readonly gateSnapshotCandidates = new Map<string, ReturnType<typeof parseGateSnapshot>>();
  private readonly gateSnapshotScheduled = new Map<string, WebSocket>();
  private readonly gateHeartbeatTimers = new Map<ExchangeId, NodeJS.Timeout>();
  private readonly gatePingSentAt = new Map<ExchangeId, number>();
  private gateNextRequestId = 1;
  private gateSnapshotQueue: Promise<void> = Promise.resolve();
  private readonly bitgetTopics = new Map<string, LiveMarketSubscription>();
  private readonly bitgetBuffers = new Map<string, BitgetBookMessage[]>();
  private readonly bitgetReady = new Set<string>();
  private readonly bitgetAwaitingBridge = new Set<string>();
  private readonly bitgetBootstrap = new Map<string, BitgetBootstrap>();
  private readonly bitgetHeartbeatTimers = new Map<ExchangeId, NodeJS.Timeout>();
  private readonly bitgetPingSentAt = new Map<ExchangeId, number>();
  private bitgetConnectionId = 0;
  private bitgetCurrentConnection: BitgetConnectionDiagnostic | null = null;
  private readonly bitgetConnectionHistory: BitgetConnectionDiagnostic[] = [];
  private readonly bitgetAcknowledgedSymbols = new Set<string>();
  private bitgetNextReconnectReason: string | null = null;
  private readonly htxTopics = new Map<string, LiveMarketSubscription>();
  private readonly htxBuffers = new Map<string, HtxBookDelta[]>();
  private readonly htxReady = new Set<string>();
  private readonly htxBootstrap = new Map<string, HtxBootstrap>();
  private readonly htxCandidates = new Map<string, { snapshot: HtxBookSnapshot; generation: number }>();
  private readonly htxRequests = new Map<string, { symbol: string; kind: 'subscribe' | 'refresh'; generation?: number }>();
  private readonly htxSnapshotGeneration = new Map<string, number>();
  private readonly htxSnapshotScheduled = new Map<string, WebSocket>();
  private htxSnapshotQueue: Promise<void> = Promise.resolve();
  private htxLastRefreshAt = 0;
  private htxNextRequestId = 1;
  private readonly cryptoComTopics = new Map<string, LiveMarketSubscription>();
  private readonly cryptoComBootstrap = new Map<string, CryptoComBootstrap>();
  private readonly cryptoComResubscribing = new Set<string>();
  private readonly cryptoComResubscribeRequests = new Map<number, string>();
  private cryptoComNextRequestId = 1;
  private readonly coinbaseTopics = new Map<string, LiveMarketSubscription>();
  private readonly coinbaseProductBySymbol = new Map<string, string>();
  private readonly coinbaseBootstrap = new Map<string, CoinbaseBootstrap>();
  private readonly coinbaseResubscribePending = new Set<string>();
  private readonly coinbaseAwaitingUnsubscribeAck = new Set<string>();
  private coinbaseLastSequence: number | string | null = null;

  constructor(private readonly subscriptions: LiveMarketSubscription[], private readonly coordinator: MarketDataCoordinator) {
    for (const exchange of ['binance', 'bybit', 'okx', 'kucoin', 'mexc', 'gate', 'bitget', 'htx', 'crypto.com', 'coinbase'] as const) {
      const selected = subscriptions.filter((item) => item.exchange === exchange);
      if (selected.length) this.sessions.set(exchange, { exchange, connections: 0, reconnects: 0, errors: [], feedHealthy: false, lastFeedHeartbeatTimestamp: null });
    }
    for (const item of subscriptions.filter((candidate) => candidate.exchange === 'bybit')) {
      this.bybitTopics.set(`orderbook.${process.env.BYBIT_ORDERBOOK_DEPTH ?? '50'}.${item.exchangeSymbol}`, item);
    }
    for (const item of subscriptions.filter((candidate) => candidate.exchange === 'okx')) {
      const instrumentId = mapOkxSpotInstrument(item);
      if (this.okxTopics.has(instrumentId)) throw new Error(`Duplicate OKX Spot instrument ${instrumentId}`);
      this.okxTopics.set(instrumentId, item);
    }
    for (const item of subscriptions.filter((candidate) => candidate.exchange === 'kucoin')) {
      const symbol = mapKucoinSpotSymbol(item);
      if (this.kucoinTopics.has(symbol)) throw new Error(`Duplicate KuCoin Spot symbol ${symbol}`);
      this.kucoinTopics.set(symbol, item);
      this.kucoinBuffers.set(symbol, []);
      this.kucoinBootstrap.set(symbol, { state: 'PENDING', startedAt: 0, completedAt: null, snapshotSequence: null, pendingDeltas: 0, lastFailure: null });
    }
    for (const item of subscriptions.filter((candidate) => candidate.exchange === 'mexc')) {
      const symbol = mapMexcSpotSymbol(item);
      if (this.mexcTopics.has(symbol)) throw new Error(`Duplicate MEXC Spot symbol ${symbol}`);
      this.mexcTopics.set(symbol, item);
      this.mexcBuffers.set(symbol, []);
      this.mexcBootstrap.set(symbol, { state: 'PENDING', startedAt: 0, completedAt: null, snapshotVersion: null, pendingDeltas: 0, lastFailure: null });
    }
    for (const item of subscriptions.filter((candidate) => candidate.exchange === 'gate')) {
      const symbol = mapGateSpotSymbol(item);
      if (this.gateTopics.has(symbol)) throw new Error(`Duplicate Gate Spot symbol ${symbol}`);
      this.gateTopics.set(symbol, item);
      this.gateBuffers.set(symbol, []);
      this.gateBootstrap.set(symbol, { state: 'PENDING', startedAt: 0, completedAt: null, snapshotId: null, pendingDeltas: 0, lastFailure: null });
    }
    for (const item of subscriptions.filter((candidate) => candidate.exchange === 'bitget')) {
      const symbol = mapBitgetSpotSymbol(item);
      if (this.bitgetTopics.has(symbol)) throw new Error(`Duplicate Bitget Spot symbol ${symbol}`);
      this.bitgetTopics.set(symbol, item);
      this.bitgetBuffers.set(symbol, []);
      this.bitgetBootstrap.set(symbol, { state: 'PENDING', startedAt: 0, completedAt: null, sequence: null, pendingDeltas: 0, lastFailure: null });
    }
    for (const item of subscriptions.filter((candidate) => candidate.exchange === 'htx')) {
      const symbol = mapHtxSpotSymbol(item);
      if (this.htxTopics.has(symbol)) throw new Error(`Duplicate HTX Spot symbol ${symbol}`);
      this.htxTopics.set(symbol, item);
      this.htxBuffers.set(symbol, []);
      this.htxBootstrap.set(symbol, { state: 'PENDING', startedAt: 0, completedAt: null, sequence: null, pendingDeltas: 0, lastFailure: null });
    }
    for (const item of subscriptions.filter((candidate) => candidate.exchange === 'crypto.com')) {
      const instrument = mapCryptoComSpotInstrument(item);
      if (this.cryptoComTopics.has(instrument)) throw new Error(`Duplicate Crypto.com Spot instrument ${instrument}`);
      this.cryptoComTopics.set(instrument, item);
      this.cryptoComBootstrap.set(instrument, { state: 'PENDING', startedAt: 0, completedAt: null, sequence: null, lastFailure: null });
    }
    for (const item of subscriptions.filter((candidate) => candidate.exchange === 'coinbase')) {
      const product = mapCoinbaseSpotProduct(item);
      if (this.coinbaseTopics.has(product)) throw new Error(`Duplicate Coinbase Spot product ${product}`);
      this.coinbaseTopics.set(product, item);
      this.coinbaseProductBySymbol.set(item.exchangeSymbol.toUpperCase(), product);
      this.coinbaseBootstrap.set(product, { state: 'PENDING', startedAt: 0, completedAt: null, localSequence: '0',
        exchangeSequence: null, sequenceSource: 'LOCAL_ORDER', lastFailure: null });
    }
  }

  start() { for (const exchange of this.sessions.keys()) this.connect(exchange); }
  interrupt(exchange: ExchangeId): boolean {
    const socket = this.sockets.get(exchange);
    if (!socket) return false;
    socket.close(4000, 'controlled reconnect validation');
    return true;
  }
  stop() {
    for (const exchange of this.sessions.keys()) this.stopped.add(exchange);
    for (const timer of this.timers.values()) clearTimeout(timer);
    this.timers.clear();
    for (const timer of this.feedHealthTimers.values()) clearInterval(timer);
    this.feedHealthTimers.clear();
    for (const timer of this.okxHeartbeatTimers.values()) clearInterval(timer);
    this.okxHeartbeatTimers.clear();
    for (const timer of this.kucoinHeartbeatTimers.values()) clearInterval(timer);
    this.kucoinHeartbeatTimers.clear();
    for (const timer of this.kucoinWelcomeTimers.values()) clearTimeout(timer);
    this.kucoinWelcomeTimers.clear();
    for (const timer of this.mexcHeartbeatTimers.values()) clearInterval(timer);
    this.mexcHeartbeatTimers.clear();
    for (const timer of this.gateHeartbeatTimers.values()) clearInterval(timer);
    this.gateHeartbeatTimers.clear();
    for (const timer of this.bitgetHeartbeatTimers.values()) clearInterval(timer);
    this.bitgetHeartbeatTimers.clear(); this.bitgetPingSentAt.clear();
    this.cryptoComResubscribeRequests.clear(); this.cryptoComResubscribing.clear();
    this.coinbaseResubscribePending.clear(); this.coinbaseAwaitingUnsubscribeAck.clear();
    this.htxRequests.clear();
    for (const socket of this.sockets.values()) socket.close(1000, 'runtime shutdown');
    this.sockets.clear();
  }

  snapshot() { return [...this.sessions.values()].map((session) => ({ ...session, errors: [...session.errors],
    ...(session.exchange === 'bitget' ? { bitgetConnections: this.bitgetConnectionDiagnostics() } : {}) })); }
  bitgetConnectionDiagnostics() {
    return { current: this.bitgetCurrentConnection, history: this.bitgetConnectionHistory.filter((item) => item.closedAt !== null) };
  }
  binanceSnapshotTelemetry() {
    const scheduler = this.binanceSnapshotScheduler.telemetry();
    const symbols = Object.fromEntries(this.subscriptions.filter((item) => item.exchange === 'binance').map((item) => {
      const symbol = item.exchangeSymbol.toUpperCase();
      const state = this.binanceBootstrap.get(symbol);
      const perSymbol = (scheduler.perSymbol as Record<string, Record<string, unknown>>)[symbol] ?? {};
      return [symbol, { ...perSymbol, state: state?.state ?? (perSymbol.state as string | undefined) ?? 'PENDING',
        schedulerState: perSymbol.state ?? 'PENDING', startedAt: state?.startedAt ?? null,
        completedAt: state?.completedAt ?? null, lastFailure: state?.lastFailure ?? perSymbol.lastError ?? null }];
    }));
    const values = [...this.binanceBootstrap.values()];
    return { ...scheduler, bootstrapStartedAt: this.binanceBootstrapStartedAt,
      bootstrapCompletedAt: this.binanceBootstrapCompletedAt,
      bootstrapDurationMs: this.binanceBootstrapStartedAt !== null && this.binanceBootstrapCompletedAt !== null
        ? this.binanceBootstrapCompletedAt - this.binanceBootstrapStartedAt : null,
      pairsRequested: this.subscriptions.filter((item) => item.exchange === 'binance').length,
      snapshotsCompleted: values.filter((state) => state.state === 'SYNCHRONIZED').length,
      pendingSymbols: values.filter((state) => state.state !== 'SYNCHRONIZED' && state.state !== 'FAILED').length,
      failedSymbols: values.filter((state) => state.state === 'FAILED').length, symbols };
  }
  symbolBootstrapState(symbol: string) { return this.binanceBootstrap.get(symbol.toUpperCase()) ?? null; }
  kucoinSnapshotTelemetry() {
    const symbols = Object.fromEntries([...this.kucoinBootstrap].map(([symbol, state]) => [symbol, { ...state,
      synchronized: this.kucoinReady.has(symbol), pendingDeltas: this.kucoinBuffers.get(symbol)?.length ?? 0 }]));
    const values = [...this.kucoinBootstrap.values()];
    return { pairsRequested: values.length, snapshotsCompleted: values.filter((state) => state.state === 'SYNCHRONIZED').length,
      pendingSymbols: values.filter((state) => ['PENDING', 'SUBSCRIBING', 'SNAPSHOTTING', 'SYNCHRONIZING', 'RESYNCHRONIZING'].includes(state.state)).length,
      failedSymbols: values.filter((state) => state.state === 'FAILED').length, symbols };
  }
  mexcSnapshotTelemetry() {
    const symbols = Object.fromEntries([...this.mexcBootstrap].map(([symbol, state]) => [symbol, { ...state,
      synchronized: this.mexcReady.has(symbol), pendingDeltas: this.mexcBuffers.get(symbol)?.length ?? 0 }]));
    const values = [...this.mexcBootstrap.values()];
    return { pairsRequested: values.length, snapshotsCompleted: values.filter((state) => state.state === 'SYNCHRONIZED').length,
      pendingSymbols: values.filter((state) => ['PENDING', 'SUBSCRIBING', 'SNAPSHOTTING', 'SYNCHRONIZING', 'RESYNCHRONIZING'].includes(state.state)).length,
      failedSymbols: values.filter((state) => state.state === 'FAILED').length, symbols };
  }
  gateSnapshotTelemetry() {
    const symbols = Object.fromEntries([...this.gateBootstrap].map(([symbol, state]) => [symbol, { ...state,
      synchronized: this.gateReady.has(symbol), pendingDeltas: this.gateBuffers.get(symbol)?.length ?? 0 }]));
    const values = [...this.gateBootstrap.values()];
    return { pairsRequested: values.length, snapshotsCompleted: values.filter((state) => state.state === 'SYNCHRONIZED').length,
      pendingSymbols: values.filter((state) => ['PENDING', 'SUBSCRIBING', 'SNAPSHOTTING', 'SYNCHRONIZING', 'RESYNCHRONIZING'].includes(state.state)).length,
      failedSymbols: values.filter((state) => state.state === 'FAILED').length, symbols };
  }
  bitgetSnapshotTelemetry() {
    const symbols = Object.fromEntries([...this.bitgetBootstrap].map(([symbol, state]) => [symbol, { ...state,
      synchronized: this.bitgetReady.has(symbol), pendingDeltas: this.bitgetBuffers.get(symbol)?.length ?? 0 }]));
    const values = [...this.bitgetBootstrap.values()];
    return { pairsRequested: values.length, snapshotsCompleted: values.filter((state) => state.state === 'SYNCHRONIZED').length,
      pendingSymbols: values.filter((state) => ['PENDING', 'SUBSCRIBING', 'SYNCHRONIZING', 'RESYNCHRONIZING'].includes(state.state)).length,
      failedSymbols: values.filter((state) => state.state === 'FAILED').length, symbols };
  }
  htxSnapshotTelemetry() {
    const symbols = Object.fromEntries([...this.htxBootstrap].map(([symbol, state]) => [symbol, { ...state,
      synchronized: this.htxReady.has(symbol), pendingDeltas: this.htxBuffers.get(symbol)?.length ?? 0 }]));
    const values = [...this.htxBootstrap.values()];
    return { pairsRequested: values.length, snapshotsCompleted: values.filter((state) => state.state === 'SYNCHRONIZED').length,
      pendingSymbols: values.filter((state) => ['PENDING', 'SUBSCRIBING', 'SNAPSHOTTING', 'SYNCHRONIZING', 'RESYNCHRONIZING'].includes(state.state)).length,
      failedSymbols: values.filter((state) => state.state === 'FAILED').length, symbols };
  }
  cryptoComSnapshotTelemetry() {
    const symbols = Object.fromEntries([...this.cryptoComBootstrap].map(([symbol, state]) => [symbol, { ...state,
      synchronized: this.coordinator.getBook('crypto.com', this.cryptoComTopics.get(symbol)!.exchangeSymbol).synchronized }]));
    const values = [...this.cryptoComBootstrap.values()];
    return { pairsRequested: values.length, snapshotsCompleted: values.filter((state) => state.state === 'SYNCHRONIZED').length,
      pendingSymbols: values.filter((state) => ['PENDING', 'SUBSCRIBING', 'RESYNCHRONIZING'].includes(state.state)).length,
      failedSymbols: values.filter((state) => state.state === 'FAILED').length, symbols };
  }
  coinbaseSnapshotTelemetry() {
    const symbols = Object.fromEntries([...this.coinbaseBootstrap].map(([product, state]) => [product, { ...state,
      synchronized: this.coordinator.getBook('coinbase', this.coinbaseTopics.get(product)!.exchangeSymbol).synchronized }]));
    const values = [...this.coinbaseBootstrap.values()];
    return { pairsRequested: values.length, snapshotsCompleted: values.filter((state) => state.state === 'SYNCHRONIZED').length,
      pendingSymbols: values.filter((state) => ['PENDING', 'SUBSCRIBING', 'RESYNCHRONIZING'].includes(state.state)).length,
      failedSymbols: values.filter((state) => state.state === 'FAILED').length,
      sequenceValidation: 'ADVANCED_TRADE_L2_DATA_SEQUENCE_NUM; GAP_INVALIDATES_ALL_BOOKS_AND_RECONNECTS',
      lastEnvelopeSequence: this.coinbaseLastSequence, symbols };
  }
  coinbaseSequenceState(exchangeSymbol: string) {
    const product = this.coinbaseProductBySymbol.get(exchangeSymbol.toUpperCase());
    const state = product ? this.coinbaseBootstrap.get(product) : undefined;
    return state ? { source: state.sequenceSource, exchangeSequence: state.exchangeSequence } : null;
  }
  connectionCount() { return [...this.sessions.values()].reduce((sum, session) => sum + session.connections, 0); }

  private connect(exchange: ExchangeId) {
    if (this.stopped.has(exchange) || this.sockets.has(exchange)) return;
    const subs = this.subscriptions.filter((item) => item.exchange === exchange);
    this.coordinator.setConnection(exchange, 'STARTING', this.sessions.get(exchange)?.reconnects ?? 0);
    if (exchange === 'binance') this.connectBinance(subs);
    else if (exchange === 'bybit') this.connectBybit(subs);
    else if (exchange === 'okx') this.connectOkx(subs);
    else if (exchange === 'kucoin') void this.connectKucoin(subs);
    else if (exchange === 'mexc') this.connectMexc(subs);
    else if (exchange === 'gate') this.connectGate(subs);
    else if (exchange === 'bitget') this.connectBitget(subs);
    else if (exchange === 'htx') this.connectHtx(subs);
    else if (exchange === 'crypto.com') this.connectCryptoCom(subs);
    else if (exchange === 'coinbase') this.connectCoinbase(subs);
  }

  private connectCoinbase(subs: LiveMarketSubscription[]) {
    const ws = new WebSocket(process.env.COINBASE_WS_URL ?? COINBASE_SPOT_WS_URL);
    this.sockets.set('coinbase', ws);
    ws.on('open', () => {
      this.connected('coinbase');
      this.startFeedHealthMonitor('coinbase', ws);
      for (const item of subs) {
        const product = mapCoinbaseSpotProduct(item), state = this.coinbaseBootstrap.get(product)!;
        state.state = 'SUBSCRIBING'; state.startedAt = Date.now(); state.completedAt = null;
        state.exchangeSequence = null; state.sequenceSource = 'LOCAL_ORDER'; state.lastFailure = null;
      }
      const products = subs.map(mapCoinbaseSpotProduct);
      this.coinbaseLastSequence = null;
      chunks(products, COINBASE_PRODUCT_BATCH_SIZE).forEach((batch, index) => {
        const send = () => { if (this.sockets.get('coinbase') === ws && ws.readyState === WebSocket.OPEN) this.sendCoinbaseSubscribe(ws, batch, 'level2'); };
        if (index === 0) send(); else setTimeout(send, index * 100);
      });
    });
    ws.on('message', (raw) => {
      try {
        const frame = parseCoinbaseFrame(raw.toString());
        if (frame.kind === 'ignore') return;
        if (frame.kind === 'error') throw new Error(`Coinbase WebSocket error: ${frame.message}`);
        const envelopeSequence = frame.sequence;
        if (envelopeSequence !== null && envelopeSequence !== undefined) {
          const disposition = classifyCoinbaseSequence(this.coinbaseLastSequence, envelopeSequence);
          if (disposition === 'STALE') return;
          if (disposition === 'GAP') {
            const expected = (BigInt(this.coinbaseLastSequence!) + 1n).toString();
            const reason = `Coinbase WebSocket envelope sequence gap: expected ${expected}, observed ${envelopeSequence}; reconnecting all products for fresh snapshots`;
            for (const item of this.subscriptions.filter((candidate) => candidate.exchange === 'coinbase')) {
              this.coordinator.markSequenceGap('coinbase', item.exchangeSymbol, expected, envelopeSequence, reason);
            }
            throw new Error(reason);
          }
          this.coinbaseLastSequence = envelopeSequence;
        }
        if (frame.kind === 'subscriptions') {
          this.touchFeed('coinbase');
          if (this.coinbaseAwaitingUnsubscribeAck.size) {
            const products = [...this.coinbaseAwaitingUnsubscribeAck];
            this.coinbaseAwaitingUnsubscribeAck.clear();
            for (const product of products) {
              const item = this.coinbaseTopics.get(product);
              if (item && this.coinbaseResubscribePending.has(product)) this.sendCoinbaseSubscribe(ws, [product], 'level2');
            }
          }
          return;
        }
        if (frame.kind === 'heartbeat') { this.touchFeed('coinbase'); return; }
        const bookFrames = frame.kind === 'batch' ? frame.frames : [frame];
        for (const bookFrame of bookFrames) this.applyCoinbaseBookFrame(bookFrame, ws);
      } catch (error) { this.fail('coinbase', error); }
    });
    this.attachClose('coinbase', ws);
  }

  private applyCoinbaseBookFrame(frame: import('./coinbase-protocol').CoinbaseBookFrame, ws: WebSocket) {
        const item = this.coinbaseTopics.get(frame.productId);
        if (!item) throw new Error(`Coinbase book data for unsubscribed Spot product ${frame.productId}`);
        this.recordMessage('coinbase', item.exchangeSymbol);
        const state = this.coinbaseBootstrap.get(frame.productId)!;
        const receivedTimestamp = Date.now();
        const exchangeTimestamp = frame.exchangeTimestamp ?? receivedTimestamp;
        if (frame.type === 'snapshot') {
          state.localSequence = (BigInt(state.localSequence) + 1n).toString();
          if (!this.coordinator.applySnapshot('coinbase', item.exchangeSymbol, { bids: frame.bids, asks: frame.asks,
            sequence: state.localSequence, exchangeTimestamp, receivedTimestamp })) {
            throw new Error(`Coordinator rejected Coinbase level2 snapshot for ${frame.productId}`);
          }
          state.exchangeSequence = null; state.sequenceSource = 'LOCAL_ORDER';
          state.state = 'SYNCHRONIZED'; state.completedAt = receivedTimestamp; state.lastFailure = null;
          this.coinbaseResubscribePending.delete(frame.productId);
          return;
        }
        state.localSequence = (BigInt(state.localSequence) + 1n).toString();
        if (!this.coordinator.applyDelta('coinbase', item.exchangeSymbol, { firstUpdateId: state.localSequence,
          finalUpdateId: state.localSequence, bids: frame.bids, asks: frame.asks, exchangeTimestamp })) {
          state.state = 'RESYNCHRONIZING'; state.lastFailure = 'Coinbase ordered level2 delta rejected';
          this.coordinator.markResynchronizing('coinbase', item.exchangeSymbol, state.lastFailure);
          this.resubscribeCoinbase(ws, frame.productId);
          return;
        }
  }

  private sendCoinbaseSubscribe(ws: WebSocket, products: string[], channel: string) {
    if (ws.readyState !== WebSocket.OPEN || products.length === 0) return;
    ws.send(JSON.stringify({ type: 'subscribe', product_ids: products, channel }));
  }

  private resubscribeCoinbase(ws: WebSocket, product: string) {
    if (this.coinbaseResubscribePending.has(product) || ws.readyState !== WebSocket.OPEN) return;
    this.coinbaseResubscribePending.add(product); this.coinbaseAwaitingUnsubscribeAck.add(product);
    ws.send(JSON.stringify({ type: 'unsubscribe', product_ids: [product], channel: 'level2' }));
  }

  private connectCryptoCom(subs: LiveMarketSubscription[]) {
    const ws = new WebSocket(process.env.CRYPTO_COM_WS_URL ?? CRYPTO_COM_SPOT_WS_URL);
    this.sockets.set('crypto.com', ws);
    ws.on('open', () => {
      this.connected('crypto.com');
      this.startFeedHealthMonitor('crypto.com', ws);
      const depth = positiveInt(process.env.CRYPTO_COM_BOOK_DEPTH, CRYPTO_COM_BOOK_DEPTH);
      const channels = subs.map((item) => `book.${mapCryptoComSpotInstrument(item)}.${depth}`);
      chunks(channels, 50).forEach((batch, index) => {
        const send = () => {
          if (this.sockets.get('crypto.com') !== ws || ws.readyState !== WebSocket.OPEN) return;
          ws.send(JSON.stringify({ id: this.cryptoComNextRequestId++, method: 'subscribe', nonce: Date.now(), params: {
            channels: batch, book_subscription_type: 'SNAPSHOT_AND_UPDATE', book_update_frequency: 100,
          } }));
          for (const channel of batch) {
            const symbol = channel.split('.')[1]; const state = this.cryptoComBootstrap.get(symbol);
            if (state) { state.state = 'SUBSCRIBING'; state.startedAt = Date.now(); state.completedAt = null; state.lastFailure = null; }
          }
        };
        if (index === 0) send(); else setTimeout(send, index * 100);
      });
    });
    ws.on('message', (raw) => {
      try {
        const frame = parseCryptoComFrame(raw.toString());
        if (frame.kind === 'ignore') return;
        if (frame.kind === 'heartbeat') {
          this.touchFeed('crypto.com');
          ws.send(JSON.stringify({ id: frame.id, method: 'public/respond-heartbeat' }));
          return;
        }
        if (frame.kind === 'ack') {
          this.touchFeed('crypto.com');
          if (frame.code !== 0) throw new Error(`Crypto.com ${frame.method ?? 'WebSocket'} rejected (${frame.code}): ${frame.message ?? 'unknown error'}`);
          const symbol = frame.id === undefined ? undefined : this.cryptoComResubscribeRequests.get(Number(frame.id));
          if (symbol) {
            this.cryptoComResubscribeRequests.delete(Number(frame.id));
            const item = this.cryptoComTopics.get(symbol);
            if (item) this.sendCryptoComSubscription(ws, item);
          }
          return;
        }
        const item = this.cryptoComTopics.get(frame.instrumentName);
        if (!item) throw new Error(`Crypto.com book update for unsubscribed Spot instrument ${frame.instrumentName}`);
        this.recordMessage('crypto.com', item.exchangeSymbol);
        const state = this.cryptoComBootstrap.get(frame.instrumentName)!;
        if (frame.channel === 'book') {
          const previous = this.coordinator.sequence('crypto.com', item.exchangeSymbol);
          if (previous !== null && BigInt(frame.sequence) <= BigInt(previous)) return;
          if (!this.coordinator.applySnapshot('crypto.com', item.exchangeSymbol, { bids: frame.bids!, asks: frame.asks!,
            sequence: frame.sequence, exchangeTimestamp: frame.exchangeTimestamp, receivedTimestamp: Date.now() })) {
            throw new Error(`Coordinator rejected Crypto.com snapshot for ${frame.instrumentName}`);
          }
          state.state = 'SYNCHRONIZED'; state.sequence = frame.sequence; state.completedAt = Date.now(); state.lastFailure = null;
          this.cryptoComResubscribing.delete(frame.instrumentName);
          return;
        }
        const current = this.coordinator.sequence('crypto.com', item.exchangeSymbol);
        if (current === null) {
          this.coordinator.markResynchronizing('crypto.com', item.exchangeSymbol, 'delta received before snapshot');
          this.resubscribeCryptoCom(ws, frame.instrumentName);
          return;
        }
        const disposition = classifyCryptoComUpdate(current, frame);
        if (disposition === 'STALE') return;
        if (disposition === 'GAP') {
          const reason = `Crypto.com sequence gap for ${frame.instrumentName}: previous ${frame.previousSequence}, current ${current}, observed ${frame.sequence}`;
          this.coordinator.markSequenceGap('crypto.com', item.exchangeSymbol, current, frame.sequence, reason);
          state.state = 'RESYNCHRONIZING'; state.lastFailure = reason;
          this.resubscribeCryptoCom(ws, frame.instrumentName);
          return;
        }
        if (!this.coordinator.applyDelta('crypto.com', item.exchangeSymbol, {
          firstUpdateId: (BigInt(current) + 1n).toString(), finalUpdateId: frame.sequence,
          bids: frame.bids!, asks: frame.asks!, exchangeTimestamp: frame.exchangeTimestamp,
        })) {
          state.state = 'RESYNCHRONIZING'; state.lastFailure = 'coordinator rejected sequenced delta';
          this.resubscribeCryptoCom(ws, frame.instrumentName);
          return;
        }
        state.sequence = frame.sequence;
      } catch (error) { this.fail('crypto.com', error); }
    });
    this.attachClose('crypto.com', ws);
  }

  private sendCryptoComSubscription(ws: WebSocket, item: LiveMarketSubscription) {
    if (ws.readyState !== WebSocket.OPEN) return;
    const instrument = mapCryptoComSpotInstrument(item);
    const depth = positiveInt(process.env.CRYPTO_COM_BOOK_DEPTH, CRYPTO_COM_BOOK_DEPTH);
    ws.send(JSON.stringify({ id: this.cryptoComNextRequestId++, method: 'subscribe', nonce: Date.now(), params: {
      channels: [`book.${instrument}.${depth}`], book_subscription_type: 'SNAPSHOT_AND_UPDATE', book_update_frequency: 100,
    } }));
  }

  private resubscribeCryptoCom(ws: WebSocket, instrument: string) {
    if (this.cryptoComResubscribing.has(instrument) || ws.readyState !== WebSocket.OPEN) return;
    this.cryptoComResubscribing.add(instrument);
    const item = this.cryptoComTopics.get(instrument);
    if (!item) return;
    const id = this.cryptoComNextRequestId++;
    this.cryptoComResubscribeRequests.set(id, instrument);
    const depth = positiveInt(process.env.CRYPTO_COM_BOOK_DEPTH, CRYPTO_COM_BOOK_DEPTH);
    ws.send(JSON.stringify({ id, method: 'unsubscribe', nonce: Date.now(), params: { channels: [`book.${instrument}.${depth}`] } }));
  }

  private connectBinance(subs: LiveMarketSubscription[]) {
    this.binanceSnapshots.clear();
    this.binanceSnapshotCandidates.clear();
    this.binanceBootstrapStartedAt = Date.now();
    this.binanceBootstrapCompletedAt = null;
    const streams = subs.map((item) => `${item.exchangeSymbol.toLowerCase()}@depth@100ms`);
    const url = process.env.BINANCE_WS_URL ?? `wss://stream.binance.com:9443/stream?streams=${streams.join('/')}`;
    const ws = new WebSocket(url);
    this.sockets.set('binance', ws);
    ws.on('open', () => {
      this.connected('binance');
      this.startFeedHealthMonitor('binance', ws);
      for (const item of subs) {
        this.binanceBuffers.set(item.exchangeSymbol.toUpperCase(), []);
        const symbol = item.exchangeSymbol.toUpperCase();
        this.binanceBootstrap.set(symbol, { state: 'QUEUED', startedAt: Date.now(), completedAt: null, lastFailure: null });
      }
      void this.loadBinanceSnapshots(subs);
    });
    ws.on('ping', () => this.touchFeed('binance'));
    ws.on('message', (raw) => {
      try {
        const envelope = JSON.parse(raw.toString()) as { stream?: string; data?: BinanceDelta };
        const data = envelope.data;
        if (!data || typeof data.U !== 'number' || typeof data.u !== 'number') throw new Error('unexpected Binance depth message');
        const symbol = envelope.stream?.split('@')[0]?.toUpperCase();
        if (!symbol) throw new Error('combined Binance message missing stream name');
        this.recordMessage('binance', symbol);
        const queue = this.binanceBuffers.get(symbol);
        if (!this.binanceSnapshots.has(symbol)) {
          if (queue) {
            queue.push(data);
            const cap = positiveInt(process.env.BINANCE_SNAPSHOT_BUFFER_MAX_EVENTS, 10_000);
            if (queue.length > cap) {
              queue.splice(0, queue.length - cap);
              this.binanceSnapshotCandidates.delete(symbol);
              this.coordinator.markResynchronizing('binance', symbol, `snapshot buffer exceeded ${cap} events`);
            }
          }
          if (this.binanceSnapshotCandidates.has(symbol)) this.reconcileBinanceSnapshot(symbol, this.binanceSnapshotCandidates.get(symbol)!);
          return;
        }
        this.applyBinanceDelta(symbol, data);
      } catch (error) { this.fail('binance', error); }
    });
    this.attachClose('binance', ws);
  }

  private async loadBinanceSnapshot(item: LiveMarketSubscription) {
    const symbol = item.exchangeSymbol.toUpperCase();
    const state = this.binanceBootstrap.get(symbol) ?? { state: 'QUEUED', startedAt: Date.now(), completedAt: null, lastFailure: null };
    state.state = 'QUEUED';
    this.binanceBootstrap.set(symbol, state);
    try {
      if (this.stopped.has('binance')) return;
      const baseUrl = (process.env.BINANCE_REST_BASE_URL ?? 'https://data-api.binance.vision').replace(/\/$/, '');
      const response = await this.binanceSnapshotScheduler.request(symbol, async () => fetch(
        `${baseUrl}/api/v3/depth?symbol=${symbol}&limit=100`, { signal: AbortSignal.timeout(12_000) },
      ) as Promise<SnapshotResponse>);
      const snapshot = await response.json() as { lastUpdateId?: unknown; bids?: string[][]; asks?: string[][] };
      if (typeof snapshot.lastUpdateId !== 'number') throw new Error(`Binance depth snapshot missing lastUpdateId for ${symbol}`);
      if (this.stopped.has('binance')) return;
      const candidate = { ...snapshot, lastUpdateId: snapshot.lastUpdateId };
      this.binanceSnapshotCandidates.set(symbol, candidate);
      state.state = 'SYNCHRONIZING';
      state.lastFailure = null;
      this.reconcileBinanceSnapshot(symbol, candidate);
    } catch (error) {
      if (this.stopped.has('binance')) return;
      state.state = 'FAILED';
      state.lastFailure = message(error);
      this.fail('binance', error);
      this.coordinator.markResynchronizing('binance', symbol, message(error));
    }
  }

  private loadBinanceSnapshots(subscriptions: LiveMarketSubscription[]) {
    for (const item of subscriptions) void this.loadBinanceSnapshot(item);
  }

  private reconcileBinanceSnapshot(symbol: string, snapshot: { lastUpdateId: number; bids?: string[][]; asks?: string[][] }) {
    if (this.binanceSnapshots.has(symbol)) return;
    const queue = this.binanceBuffers.get(symbol) ?? [];
    const firstBuffered = queue[0];
    if (!firstBuffered) return;
    // Binance's documented bootstrap says a snapshot behind the first buffered U is obsolete.
    if (snapshot.lastUpdateId < firstBuffered.U - 1) {
      this.binanceSnapshotCandidates.delete(symbol);
      const subscription = this.subscriptions.find((item) => item.exchange === 'binance' && item.exchangeSymbol.toUpperCase() === symbol);
      if (subscription) void this.loadBinanceSnapshot(subscription);
      return;
    }
    let bridgeIndex = -1;
    for (let index = 0; index < queue.length; index += 1) {
      const event = queue[index];
      if (event.u <= snapshot.lastUpdateId) continue;
      if (event.U > snapshot.lastUpdateId + 1) {
        this.binanceSnapshotCandidates.delete(symbol);
        this.coordinator.markResynchronizing('binance', symbol, `bootstrap sequence gap ${event.U}-${event.u} after snapshot ${snapshot.lastUpdateId}`);
        const subscription = this.subscriptions.find((item) => item.exchange === 'binance' && item.exchangeSymbol.toUpperCase() === symbol);
        if (subscription) void this.loadBinanceSnapshot(subscription);
        return;
      }
      bridgeIndex = index;
      break;
    }
    if (bridgeIndex < 0) return; // Keep the candidate and buffered deltas; a later event may bridge it.
    const timestamp = queue[bridgeIndex].E ?? Date.now();
    if (!this.coordinator.applySnapshot('binance', symbol, {
      bids: levels(snapshot.bids), asks: levels(snapshot.asks), sequence: snapshot.lastUpdateId,
      exchangeTimestamp: timestamp, receivedTimestamp: Date.now(),
    })) throw new Error(`Coordinator rejected Binance snapshot for ${symbol}`);
    this.binanceSnapshots.add(symbol);
    for (const event of queue.slice(bridgeIndex)) {
      if (!this.applyBinanceDelta(symbol, event)) {
        this.binanceSnapshots.delete(symbol);
        return;
      }
    }
    this.binanceBuffers.set(symbol, []);
    this.binanceSnapshotCandidates.delete(symbol);
    const bootstrap = this.binanceBootstrap.get(symbol);
    if (bootstrap) { bootstrap.state = 'SYNCHRONIZED'; bootstrap.completedAt = Date.now(); bootstrap.lastFailure = null; }
    if (this.binanceSnapshots.size === this.subscriptions.filter((item) => item.exchange === 'binance').length) {
      this.binanceBootstrapCompletedAt ??= Date.now();
    }
  }

  private applyBinanceDelta(symbol: string, event: BinanceDelta): boolean {
    const book = this.coordinator.getBook('binance', symbol);
    if (book.sequence === null) return false;
    const next = Number(book.sequence);
    if (event.u <= next) return true;
    if (event.U > next + 1) {
      this.coordinator.markResynchronizing('binance', symbol, `sequence gap ${event.U}-${event.u} after ${next}`);
      this.binanceSnapshots.delete(symbol);
      this.binanceBuffers.set(symbol, []);
      this.binanceSnapshotCandidates.delete(symbol);
      const bootstrap = this.binanceBootstrap.get(symbol);
      if (bootstrap) bootstrap.state = 'RESYNCHRONIZING';
      const subscription = this.subscriptions.find((item) => item.exchange === 'binance' && item.exchangeSymbol.toUpperCase() === symbol);
      if (subscription) void this.loadBinanceSnapshot(subscription);
      return false;
    }
    return this.coordinator.applyDelta('binance', symbol, {
      firstUpdateId: event.U, finalUpdateId: event.u, bids: levels(event.b), asks: levels(event.a),
      exchangeTimestamp: event.E ?? Date.now(),
    });
  }

  private connectBybit(subs: LiveMarketSubscription[]) {
    const ws = new WebSocket(process.env.BYBIT_WS_URL ?? 'wss://stream.bybit.com/v5/public/spot');
    this.sockets.set('bybit', ws);
    ws.on('open', () => {
      this.connected('bybit');
      this.startFeedHealthMonitor('bybit', ws);
      const topics = subs.map((item) => `orderbook.${process.env.BYBIT_ORDERBOOK_DEPTH ?? '50'}.${item.exchangeSymbol}`);
      const batches = chunks(topics, 10);
      batches.forEach((args, index) => {
        const send = () => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ op: 'subscribe', args })); };
        if (index === 0) send();
        else setTimeout(send, index * 100);
      });
      const heartbeat = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ op: 'ping' }));
      }, 20_000);
      ws.once('close', () => clearInterval(heartbeat));
    });
    ws.on('message', (raw) => {
      try {
        const message = JSON.parse(raw.toString()) as any;
        if (message.op === 'ping' || message.op === 'pong') { this.touchFeed('bybit'); return; }
        if (message.op === 'subscribe') {
          if (!message.success) throw new Error(message.ret_msg ?? `Bybit subscription rejected (${message.retCode})`);
          this.touchFeed('bybit');
          return;
        }
        if (typeof message.topic !== 'string' || !message.data) return;
        const item = this.bybitTopics.get(message.topic);
        if (!item) return;
        const data = message.data;
        if (!Array.isArray(data.b) || !Array.isArray(data.a) || !Number.isSafeInteger(data.u)) throw new Error(`invalid Bybit orderbook payload for ${item.exchangeSymbol}`);
        const receivedTimestamp = Date.now();
        const exchangeTimestamp = Number(message.cts ?? message.ts);
        const time = Number.isFinite(exchangeTimestamp) && exchangeTimestamp > 0 ? exchangeTimestamp : receivedTimestamp;
        this.recordMessage('bybit', item.exchangeSymbol);
        if (message.type === 'snapshot') {
          if (!this.coordinator.applySnapshot('bybit', item.exchangeSymbol, {
            bids: levels(data.b), asks: levels(data.a), sequence: data.u,
            exchangeTimestamp: time, receivedTimestamp,
          })) throw new Error(`Coordinator rejected Bybit snapshot for ${item.exchangeSymbol}`);
          return;
        }
        if (message.type !== 'delta') throw new Error(`unsupported Bybit book message type ${message.type}`);
        const current = this.coordinator.getBook('bybit', item.exchangeSymbol);
        if (current.sequence === null) { this.coordinator.markResynchronizing('bybit', item.exchangeSymbol, 'delta received before snapshot'); return; }
        if (data.u <= Number(current.sequence)) return;
        // Bybit u is an increasing update ID, not a contiguous range. Preserve ordering without imposing Binance U/u rules.
        if (!this.coordinator.applyDelta('bybit', item.exchangeSymbol, {
          firstUpdateId: Number(current.sequence) + 1, finalUpdateId: data.u,
          bids: levels(data.b), asks: levels(data.a), exchangeTimestamp: time,
        })) this.coordinator.markResynchronizing('bybit', item.exchangeSymbol, `Bybit update rejected (u=${data.u})`);
      } catch (error) { this.fail('bybit', error); }
    });
    this.attachClose('bybit', ws);
  }

  private connectOkx(subs: LiveMarketSubscription[]) {
    const ws = new WebSocket(process.env.OKX_WS_URL ?? OKX_PUBLIC_SPOT_WS_URL);
    this.sockets.set('okx', ws);
    ws.on('open', () => {
      this.connected('okx');
      this.startFeedHealthMonitor('okx', ws);
      const args = subs.map((item) => ({ channel: 'books', instId: mapOkxSpotInstrument(item) }));
      chunks(args, 50).forEach((batch, index) => {
        const send = () => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ op: 'subscribe', args: batch })); };
        if (index === 0) send(); else setTimeout(send, index * 100);
      });
      const heartbeat = setInterval(() => {
        if (this.sockets.get('okx') !== ws || ws.readyState !== WebSocket.OPEN) return;
        const now = Date.now(), pingSentAt = this.okxPingSentAt.get('okx');
        if (pingSentAt !== undefined) {
          if (now - pingSentAt > 10_000) this.fail('okx', new Error('OKX WebSocket heartbeat pong timeout'));
          return;
        }
        if (now - (this.lastFeedHeartbeat.get('okx') ?? now) >= 20_000) {
          this.okxPingSentAt.set('okx', now);
          ws.send('ping');
        }
      }, 5_000);
      this.okxHeartbeatTimers.set('okx', heartbeat);
    });
    ws.on('message', (raw) => {
      try {
        const frame = parseOkxFrame(raw.toString());
        if (frame.kind === 'pong') {
          this.okxPingSentAt.delete('okx');
          this.touchFeed('okx');
          return;
        }
        if (frame.kind === 'ignore') return;
        if (frame.kind === 'event') {
          if (frame.event === 'error') throw new Error(`OKX subscription error ${frame.code ?? ''}: ${frame.message ?? 'unknown error'}`);
          this.touchFeed('okx');
          return;
        }
        const push = frame.push;
        const item = this.okxTopics.get(push.instrumentId);
        if (!item) throw new Error(`OKX books update for unsubscribed Spot instrument ${push.instrumentId}`);
        this.recordMessage('okx', item.exchangeSymbol);
        if (push.action === 'snapshot') {
          if (!this.coordinator.applySnapshot('okx', item.exchangeSymbol, {
            bids: push.bids, asks: push.asks, sequence: push.sequence,
            exchangeTimestamp: push.exchangeTimestamp, receivedTimestamp: Date.now(),
          })) throw new Error(`Coordinator rejected OKX snapshot for ${push.instrumentId}`);
          this.okxResubscribing.delete(push.instrumentId);
          return;
        }
        const current = this.coordinator.getBook('okx', item.exchangeSymbol);
        if (current.sequence === null) {
          this.coordinator.markResynchronizing('okx', item.exchangeSymbol, `OKX update before snapshot for ${push.instrumentId}`);
          this.resubscribeOkx(ws, push.instrumentId);
          return;
        }
        const currentSequence = Number(current.sequence);
        const continuity = classifyOkxUpdate(currentSequence, push);
        if (continuity === 'HEARTBEAT') return;
        if (continuity === 'GAP') {
          const expected = push.previousSequence ?? currentSequence;
          const reason = `OKX seqId gap for ${push.instrumentId}: previous ${push.previousSequence}, current ${currentSequence}, observed ${push.sequence}`;
          this.coordinator.markSequenceGap('okx', item.exchangeSymbol, expected, push.sequence, reason);
          this.resubscribeOkx(ws, push.instrumentId);
          return;
        }
        if (!this.coordinator.applyDelta('okx', item.exchangeSymbol, {
          firstUpdateId: currentSequence + 1, finalUpdateId: push.sequence,
          bids: push.bids, asks: push.asks, exchangeTimestamp: push.exchangeTimestamp,
        })) {
          this.resubscribeOkx(ws, push.instrumentId);
        }
      } catch (error) { this.fail('okx', error); }
    });
    this.attachClose('okx', ws);
  }

  private resubscribeOkx(ws: WebSocket, instrumentId: string) {
    if (this.okxResubscribing.has(instrumentId) || ws.readyState !== WebSocket.OPEN) return;
    this.okxResubscribing.add(instrumentId);
    const arg = { channel: 'books', instId: instrumentId };
    ws.send(JSON.stringify({ op: 'unsubscribe', args: [arg] }));
    ws.send(JSON.stringify({ op: 'subscribe', args: [arg] }));
  }

  private async connectKucoin(subs: LiveMarketSubscription[]) {
    try {
      const response = await fetch(process.env.KUCOIN_PUBLIC_TOKEN_URL ?? KUCOIN_PUBLIC_TOKEN_URL, {
        method: 'POST', signal: AbortSignal.timeout(12_000),
      });
      if (!response.ok) throw new Error(`KuCoin public-token HTTP ${response.status}`);
      const result = await response.json() as { code?: string; msg?: string; data?: {
        token?: string; instanceServers?: Array<{ endpoint?: string; pingInterval?: number; pingTimeout?: number }>;
      } };
      const server = result.data?.instanceServers?.[0];
      if (result.code !== '200000' || !result.data?.token || !server?.endpoint) {
        throw new Error(`KuCoin public-token response invalid: ${result.msg ?? result.code ?? 'missing token/server'}`);
      }
      if (this.stopped.has('kucoin')) return;
      const url = new URL(server.endpoint);
      url.searchParams.set('token', result.data.token);
      url.searchParams.set('connectId', randomBytes(16).toString('hex'));
      const intervalMs = positiveInt(String(server.pingInterval), 18_000);
      const timeoutMs = positiveInt(String(server.pingTimeout), 10_000);
      this.kucoinPingConfig.set('kucoin', { intervalMs, timeoutMs });
      this.kucoinSubscribeRequests.clear();
      for (const [symbol, state] of this.kucoinBootstrap) {
        if (!subs.some((item) => item.exchangeSymbol.toUpperCase() === symbol)) continue;
        this.kucoinReady.delete(symbol);
        this.kucoinBuffers.set(symbol, []);
        state.state = 'SUBSCRIBING'; state.startedAt = Date.now(); state.completedAt = null;
        state.snapshotSequence = null; state.pendingDeltas = 0; state.lastFailure = null;
      }
      const ws = new WebSocket(url.toString());
      this.sockets.set('kucoin', ws);
      ws.on('open', () => {
        this.kucoinWelcomeTimers.set('kucoin', setTimeout(() => this.fail('kucoin', new Error('KuCoin WebSocket welcome timeout')), 10_000));
      });
      ws.on('message', (raw) => {
        try {
          const frame = parseKucoinFrame(raw.toString());
          if (frame.kind === 'welcome') {
            const welcomeTimer = this.kucoinWelcomeTimers.get('kucoin');
            if (welcomeTimer) clearTimeout(welcomeTimer);
            this.kucoinWelcomeTimers.delete('kucoin');
            this.connected('kucoin');
            this.startFeedHealthMonitor('kucoin', ws);
            this.startKucoinHeartbeat(ws);
            this.sendKucoinSubscriptions(ws, subs);
            return;
          }
          if (frame.kind === 'ping') {
            this.touchFeed('kucoin');
            ws.send(JSON.stringify({ id: frame.id ?? String(Date.now()), type: 'pong', ...(frame.timestamp ? { timestamp: frame.timestamp } : {}) }));
            return;
          }
          if (frame.kind === 'pong') {
            const pending = this.kucoinPingSentAt.get('kucoin');
            if (!pending || !frame.id || frame.id === pending.id) {
              this.kucoinPingSentAt.delete('kucoin');
              this.touchFeed('kucoin');
            }
            return;
          }
          if (frame.kind === 'ack') {
            this.touchFeed('kucoin');
            const symbols = frame.id ? this.kucoinSubscribeRequests.get(frame.id) : undefined;
            if (symbols) {
              this.kucoinSubscribeRequests.delete(frame.id!);
              for (const symbol of symbols) {
                const item = this.kucoinTopics.get(symbol);
                if (item) this.enqueueKucoinSnapshot(item, ws);
              }
            }
            return;
          }
          if (frame.kind === 'error') throw new Error(`KuCoin WebSocket error ${frame.id ?? ''}: ${frame.message}`);
          if (frame.kind !== 'delta') return;
          const { delta } = frame;
          const item = this.kucoinTopics.get(delta.symbol);
          if (!item) throw new Error(`KuCoin Level 2 update for unsubscribed Spot symbol ${delta.symbol}`);
          this.recordMessage('kucoin', item.exchangeSymbol);
          if (!this.kucoinReady.has(delta.symbol)) {
            const buffer = this.kucoinBuffers.get(delta.symbol) ?? [];
            buffer.push(delta);
            const limit = positiveInt(process.env.KUCOIN_SNAPSHOT_BUFFER_MAX_EVENTS, 10_000);
            const state = this.kucoinBootstrap.get(delta.symbol);
            if (buffer.length > limit) {
              buffer.length = 0;
              this.coordinator.markResynchronizing('kucoin', item.exchangeSymbol, `KuCoin pre-snapshot buffer exceeded ${limit} events`);
              if (state) { state.state = 'RESYNCHRONIZING'; state.lastFailure = `delta buffer exceeded ${limit} events`; }
              this.enqueueKucoinSnapshot(item, ws);
            }
            this.kucoinBuffers.set(delta.symbol, buffer);
            if (state) state.pendingDeltas = buffer.length;
            return;
          }
          const current = Number(this.coordinator.sequence('kucoin', item.exchangeSymbol));
          const disposition = classifyKucoinDelta(current, delta);
          if (disposition === 'STALE') return;
          if (disposition === 'GAP') {
            const reason = `KuCoin sequence gap for ${delta.symbol}: range ${delta.sequenceStart}-${delta.sequenceEnd} after ${current}`;
            this.coordinator.markSequenceGap('kucoin', item.exchangeSymbol, current + 1, delta.sequenceStart, reason);
            this.kucoinReady.delete(delta.symbol);
            this.kucoinBuffers.set(delta.symbol, [delta]);
            const state = this.kucoinBootstrap.get(delta.symbol);
            if (state) { state.state = 'RESYNCHRONIZING'; state.pendingDeltas = 1; state.lastFailure = reason; }
            this.enqueueKucoinSnapshot(item, ws);
            return;
          }
          if (!this.applyKucoinDelta(item, delta)) {
            this.kucoinReady.delete(delta.symbol);
            this.kucoinBuffers.set(delta.symbol, [delta]);
            const state = this.kucoinBootstrap.get(delta.symbol);
            if (state) { state.state = 'RESYNCHRONIZING'; state.pendingDeltas = 1; }
            void this.loadKucoinSnapshot(item, ws);
          }
        } catch (error) { this.fail('kucoin', error); }
      });
      this.attachClose('kucoin', ws);
    } catch (error) {
      if (!this.stopped.has('kucoin')) this.scheduleReconnect('kucoin', `KuCoin token/connection setup failed: ${message(error)}`);
    }
  }

  private sendKucoinSubscriptions(ws: WebSocket, subscriptions: LiveMarketSubscription[]) {
    chunks(subscriptions.map(mapKucoinSpotSymbol), 100).forEach((symbols, index) => {
      const id = `${Date.now()}-${index}`;
      this.kucoinSubscribeRequests.set(id, symbols);
      ws.send(JSON.stringify({ id, type: 'subscribe', topic: `${KUCOIN_LEVEL2_TOPIC}:${symbols.join(',')}`, privateChannel: false, response: true }));
    });
  }

  private startKucoinHeartbeat(ws: WebSocket) {
    const old = this.kucoinHeartbeatTimers.get('kucoin');
    if (old) clearInterval(old);
    const config = this.kucoinPingConfig.get('kucoin') ?? { intervalMs: 18_000, timeoutMs: 10_000 };
    const tick = Math.max(250, Math.min(config.intervalMs, config.timeoutMs));
    const timer = setInterval(() => {
      if (this.sockets.get('kucoin') !== ws || ws.readyState !== WebSocket.OPEN) return;
      const now = Date.now(), pending = this.kucoinPingSentAt.get('kucoin');
      if (pending) {
        if (now - pending.sentAt > config.timeoutMs) this.fail('kucoin', new Error('KuCoin WebSocket heartbeat pong timeout'));
        return;
      }
      if (now - (this.kucoinLastPingAt.get('kucoin') ?? 0) < config.intervalMs) return;
      const id = `ping-${now}`;
      this.kucoinPingSentAt.set('kucoin', { id, sentAt: now });
      this.kucoinLastPingAt.set('kucoin', now);
      ws.send(JSON.stringify({ id, type: 'ping' }));
    }, tick);
    this.kucoinHeartbeatTimers.set('kucoin', timer);
  }

  private async loadKucoinSnapshot(item: LiveMarketSubscription, ws: WebSocket) {
    const symbol = mapKucoinSpotSymbol(item);
    const generation = (this.kucoinSnapshotGeneration.get(symbol) ?? 0) + 1;
    this.kucoinSnapshotGeneration.set(symbol, generation);
    const state = this.kucoinBootstrap.get(symbol)!;
    state.state = 'SNAPSHOTTING'; state.startedAt ||= Date.now(); state.lastFailure = null;
    try {
      const base = (process.env.KUCOIN_REST_BASE_URL ?? 'https://api.kucoin.com').replace(/\/$/, '');
      const endpoint = KUCOIN_FULL_ORDERBOOK_URL.replace('https://api.kucoin.com', '');
      const response = await fetch(`${base}${endpoint}?symbol=${encodeURIComponent(symbol)}`, { signal: AbortSignal.timeout(12_000) });
      if (!response.ok) throw new Error(`KuCoin order-book snapshot HTTP ${response.status} for ${symbol}`);
      const snapshot = parseKucoinSnapshot(await response.json(), symbol);
      if (this.stopped.has('kucoin') || this.sockets.get('kucoin') !== ws || this.kucoinSnapshotGeneration.get(symbol) !== generation) return;
      state.state = 'SYNCHRONIZING';
      const deltas = this.kucoinBuffers.get(symbol) ?? [];
      if (!this.reconcileKucoinSnapshot(item, snapshot, deltas)) {
        state.state = 'RESYNCHRONIZING'; state.lastFailure ??= `snapshot sequence ${snapshot.sequence} did not bridge buffered updates`;
        state.pendingDeltas = deltas.length;
        this.scheduleKucoinSnapshotRetry(item, ws);
        return;
      }
      this.kucoinReady.add(symbol);
      this.kucoinBuffers.set(symbol, []);
      state.state = 'SYNCHRONIZED'; state.completedAt = Date.now(); state.snapshotSequence = snapshot.sequence;
      state.pendingDeltas = 0; state.lastFailure = null;
    } catch (error) {
      if (this.stopped.has('kucoin') || this.sockets.get('kucoin') !== ws || this.kucoinSnapshotGeneration.get(symbol) !== generation) return;
      const reason = message(error);
      state.state = 'FAILED'; state.lastFailure = reason;
      const session = this.sessions.get('kucoin')!;
      session.errors = [...session.errors, `snapshot ${symbol}: ${reason}`].slice(-20);
      this.coordinator.markResynchronizing('kucoin', item.exchangeSymbol, `KuCoin snapshot failed: ${reason}`);
      this.scheduleKucoinSnapshotRetry(item, ws);
    }
  }

  private enqueueKucoinSnapshot(item: LiveMarketSubscription, ws: WebSocket) {
    const symbol = mapKucoinSpotSymbol(item);
    if (this.kucoinSnapshotScheduled.get(symbol) === ws) return;
    this.kucoinSnapshotScheduled.set(symbol, ws);
    this.kucoinSnapshotQueue = this.kucoinSnapshotQueue.then(async () => {
      try {
        if (!this.stopped.has('kucoin') && this.sockets.get('kucoin') === ws) await this.loadKucoinSnapshot(item, ws);
      } finally { if (this.kucoinSnapshotScheduled.get(symbol) === ws) this.kucoinSnapshotScheduled.delete(symbol); }
    });
  }

  private reconcileKucoinSnapshot(item: LiveMarketSubscription, snapshot: ReturnType<typeof parseKucoinSnapshot>, deltas: KucoinDelta[]): boolean {
    if (!this.coordinator.applySnapshot('kucoin', item.exchangeSymbol, {
      bids: snapshot.bids, asks: snapshot.asks, sequence: snapshot.sequence,
      exchangeTimestamp: snapshot.exchangeTimestamp, receivedTimestamp: Date.now(),
    })) throw new Error(`Coordinator rejected KuCoin snapshot for ${snapshot.symbol}`);
    let current = snapshot.sequence;
    for (const delta of deltas) {
      const disposition = classifyKucoinDelta(current, delta);
      if (disposition === 'STALE') continue;
      if (disposition === 'GAP') {
        const reason = `KuCoin bootstrap sequence gap for ${delta.symbol}: range ${delta.sequenceStart}-${delta.sequenceEnd} after ${current}`;
        this.coordinator.markSequenceGap('kucoin', item.exchangeSymbol, current + 1, delta.sequenceStart, reason);
        const state = this.kucoinBootstrap.get(delta.symbol);
        if (state) state.lastFailure = reason;
        return false;
      }
      if (!this.applyKucoinDelta(item, delta)) return false;
      current = delta.sequenceEnd;
    }
    return true;
  }

  private applyKucoinDelta(item: LiveMarketSubscription, delta: KucoinDelta): boolean {
    return this.coordinator.applyDelta('kucoin', item.exchangeSymbol, {
      firstUpdateId: delta.sequenceStart, finalUpdateId: delta.sequenceEnd,
      bids: delta.bids, asks: delta.asks, exchangeTimestamp: delta.exchangeTimestamp,
    });
  }

  private scheduleKucoinSnapshotRetry(item: LiveMarketSubscription, ws: WebSocket) {
    const symbol = mapKucoinSpotSymbol(item), key = `kucoin-snapshot:${symbol}`;
    if (this.timers.has(key)) return;
    const timer = setTimeout(() => {
      this.timers.delete(key);
      if (!this.stopped.has('kucoin') && this.sockets.get('kucoin') === ws) this.enqueueKucoinSnapshot(item, ws);
    }, 500);
    this.timers.set(key, timer);
  }

  private connectBitget(subs: LiveMarketSubscription[]) {
    const attemptNumber = (this.reconnectAttempts.get('bitget') ?? 0) + 1;
    const diagnostic: BitgetConnectionDiagnostic = {
      connectionId: ++this.bitgetConnectionId, attemptNumber, reasonForConnect: this.bitgetNextReconnectReason ?? 'initial connection', startedAt: Date.now(),
      openedAt: null, closedAt: null, closeCode: null, closeReason: null, phase: 'CONNECTING',
      subscriptionRequest: { status: 'NOT_SENT', sentAt: null, requestedChannels: subs.length, requestedSymbols: [],
        acknowledgedChannels: 0, acknowledgedSymbols: [], ackResponses: 0, responseAt: null, channelErrors: [] },
      receivedMessages: 0, receivedBookMessages: 0, firstBookUpdateAt: null,
      disconnectAfterFirstBookUpdate: null, synchronizedBooksAtDisconnect: null, disconnectPhase: null,
      transportPingFrames: 0, transportPongFrames: 0, textPongMessages: 0,
      lastError: null, triggerReason: null, reconnectAttempt: null, reconnectReason: null,
      reconnectScheduledAt: null, sequenceStateAtDisconnect: null, errors: [],
    };
    this.bitgetNextReconnectReason = null;
    this.bitgetCurrentConnection = diagnostic;
    this.bitgetConnectionHistory.push(diagnostic);
    if (this.bitgetConnectionHistory.length > 20) this.bitgetConnectionHistory.shift();
    this.bitgetAcknowledgedSymbols.clear();
    const ws = new WebSocket(process.env.BITGET_WS_URL ?? BITGET_SPOT_WS_URL);
    this.sockets.set('bitget', ws);
    for (const [symbol, state] of this.bitgetBootstrap) {
      this.bitgetReady.delete(symbol); this.bitgetBuffers.set(symbol, []);
      state.state = 'SUBSCRIBING'; state.startedAt = Date.now(); state.completedAt = null; state.sequence = null; state.pendingDeltas = 0; state.lastFailure = null;
    }
    ws.on('open', () => {
      diagnostic.openedAt = Date.now(); diagnostic.phase = 'SUBSCRIBING';
      this.connected('bitget'); this.startFeedHealthMonitor('bitget', ws);
      const args = subs.map((item) => ({ instType: 'spot', topic: BITGET_SPOT_BOOK_TOPIC, symbol: mapBitgetSpotSymbol(item) }));
      diagnostic.subscriptionRequest.sentAt = Date.now(); diagnostic.subscriptionRequest.status = 'SENT';
      diagnostic.subscriptionRequest.requestedChannels = args.length;
      diagnostic.subscriptionRequest.requestedSymbols = args.map((arg) => arg.symbol);
      ws.send(JSON.stringify({ op: 'subscribe', args }));
      this.startBitgetHeartbeat(ws);
    });
    ws.on('ping', () => { diagnostic.transportPingFrames += 1; this.touchFeed('bitget'); });
    ws.on('pong', () => { diagnostic.transportPongFrames += 1; this.bitgetPingSentAt.delete('bitget'); this.touchFeed('bitget'); });
    ws.on('message', (raw) => {
      diagnostic.receivedMessages += 1;
      try {
        const frame = parseBitgetFrame(raw.toString());
        if (frame.kind === 'ignore') return;
        if (frame.kind === 'pong') { diagnostic.textPongMessages += 1; this.bitgetPingSentAt.delete('bitget'); this.touchFeed('bitget'); return; }
        if (frame.kind === 'ack') {
          this.touchFeed('bitget');
          if (frame.event === 'subscribe') {
            diagnostic.subscriptionRequest.ackResponses += 1;
            diagnostic.subscriptionRequest.responseAt = Date.now();
            if (!frame.error && frame.symbol) this.bitgetAcknowledgedSymbols.add(frame.symbol);
            diagnostic.subscriptionRequest.acknowledgedChannels = this.bitgetAcknowledgedSymbols.size;
            diagnostic.subscriptionRequest.acknowledgedSymbols = [...this.bitgetAcknowledgedSymbols];
            if (!frame.error && (diagnostic.subscriptionRequest.acknowledgedChannels >= diagnostic.subscriptionRequest.requestedChannels
              || diagnostic.subscriptionRequest.ackResponses >= diagnostic.subscriptionRequest.requestedChannels)) {
              diagnostic.subscriptionRequest.status = 'ACKNOWLEDGED';
            }
          }
          if (frame.error) {
            diagnostic.subscriptionRequest.status = 'ERROR';
            diagnostic.subscriptionRequest.responseAt = Date.now();
            diagnostic.subscriptionRequest.channelErrors.push({ at: Date.now(), event: frame.event, symbol: frame.symbol, message: frame.error });
            this.fail('bitget', new Error(`Bitget ${frame.event} ${frame.symbol ?? ''}: ${frame.error}`));
          }
          return;
        }
        const message = frame.book, item = this.bitgetTopics.get(message.symbol);
        if (!item) return;
        diagnostic.receivedBookMessages += 1;
        diagnostic.firstBookUpdateAt ??= Date.now();
        if (diagnostic.phase === 'SUBSCRIBING') diagnostic.phase = 'SNAPSHOT_BOOTSTRAP';
        this.recordMessage('bitget', item.exchangeSymbol);
        if (message.action === 'snapshot') { this.applyBitgetSnapshot(item, message, ws); return; }
        if (!this.bitgetReady.has(message.symbol)) {
          const buffer = this.bitgetBuffers.get(message.symbol) ?? [];
          buffer.push(message);
          const cap = positiveInt(process.env.BITGET_SNAPSHOT_BUFFER_MAX_EVENTS, 10_000);
          if (buffer.length > cap) {
            this.bitgetBuffers.set(message.symbol, []);
            this.bitgetBootstrap.get(message.symbol)!.state = 'RESYNCHRONIZING';
            this.requestBitgetResync(message, ws, `pre-snapshot update buffer exceeded ${cap} events`);
          } else {
            this.bitgetBuffers.set(message.symbol, buffer);
            this.bitgetBootstrap.get(message.symbol)!.pendingDeltas = buffer.length;
          }
          return;
        }
        this.applyBitgetUpdate(item, message, ws, this.bitgetAwaitingBridge.has(message.symbol));
      } catch (error) { this.fail('bitget', error); }
    });
    this.attachClose('bitget', ws);
  }

  private startBitgetHeartbeat(ws: WebSocket) {
    const prior = this.bitgetHeartbeatTimers.get('bitget'); if (prior) clearInterval(prior);
    const interval = positiveInt(process.env.BITGET_PING_INTERVAL_MS, 30_000);
    const timeout = positiveInt(process.env.BITGET_PONG_TIMEOUT_MS, 10_000);
    const timer = setInterval(() => {
      if (this.sockets.get('bitget') !== ws || ws.readyState !== WebSocket.OPEN) return;
      const pending = this.bitgetPingSentAt.get('bitget'), now = Date.now();
      if (pending !== undefined) { if (now - pending > timeout) this.fail('bitget', new Error('Bitget WebSocket pong timed out')); return; }
      this.bitgetPingSentAt.set('bitget', now); ws.send('ping');
    }, interval);
    this.bitgetHeartbeatTimers.set('bitget', timer);
  }

  private applyBitgetSnapshot(item: LiveMarketSubscription, snapshot: BitgetBookMessage, ws: WebSocket) {
    const symbol = snapshot.symbol, state = this.bitgetBootstrap.get(symbol)!;
    if (!this.coordinator.applySnapshot('bitget', item.exchangeSymbol, { bids: snapshot.bids, asks: snapshot.asks,
      sequence: snapshot.sequence, exchangeTimestamp: snapshot.exchangeTimestamp, receivedTimestamp: Date.now() })) {
      throw new Error(`Coordinator rejected Bitget snapshot for ${symbol}`);
    }
    this.bitgetReady.add(symbol); state.state = 'SYNCHRONIZING'; state.sequence = snapshot.sequence; state.lastFailure = null;
    this.bitgetAwaitingBridge.add(symbol);
    let awaitingBridge = true;
    const buffered = this.bitgetBuffers.get(symbol) ?? [];
    this.bitgetBuffers.set(symbol, []);
    for (const update of buffered) {
      if (!this.applyBitgetUpdate(item, update, ws, awaitingBridge)) return;
      const current = this.coordinator.sequence('bitget', item.exchangeSymbol);
      if (current !== null && BigInt(String(current)) > BigInt(snapshot.sequence)) awaitingBridge = false;
    }
    state.state = 'SYNCHRONIZED'; state.sequence = String(this.coordinator.sequence('bitget', item.exchangeSymbol));
    state.completedAt = Date.now(); state.pendingDeltas = 0;
    this.updateBitgetConnectionPhase();
  }

  private applyBitgetUpdate(item: LiveMarketSubscription, update: BitgetBookMessage, ws: WebSocket, awaitingBridge: boolean): boolean {
    const symbol = update.symbol, state = this.bitgetBootstrap.get(symbol)!;
    const currentValue = this.coordinator.sequence('bitget', item.exchangeSymbol);
    if (currentValue === null) return false;
    const current = String(currentValue), disposition = classifyBitgetUpdate(current, update, awaitingBridge);
    if (disposition === 'STALE') return true;
    if (disposition === 'GAP') {
      const reason = `Bitget sequence discontinuity for ${symbol}: pseq=${update.previousSequence} seq=${update.sequence} after=${current}`;
      this.coordinator.markSequenceGap('bitget', item.exchangeSymbol, (BigInt(current) + 1n).toString(), update.sequence, reason);
      this.requestBitgetResync(update, ws, reason); return false;
    }
    if (!this.coordinator.applyDelta('bitget', item.exchangeSymbol, { firstUpdateId: update.previousSequence!, finalUpdateId: update.sequence,
      bids: update.bids, asks: update.asks, exchangeTimestamp: update.exchangeTimestamp })) {
      const reason = `Coordinator rejected Bitget update for ${symbol}: seq=${update.sequence}`;
      this.coordinator.markResynchronizing('bitget', item.exchangeSymbol, reason);
      this.requestBitgetResync(update, ws, reason); return false;
    }
    if (disposition === 'BRIDGE') this.bitgetAwaitingBridge.delete(symbol);
    state.sequence = update.sequence; state.state = 'SYNCHRONIZED'; state.lastFailure = null;
    this.updateBitgetConnectionPhase();
    return true;
  }

  private updateBitgetConnectionPhase() {
    const diagnostic = this.bitgetCurrentConnection;
    if (!diagnostic || diagnostic.closedAt !== null) return;
    const synchronized = [...this.bitgetBootstrap.keys()].filter((symbol) => this.bitgetReady.has(symbol)
      && this.coordinator.getBook('bitget', this.bitgetTopics.get(symbol)!.exchangeSymbol).synchronized).length;
    diagnostic.phase = synchronized === diagnostic.subscriptionRequest.requestedChannels ? 'SYNCHRONIZED'
      : synchronized > 0 ? 'PARTIALLY_SYNCHRONIZED' : diagnostic.receivedBookMessages > 0 ? 'SNAPSHOT_BOOTSTRAP' : diagnostic.openedAt ? 'SUBSCRIBING' : 'CONNECTING';
  }

  private captureBitgetDisconnect(diagnostic: BitgetConnectionDiagnostic, code: number, reason: Buffer) {
    const now = Date.now();
    diagnostic.closedAt = now; diagnostic.closeCode = code; diagnostic.closeReason = reason.toString();
    const sequenceState: NonNullable<BitgetConnectionDiagnostic['sequenceStateAtDisconnect']> = {};
    let synchronized = 0;
    for (const [symbol, state] of this.bitgetBootstrap) {
      const item = this.bitgetTopics.get(symbol)!;
      const book = this.coordinator.getBook('bitget', item.exchangeSymbol, now);
      const details = this.coordinator.symbolDiagnostics('bitget', item.exchangeSymbol, now);
      const isSynchronized = book.synchronized && book.sequenceValid;
      if (isSynchronized) synchronized += 1;
      sequenceState[symbol] = { bootstrapState: state.state, sequence: state.sequence,
        pendingDeltas: this.bitgetBuffers.get(symbol)?.length ?? 0, synchronized: isSynchronized,
        sequenceGaps: details.sequenceGaps, resynchronizations: details.resynchronizations,
        lastFailure: state.lastFailure ?? details.lastFailureReason };
    }
    diagnostic.synchronizedBooksAtDisconnect = synchronized;
    diagnostic.disconnectAfterFirstBookUpdate = diagnostic.firstBookUpdateAt !== null;
    diagnostic.disconnectPhase = !diagnostic.openedAt ? 'CONNECTING'
      : synchronized === diagnostic.subscriptionRequest.requestedChannels ? 'AFTER_SYNCHRONIZATION'
        : synchronized > 0 ? 'PARTIALLY_SYNCHRONIZED'
          : diagnostic.firstBookUpdateAt !== null ? 'SNAPSHOT_BOOTSTRAP' : 'SUBSCRIBING';
    diagnostic.phase = 'CLOSED'; diagnostic.sequenceStateAtDisconnect = sequenceState;
    diagnostic.reconnectReason = diagnostic.triggerReason ?? (diagnostic.lastError
      ? 'Bitget failure: ' + diagnostic.lastError
      : 'socket closed ' + code + ': ' + (reason.toString() || 'no close reason'));
    diagnostic.errors.push('socket closed ' + code + ': ' + (reason.toString() || 'no close reason'));
  }

  private requestBitgetResync(update: BitgetBookMessage, ws: WebSocket, reason: string) {
    const state = this.bitgetBootstrap.get(update.symbol)!;
    if (this.bitgetCurrentConnection && this.bitgetCurrentConnection.closedAt === null) this.bitgetCurrentConnection.triggerReason = reason;
    state.state = 'RESYNCHRONIZING'; state.lastFailure = reason; state.completedAt = null;
    this.bitgetReady.delete(update.symbol); this.bitgetAwaitingBridge.delete(update.symbol); this.bitgetBuffers.set(update.symbol, []); state.pendingDeltas = 0;
    if (this.sockets.get('bitget') === ws && ws.readyState === WebSocket.OPEN) ws.terminate();
  }

  private connectHtx(subs: LiveMarketSubscription[]) {
    const levels = this.htxLevels();
    const ws = new WebSocket(process.env.HTX_WS_URL ?? HTX_SPOT_MBP_WS_URL);
    this.sockets.set('htx', ws);
    this.htxRequests.clear();
    for (const [symbol, state] of this.htxBootstrap) {
      this.htxReady.delete(symbol); this.htxBuffers.set(symbol, []); this.htxCandidates.delete(symbol);
      state.state = 'SUBSCRIBING'; state.startedAt = Date.now(); state.completedAt = null;
      state.sequence = null; state.pendingDeltas = 0; state.lastFailure = null;
    }
    ws.on('open', () => {
      this.connected('htx'); this.startFeedHealthMonitor('htx', ws);
      for (const item of subs) {
        const symbol = mapHtxSpotSymbol(item), topic = htxMbpTopic(symbol, levels);
        const id = `htx-sub-${this.htxNextRequestId++}`;
        this.htxRequests.set(id, { symbol, kind: 'subscribe' });
        ws.send(JSON.stringify({ sub: topic, id }));
      }
    });
    ws.on('ping', () => this.touchFeed('htx'));
    ws.on('message', (raw) => {
      try {
        const frame = parseHtxFrame(raw as Buffer | Buffer[]);
        if (frame.kind === 'ignore') return;
        if (frame.kind === 'heartbeat') {
          this.touchFeed('htx');
          if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ pong: frame.ping }));
          return;
        }
        if (frame.kind === 'ack') {
          this.touchFeed('htx');
          const request = frame.id ? this.htxRequests.get(frame.id) : undefined;
          if (frame.error) { this.fail('htx', new Error(`HTX ${frame.topic ?? request?.symbol ?? ''}: ${frame.error}`)); return; }
          if (request?.kind === 'subscribe') {
            this.htxRequests.delete(frame.id!);
            if (frame.topic !== htxMbpTopic(request.symbol, this.htxLevels())) {
              this.fail('htx', new Error(`HTX subscription acknowledgement topic mismatch for ${request.symbol}: ${frame.topic ?? 'missing topic'}`));
              return;
            }
            const item = this.htxTopics.get(request.symbol);
            if (item && this.sockets.get('htx') === ws) this.enqueueHtxSnapshot(item, ws);
          }
          return;
        }
        if (frame.kind === 'snapshot') {
          this.touchFeed('htx');
          const request = this.htxRequests.get(frame.id);
          if (!request || request.kind !== 'refresh') return;
          this.htxRequests.delete(frame.id);
          if (frame.error) {
            const state = this.htxBootstrap.get(request.symbol)!;
            state.state = 'FAILED'; state.lastFailure = frame.error;
            const item = this.htxTopics.get(request.symbol);
            if (item) this.scheduleHtxSnapshotRetry(item, ws);
            return;
          }
          if (request.symbol !== frame.snapshot.symbol) throw new Error(`HTX refresh symbol mismatch: requested ${request.symbol}, received ${frame.snapshot.symbol}`);
          if (request.generation !== this.htxSnapshotGeneration.get(request.symbol)) return;
          const item = this.htxTopics.get(request.symbol);
          if (!item) return;
          const state = this.htxBootstrap.get(request.symbol)!;
          state.state = 'SYNCHRONIZING'; state.sequence = frame.snapshot.sequence; state.lastFailure = null;
          this.htxCandidates.set(request.symbol, { snapshot: frame.snapshot, generation: request.generation! });
          this.reconcileHtxSnapshot(item, ws);
          return;
        }
        const delta = frame.delta, item = this.htxTopics.get(delta.symbol);
        if (!item) return;
        this.recordMessage('htx', item.exchangeSymbol);
        if (!this.htxReady.has(delta.symbol)) {
          const buffer = this.htxBuffers.get(delta.symbol) ?? [];
          buffer.push(delta);
          const cap = positiveInt(process.env.HTX_SNAPSHOT_BUFFER_MAX_EVENTS, 10_000);
          if (buffer.length > cap) {
            this.htxBuffers.set(delta.symbol, []);
            this.htxCandidates.delete(delta.symbol);
            this.coordinator.markResynchronizing('htx', item.exchangeSymbol, `HTX snapshot buffer exceeded ${cap} events`);
            this.scheduleHtxSnapshotRetry(item, ws);
            return;
          }
          this.htxBuffers.set(delta.symbol, buffer);
          this.htxBootstrap.get(delta.symbol)!.pendingDeltas = buffer.length;
          if (this.htxCandidates.has(delta.symbol)) this.reconcileHtxSnapshot(item, ws);
          return;
        }
        this.applyHtxDelta(item, delta, ws);
      } catch (error) { this.fail('htx', error); }
    });
    this.attachClose('htx', ws);
  }

  private enqueueHtxSnapshot(item: LiveMarketSubscription, ws: WebSocket) {
    const symbol = mapHtxSpotSymbol(item);
    if (this.htxSnapshotScheduled.get(symbol) === ws) return;
    this.htxSnapshotScheduled.set(symbol, ws);
    this.htxSnapshotQueue = this.htxSnapshotQueue.then(async () => {
      try {
        if (this.stopped.has('htx') || this.sockets.get('htx') !== ws || ws.readyState !== WebSocket.OPEN) return;
        const wait = Math.max(0, 125 - (Date.now() - this.htxLastRefreshAt));
        if (wait) await new Promise((resolve) => setTimeout(resolve, wait));
        if (this.stopped.has('htx') || this.sockets.get('htx') !== ws || ws.readyState !== WebSocket.OPEN) return;
        const topic = htxMbpTopic(symbol, this.htxLevels());
        const generation = (this.htxSnapshotGeneration.get(symbol) ?? 0) + 1;
        this.htxSnapshotGeneration.set(symbol, generation);
        const id = `htx-req-${this.htxNextRequestId++}`;
        this.htxRequests.set(id, { symbol, kind: 'refresh', generation });
        const state = this.htxBootstrap.get(symbol)!;
        state.state = 'SNAPSHOTTING'; state.startedAt ||= Date.now();
        this.htxLastRefreshAt = Date.now();
        ws.send(JSON.stringify({ req: topic, id }));
      } finally {
        if (this.htxSnapshotScheduled.get(symbol) === ws) this.htxSnapshotScheduled.delete(symbol);
      }
    }).catch((error) => this.fail('htx', error));
  }

  private htxLevels() {
    const depth = positiveInt(process.env.ORDERBOOK_DEPTH_LEVELS, 20);
    if (depth > 400) throw new Error(`HTX supports at most 400 MBP levels; requested ORDERBOOK_DEPTH_LEVELS=${depth}`);
    // HTX documents 5/20 levels as symbol-limited; 150/400 is supported across all Spot symbols.
    return depth <= 150 ? 150 : 400;
  }

  private scheduleHtxSnapshotRetry(item: LiveMarketSubscription, ws: WebSocket) {
    const symbol = mapHtxSpotSymbol(item), key = `htx-snapshot:${symbol}`;
    if (this.timers.has(key)) return;
    const timer = setTimeout(() => {
      this.timers.delete(key);
      if (!this.stopped.has('htx') && this.sockets.get('htx') === ws) this.enqueueHtxSnapshot(item, ws);
    }, 300);
    this.timers.set(key, timer);
  }

  private reconcileHtxSnapshot(item: LiveMarketSubscription, ws: WebSocket): boolean {
    const symbol = mapHtxSpotSymbol(item), candidate = this.htxCandidates.get(symbol);
    if (!candidate || this.sockets.get('htx') !== ws || candidate.generation !== this.htxSnapshotGeneration.get(symbol)) return false;
    const snapshot = candidate.snapshot;
    // HTX specifies a strict prevSeqNum == prior seqNum bridge; never sort pushes to mask loss.
    const pending = (this.htxBuffers.get(symbol) ?? []).filter((delta) => BigInt(delta.sequence) > BigInt(snapshot.sequence));
    const first = pending[0];
    if (!first) return false; // wait for the next update to provide a documented bridge
    if (BigInt(first.previousSequence) !== BigInt(snapshot.sequence)) {
      const reason = `HTX snapshot bridge gap for ${symbol}: snapshot=${snapshot.sequence}, first prevSeqNum=${first.previousSequence}, seqNum=${first.sequence}`;
      // A refresh that cannot yet be aligned is a bootstrap miss, not a gap in a live book.
      this.coordinator.markResynchronizing('htx', item.exchangeSymbol, reason);
      const state = this.htxBootstrap.get(symbol)!; state.state = 'RESYNCHRONIZING'; state.lastFailure = reason;
      // Keep cached frames: the next refresh may align with one already received.
      this.htxCandidates.delete(symbol); this.scheduleHtxSnapshotRetry(item, ws); return false;
    }
    if (!this.coordinator.applySnapshot('htx', item.exchangeSymbol, { bids: snapshot.bids, asks: snapshot.asks,
      sequence: snapshot.sequence, exchangeTimestamp: snapshot.exchangeTimestamp ?? Date.now(), receivedTimestamp: Date.now() })) {
      throw new Error(`Coordinator rejected HTX snapshot for ${symbol}`);
    }
    let current = snapshot.sequence;
    for (const delta of pending) {
      const disposition = classifyHtxDelta(current, delta);
      if (disposition === 'STALE') continue;
      if (disposition === 'GAP') {
        const reason = `HTX buffered sequence gap for ${symbol}: prevSeqNum=${delta.previousSequence}, seqNum=${delta.sequence}, prior=${current}`;
        this.coordinator.markSequenceGap('htx', item.exchangeSymbol, current, delta.previousSequence, reason);
        const state = this.htxBootstrap.get(symbol)!; state.state = 'RESYNCHRONIZING'; state.lastFailure = reason;
        // Preserve the bounded cache until the next refresh has had a chance to bridge it.
        this.htxReady.delete(symbol); this.htxCandidates.delete(symbol);
        this.scheduleHtxSnapshotRetry(item, ws); return false;
      }
      if (!this.applyHtxToCoordinator(item, delta)) {
        this.scheduleHtxSnapshotRetry(item, ws); return false;
      }
      current = delta.sequence;
    }
    this.htxReady.add(symbol); this.htxBuffers.set(symbol, []); this.htxCandidates.delete(symbol);
    const state = this.htxBootstrap.get(symbol)!; state.state = 'SYNCHRONIZED';
    state.completedAt = Date.now(); state.sequence = current; state.pendingDeltas = 0; state.lastFailure = null;
    return true;
  }

  private applyHtxDelta(item: LiveMarketSubscription, delta: HtxBookDelta, ws: WebSocket) {
    const current = this.coordinator.sequence('htx', item.exchangeSymbol);
    if (current === null) { this.htxReady.delete(delta.symbol); this.scheduleHtxSnapshotRetry(item, ws); return false; }
    const disposition = classifyHtxDelta(String(current), delta);
    if (disposition === 'STALE') return true;
    if (disposition === 'GAP') {
      const reason = `HTX sequence discontinuity for ${delta.symbol}: prevSeqNum=${delta.previousSequence}, seqNum=${delta.sequence}, prior=${current}`;
      this.coordinator.markSequenceGap('htx', item.exchangeSymbol, current, delta.previousSequence, reason);
      const state = this.htxBootstrap.get(delta.symbol)!; state.state = 'RESYNCHRONIZING'; state.lastFailure = reason; state.completedAt = null;
      this.htxReady.delete(delta.symbol); this.htxBuffers.set(delta.symbol, [delta]); this.htxCandidates.delete(delta.symbol);
      this.scheduleHtxSnapshotRetry(item, ws); return false;
    }
    if (!this.applyHtxToCoordinator(item, delta)) {
      const reason = `Coordinator rejected HTX MBP update for ${delta.symbol}`;
      this.coordinator.markResynchronizing('htx', item.exchangeSymbol, reason);
      const state = this.htxBootstrap.get(delta.symbol)!; state.state = 'RESYNCHRONIZING'; state.lastFailure = reason;
      this.htxReady.delete(delta.symbol); this.htxBuffers.set(delta.symbol, []); this.scheduleHtxSnapshotRetry(item, ws); return false;
    }
    const state = this.htxBootstrap.get(delta.symbol)!; state.sequence = delta.sequence; state.state = 'SYNCHRONIZED';
    return true;
  }

  private applyHtxToCoordinator(item: LiveMarketSubscription, delta: HtxBookDelta) {
    return this.coordinator.applyDelta('htx', item.exchangeSymbol, { firstUpdateId: delta.previousSequence,
      finalUpdateId: delta.sequence, bids: delta.bids, asks: delta.asks, exchangeTimestamp: delta.exchangeTimestamp });
  }

  private connectGate(subs: LiveMarketSubscription[]) {
    const ws = new WebSocket(process.env.GATE_WS_URL ?? GATE_SPOT_WS_URL);
    this.sockets.set('gate', ws);
    this.gateSubscribeRequests.clear();
    for (const [symbol, state] of this.gateBootstrap) {
      this.gateReady.delete(symbol); this.gateBuffers.set(symbol, []); this.gateSnapshotCandidates.delete(symbol);
      state.state = 'SUBSCRIBING'; state.startedAt = Date.now(); state.completedAt = null;
      state.snapshotId = null; state.pendingDeltas = 0; state.lastFailure = null;
    }
    ws.on('open', () => {
      this.connected('gate'); this.startFeedHealthMonitor('gate', ws);
      for (const item of subs) {
        const symbol = mapGateSpotSymbol(item), id = this.gateNextRequestId++;
        this.gateSubscribeRequests.set(id, symbol);
        ws.send(JSON.stringify({ time: Math.floor(Date.now() / 1_000), id, channel: GATE_SPOT_ORDER_BOOK_UPDATE_CHANNEL,
          event: 'subscribe', payload: [symbol, '100ms'] }));
      }
      this.startGateHeartbeat(ws);
    });
    ws.on('ping', () => this.touchFeed('gate'));
    ws.on('message', (raw) => {
      try {
        const frame = parseGateFrame(raw.toString());
        if (frame.kind === 'pong') { this.gatePingSentAt.delete('gate'); this.touchFeed('gate'); return; }
        if (frame.kind === 'ack') {
          this.touchFeed('gate');
          const id = typeof frame.id === 'number' ? frame.id : Number(frame.id);
          const symbol = Number.isSafeInteger(id) ? this.gateSubscribeRequests.get(id) : undefined;
          if (frame.error !== null) throw new Error(`Gate WebSocket ${frame.channel} ${frame.event} failed: ${frame.error}`);
          if (symbol) { this.gateSubscribeRequests.delete(id); const item = this.gateTopics.get(symbol); if (item) this.enqueueGateSnapshot(item, ws); }
          return;
        }
        if (frame.kind !== 'book') return;
        const delta = frame.delta, item = this.gateTopics.get(delta.symbol);
        if (!item) throw new Error(`Gate order-book update for unsubscribed Spot pair ${delta.symbol}`);
        this.recordMessage('gate', item.exchangeSymbol);
        if (!this.gateReady.has(delta.symbol)) {
          const buffer = this.gateBuffers.get(delta.symbol) ?? []; buffer.push(delta);
          const limit = positiveInt(process.env.GATE_SNAPSHOT_BUFFER_MAX_EVENTS, 10_000);
          const state = this.gateBootstrap.get(delta.symbol)!;
          if (buffer.length > limit) {
            buffer.length = 0; state.state = 'RESYNCHRONIZING'; state.lastFailure = `update buffer exceeded ${limit} events`;
            this.coordinator.markResynchronizing('gate', item.exchangeSymbol, state.lastFailure); this.enqueueGateSnapshot(item, ws);
          }
          this.gateBuffers.set(delta.symbol, buffer); state.pendingDeltas = buffer.length;
          const candidate = this.gateSnapshotCandidates.get(delta.symbol);
          if (candidate) this.reconcileGateSnapshot(item, candidate, buffer, ws);
          return;
        }
        const current = Number(this.coordinator.sequence('gate', item.exchangeSymbol));
        const disposition = classifyGateDelta(current, delta);
        if (disposition === 'STALE') return;
        if (disposition === 'GAP') {
          const reason = `Gate order-book update gap for ${delta.symbol}: range ${delta.firstUpdateId}-${delta.finalUpdateId}, expected ${current + 1}`;
          this.coordinator.markSequenceGap('gate', item.exchangeSymbol, current + 1, delta.firstUpdateId, reason);
          this.gateReady.delete(delta.symbol); this.gateBuffers.set(delta.symbol, [delta]);
          const state = this.gateBootstrap.get(delta.symbol)!; state.state = 'RESYNCHRONIZING'; state.pendingDeltas = 1; state.lastFailure = reason;
          this.gateSnapshotCandidates.delete(delta.symbol); this.enqueueGateSnapshot(item, ws); return;
        }
        if (!this.applyGateDelta(item, delta)) {
          this.gateReady.delete(delta.symbol); this.gateBuffers.set(delta.symbol, [delta]);
          this.coordinator.markResynchronizing('gate', item.exchangeSymbol, 'coordinator rejected Gate order-book update');
          this.enqueueGateSnapshot(item, ws);
        }
      } catch (error) { this.fail('gate', error); }
    });
    this.attachClose('gate', ws);
  }

  private startGateHeartbeat(ws: WebSocket) {
    const prior = this.gateHeartbeatTimers.get('gate'); if (prior) clearInterval(prior);
    const interval = positiveInt(process.env.GATE_PING_INTERVAL_MS, 20_000), timeout = positiveInt(process.env.GATE_PING_TIMEOUT_MS, 10_000);
    const timer = setInterval(() => {
      if (this.sockets.get('gate') !== ws || ws.readyState !== WebSocket.OPEN) return;
      const pending = this.gatePingSentAt.get('gate'), now = Date.now();
      if (pending !== undefined) { if (now - pending > timeout) this.fail('gate', new Error('Gate spot.ping timed out')); return; }
      this.gatePingSentAt.set('gate', now);
      ws.send(JSON.stringify({ time: Math.floor(now / 1_000), channel: 'spot.ping' }));
    }, interval);
    this.gateHeartbeatTimers.set('gate', timer);
  }

  private enqueueGateSnapshot(item: LiveMarketSubscription, ws: WebSocket) {
    const symbol = mapGateSpotSymbol(item);
    if (this.gateSnapshotScheduled.get(symbol) === ws) return;
    this.gateSnapshotScheduled.set(symbol, ws);
    this.gateSnapshotQueue = this.gateSnapshotQueue.then(async () => {
      try { if (!this.stopped.has('gate') && this.sockets.get('gate') === ws) await this.loadGateSnapshot(item, ws); }
      finally { if (this.gateSnapshotScheduled.get(symbol) === ws) this.gateSnapshotScheduled.delete(symbol); }
    });
  }

  private async loadGateSnapshot(item: LiveMarketSubscription, ws: WebSocket) {
    const symbol = mapGateSpotSymbol(item), generation = (this.gateSnapshotGeneration.get(symbol) ?? 0) + 1;
    this.gateSnapshotGeneration.set(symbol, generation);
    const state = this.gateBootstrap.get(symbol)!; state.state = 'SNAPSHOTTING'; state.startedAt ||= Date.now(); state.lastFailure = null;
    try {
      const base = (process.env.GATE_REST_BASE_URL ?? 'https://api.gateio.ws').replace(/\/$/, '');
      const endpoint = GATE_SPOT_ORDER_BOOK_URL.replace('https://api.gateio.ws', '');
      const response = await fetch(`${base}${endpoint}?currency_pair=${encodeURIComponent(symbol)}&limit=100&with_id=true`, { signal: AbortSignal.timeout(12_000) });
      if (!response.ok) throw new Error(`Gate REST order-book snapshot HTTP ${response.status} for ${symbol}`);
      const snapshot = parseGateSnapshot(await response.json(), symbol);
      if (this.stopped.has('gate') || this.sockets.get('gate') !== ws || this.gateSnapshotGeneration.get(symbol) !== generation) return;
      state.state = 'SYNCHRONIZING'; state.snapshotId = snapshot.updateId; this.gateSnapshotCandidates.set(symbol, snapshot);
      if (!this.reconcileGateSnapshot(item, snapshot, this.gateBuffers.get(symbol) ?? [], ws)) {
        state.state = 'RESYNCHRONIZING'; state.lastFailure ??= `snapshot ${snapshot.updateId} did not bridge buffered updates`;
        this.scheduleGateSnapshotRetry(item, ws);
      }
    } catch (error) {
      if (this.stopped.has('gate') || this.sockets.get('gate') !== ws || this.gateSnapshotGeneration.get(symbol) !== generation) return;
      const reason = message(error); state.state = 'FAILED'; state.lastFailure = reason;
      const session = this.sessions.get('gate')!; session.errors = [...session.errors, `snapshot ${symbol}: ${reason}`].slice(-20);
      this.coordinator.markResynchronizing('gate', item.exchangeSymbol, `Gate snapshot failed: ${reason}`);
      this.scheduleGateSnapshotRetry(item, ws);
    }
  }

  private reconcileGateSnapshot(item: LiveMarketSubscription, snapshot: ReturnType<typeof parseGateSnapshot>, deltas: GateBookDelta[], ws: WebSocket): boolean {
    const symbol = snapshot.symbol, expected = snapshot.updateId + 1;
    // Keep wire arrival order; only notifications strictly before baseID+1 are stale.
    const pending = deltas.filter((delta) => delta.finalUpdateId >= expected);
    const first = pending[0];
    if (!first) return true;
    const firstDisposition = classifyGateDelta(snapshot.updateId, first, true);
    if (firstDisposition === 'GAP') {
      const reason = `Gate bootstrap gap for ${symbol}: first range ${first.firstUpdateId}-${first.finalUpdateId} skips snapshot next ID ${expected}`;
      // Gate explicitly requires a newer snapshot when the base trails the buffered feed.
      this.coordinator.markResynchronizing('gate', item.exchangeSymbol, reason);
      const state = this.gateBootstrap.get(symbol)!; state.state = 'RESYNCHRONIZING'; state.lastFailure = reason;
      // Keep buffered notifications; a newer REST base may bridge an already-cached range.
      this.gateSnapshotCandidates.delete(symbol); this.scheduleGateSnapshotRetry(item, ws); return false;
    }
    if (!this.coordinator.applySnapshot('gate', item.exchangeSymbol, { bids: snapshot.bids, asks: snapshot.asks,
      sequence: snapshot.updateId, exchangeTimestamp: snapshot.exchangeTimestamp, receivedTimestamp: Date.now() })) {
      throw new Error(`Coordinator rejected Gate snapshot for ${symbol}`);
    }
    let current = snapshot.updateId, awaitingBridge = true;
    for (const delta of pending) {
      const disposition = classifyGateDelta(current, delta, awaitingBridge);
      if (disposition === 'STALE') continue;
      if (disposition === 'GAP') {
        const reason = `Gate buffered update gap for ${symbol}: range ${delta.firstUpdateId}-${delta.finalUpdateId}, expected ${current + 1}`;
        this.coordinator.markSequenceGap('gate', item.exchangeSymbol, current + 1, delta.firstUpdateId, reason);
        const state = this.gateBootstrap.get(symbol)!; state.lastFailure = reason; state.state = 'RESYNCHRONIZING';
        // Preserve all buffered frames for reconciliation with the replacement snapshot.
        this.gateSnapshotCandidates.delete(symbol); this.scheduleGateSnapshotRetry(item, ws); return false;
      }
      if (!this.applyGateDelta(item, delta)) return false;
      current = delta.finalUpdateId; awaitingBridge = false;
    }
    this.gateReady.add(symbol); this.gateBuffers.set(symbol, []); this.gateSnapshotCandidates.delete(symbol);
    const state = this.gateBootstrap.get(symbol)!; state.state = 'SYNCHRONIZED'; state.completedAt = Date.now();
    state.snapshotId = snapshot.updateId; state.pendingDeltas = 0; state.lastFailure = null; return true;
  }

  private applyGateDelta(item: LiveMarketSubscription, delta: GateBookDelta): boolean {
    return this.coordinator.applyDelta('gate', item.exchangeSymbol, { firstUpdateId: delta.firstUpdateId, finalUpdateId: delta.finalUpdateId,
      bids: delta.bids, asks: delta.asks, exchangeTimestamp: delta.exchangeTimestamp });
  }
  private scheduleGateSnapshotRetry(item: LiveMarketSubscription, ws: WebSocket) {
    const symbol = mapGateSpotSymbol(item), key = `gate-snapshot:${symbol}`;
    if (this.timers.has(key)) return;
    const timer = setTimeout(() => { this.timers.delete(key); if (!this.stopped.has('gate') && this.sockets.get('gate') === ws) this.enqueueGateSnapshot(item, ws); }, 500);
    this.timers.set(key, timer);
  }

  private connectMexc(subs: LiveMarketSubscription[]) {
    if (subs.length > MEXC_MAX_SUBSCRIPTIONS_PER_SOCKET) {
      this.scheduleReconnect('mexc', `MEXC Spot permits at most ${MEXC_MAX_SUBSCRIPTIONS_PER_SOCKET} depth subscriptions per socket; requested ${subs.length}`);
      return;
    }
    const ws = new WebSocket(process.env.MEXC_WS_URL ?? MEXC_SPOT_WS_URL);
    this.sockets.set('mexc', ws);
    this.mexcSubscribeRequests.clear();
    for (const [symbol, state] of this.mexcBootstrap) {
      this.mexcReady.delete(symbol);
      this.mexcBuffers.set(symbol, []);
      state.state = 'SUBSCRIBING'; state.startedAt = Date.now(); state.completedAt = null;
      state.snapshotVersion = null; state.pendingDeltas = 0; state.lastFailure = null;
    }
    ws.on('open', () => {
      this.connected('mexc');
      this.startFeedHealthMonitor('mexc', ws);
      for (const item of subs) {
        const symbol = mapMexcSpotSymbol(item), id = this.mexcNextRequestId++;
        this.mexcSubscribeRequests.set(id, symbol);
        ws.send(JSON.stringify({ method: 'SUBSCRIPTION', params: [mexcDepthChannel(symbol)], id }));
      }
      this.startMexcHeartbeat(ws);
    });
    ws.on('message', (raw, isBinary) => {
      try {
        const frame = parseMexcFrame(isBinary ? new Uint8Array(raw as Buffer) : raw.toString(), isBinary);
        if (frame.kind === 'ack') {
          this.touchFeed('mexc');
          const symbol = this.mexcSubscribeRequests.get(frame.id);
          if (symbol) {
            this.mexcSubscribeRequests.delete(frame.id);
            if (frame.code !== 0) throw new Error(`MEXC subscription rejected (${frame.code}): ${frame.message}`);
            const item = this.mexcTopics.get(symbol);
            if (item) this.enqueueMexcSnapshot(item, ws);
          } else if (frame.code !== 0) throw new Error(`MEXC WebSocket error (${frame.code}): ${frame.message}`);
          return;
        }
        if (frame.kind === 'pong') {
          this.mexcPingSentAt.delete('mexc'); this.touchFeed('mexc'); return;
        }
        if (frame.kind !== 'delta') return;
        const delta = frame.delta, item = this.mexcTopics.get(delta.symbol);
        if (!item) throw new Error(`MEXC depth update for unsubscribed Spot symbol ${delta.symbol}`);
        this.recordMessage('mexc', item.exchangeSymbol);
        if (!this.mexcReady.has(delta.symbol)) {
          const buffer = this.mexcBuffers.get(delta.symbol) ?? [];
          buffer.push(delta);
          const cap = positiveInt(process.env.MEXC_SNAPSHOT_BUFFER_MAX_EVENTS, 10_000);
          const state = this.mexcBootstrap.get(delta.symbol)!;
          if (buffer.length > cap) {
            buffer.splice(0, buffer.length);
            state.state = 'RESYNCHRONIZING'; state.lastFailure = `delta buffer exceeded ${cap} events`;
            this.coordinator.markResynchronizing('mexc', item.exchangeSymbol, state.lastFailure);
            this.enqueueMexcSnapshot(item, ws);
          }
          this.mexcBuffers.set(delta.symbol, buffer); state.pendingDeltas = buffer.length;
          const candidate = this.mexcSnapshotCandidates.get(delta.symbol);
          if (candidate) this.reconcileMexcSnapshot(item, candidate, buffer, ws);
          return;
        }
        const current = Number(this.coordinator.sequence('mexc', item.exchangeSymbol));
        const disposition = classifyMexcDelta(current, delta);
        if (disposition === 'STALE') return;
        if (disposition === 'GAP') {
          const reason = `MEXC version gap for ${delta.symbol}: range ${delta.fromVersion}-${delta.toVersion} after ${current}`;
          console.warn(reason);
          this.coordinator.markSequenceGap('mexc', item.exchangeSymbol, current + 1, delta.fromVersion, reason);
          this.mexcReady.delete(delta.symbol); this.mexcBuffers.set(delta.symbol, [delta]);
          const state = this.mexcBootstrap.get(delta.symbol)!;
          state.state = 'RESYNCHRONIZING'; state.pendingDeltas = 1; state.lastFailure = reason;
          this.mexcSnapshotCandidates.delete(delta.symbol); this.enqueueMexcSnapshot(item, ws); return;
        }
        if (!this.applyMexcDelta(item, delta)) {
          this.mexcReady.delete(delta.symbol); this.mexcBuffers.set(delta.symbol, [delta]);
          this.coordinator.markResynchronizing('mexc', item.exchangeSymbol, 'coordinator rejected MEXC version update');
          this.enqueueMexcSnapshot(item, ws);
        }
      } catch (error) { this.fail('mexc', error); }
    });
    this.attachClose('mexc', ws);
  }

  private startMexcHeartbeat(ws: WebSocket) {
    const old = this.mexcHeartbeatTimers.get('mexc'); if (old) clearInterval(old);
    const interval = positiveInt(process.env.MEXC_PING_INTERVAL_MS, 20_000);
    const timeout = positiveInt(process.env.MEXC_PING_TIMEOUT_MS, 10_000);
    const timer = setInterval(() => {
      if (this.sockets.get('mexc') !== ws || ws.readyState !== WebSocket.OPEN) return;
      const pending = this.mexcPingSentAt.get('mexc'), now = Date.now();
      if (pending !== undefined) { if (now - pending > timeout) this.fail('mexc', new Error('MEXC WebSocket PONG timeout')); return; }
      this.mexcPingSentAt.set('mexc', now);
      ws.send(JSON.stringify({ method: 'PING' }));
    }, interval);
    this.mexcHeartbeatTimers.set('mexc', timer);
  }

  private enqueueMexcSnapshot(item: LiveMarketSubscription, ws: WebSocket) {
    const symbol = mapMexcSpotSymbol(item);
    if (this.mexcSnapshotScheduled.get(symbol) === ws) return;
    this.mexcSnapshotScheduled.set(symbol, ws);
    this.mexcSnapshotQueue = this.mexcSnapshotQueue.then(async () => {
      try { if (!this.stopped.has('mexc') && this.sockets.get('mexc') === ws) await this.loadMexcSnapshot(item, ws); }
      finally { if (this.mexcSnapshotScheduled.get(symbol) === ws) this.mexcSnapshotScheduled.delete(symbol); }
    });
  }

  private async loadMexcSnapshot(item: LiveMarketSubscription, ws: WebSocket) {
    const symbol = mapMexcSpotSymbol(item), generation = (this.mexcSnapshotGeneration.get(symbol) ?? 0) + 1;
    this.mexcSnapshotGeneration.set(symbol, generation);
    const state = this.mexcBootstrap.get(symbol)!;
    state.state = 'SNAPSHOTTING'; state.startedAt ||= Date.now(); state.lastFailure = null;
    try {
      const base = (process.env.MEXC_REST_BASE_URL ?? 'https://api.mexc.com').replace(/\/$/, '');
      const response = await fetch(`${base}${MEXC_DEPTH_SNAPSHOT_URL.replace('https://api.mexc.com', '')}?symbol=${encodeURIComponent(symbol)}&limit=1000`, { signal: AbortSignal.timeout(12_000) });
      if (!response.ok) throw new Error(`MEXC depth snapshot HTTP ${response.status} for ${symbol}`);
      const snapshot = parseMexcSnapshot(await response.json(), symbol);
      if (this.stopped.has('mexc') || this.sockets.get('mexc') !== ws || this.mexcSnapshotGeneration.get(symbol) !== generation) return;
      state.state = 'SYNCHRONIZING'; state.snapshotVersion = snapshot.lastUpdateId;
      this.mexcSnapshotCandidates.set(symbol, snapshot);
      if (!this.reconcileMexcSnapshot(item, snapshot, this.mexcBuffers.get(symbol) ?? [], ws)) {
        state.state = 'RESYNCHRONIZING'; state.lastFailure ??= `snapshot version ${snapshot.lastUpdateId} did not bridge buffered updates`;
        this.scheduleMexcSnapshotRetry(item, ws);
      }
    } catch (error) {
      if (this.stopped.has('mexc') || this.sockets.get('mexc') !== ws || this.mexcSnapshotGeneration.get(symbol) !== generation) return;
      const reason = message(error); state.state = 'FAILED'; state.lastFailure = reason;
      const session = this.sessions.get('mexc')!; session.errors = [...session.errors, `snapshot ${symbol}: ${reason}`].slice(-20);
      this.coordinator.markResynchronizing('mexc', item.exchangeSymbol, `MEXC snapshot failed: ${reason}`);
      this.scheduleMexcSnapshotRetry(item, ws);
    }
  }

  private reconcileMexcSnapshot(item: LiveMarketSubscription, snapshot: ReturnType<typeof parseMexcSnapshot>, deltas: MexcDepthDelta[], ws: WebSocket): boolean {
    const symbol = snapshot.symbol;
    // Preserve the exchange's arrival order: sorting could conceal an out-of-order push.
    const ordered = deltas.filter((delta) => delta.toVersion >= snapshot.lastUpdateId);
    const first = ordered[0];
    if (!first) return true; // keep the snapshot private; a later push must bridge it
    const bootstrapDisposition = classifyMexcDelta(snapshot.lastUpdateId, first, true);
    if (bootstrapDisposition === 'GAP') {
      const reason = `MEXC bootstrap gap for ${symbol}: first buffered range ${first.fromVersion}-${first.toVersion} is after snapshot ${snapshot.lastUpdateId}`;
      console.warn(reason);
      this.coordinator.markSequenceGap('mexc', item.exchangeSymbol, snapshot.lastUpdateId, first.fromVersion, reason);
      const state = this.mexcBootstrap.get(symbol)!; state.lastFailure = reason; state.state = 'RESYNCHRONIZING';
      this.mexcSnapshotCandidates.delete(symbol);
      // The already-buffered future event cannot bridge this snapshot; wait for a later
      // event after this snapshot version instead of repeatedly fetching the same stale base.
      this.mexcBuffers.set(symbol, []);
      this.scheduleMexcSnapshotRetry(item, ws); return false;
    }
    if (bootstrapDisposition !== 'BRIDGE') return true;
    if (!this.coordinator.applySnapshot('mexc', item.exchangeSymbol, { bids: snapshot.bids, asks: snapshot.asks,
      sequence: snapshot.lastUpdateId, exchangeTimestamp: first.exchangeTimestamp, receivedTimestamp: Date.now() })) {
      throw new Error(`Coordinator rejected MEXC snapshot for ${symbol}`);
    }
    let current = snapshot.lastUpdateId;
    let awaitingBridge = true;
    for (const delta of ordered) {
      const disposition = classifyMexcDelta(current, delta, awaitingBridge);
      if (disposition === 'STALE') continue;
      if (disposition === 'GAP') {
        const reason = `MEXC buffered version gap for ${symbol}: ${delta.fromVersion}-${delta.toVersion} after ${current}`;
        console.warn(reason);
        this.coordinator.markSequenceGap('mexc', item.exchangeSymbol, current + 1, delta.fromVersion, reason);
        const state = this.mexcBootstrap.get(symbol)!; state.lastFailure = reason; state.state = 'RESYNCHRONIZING';
        this.mexcSnapshotCandidates.delete(symbol); this.mexcSnapshotBuffersAfterGap(symbol, delta);
        this.scheduleMexcSnapshotRetry(item, ws); return false;
      }
      if (!this.applyMexcDelta(item, delta)) return false;
      current = delta.toVersion;
      awaitingBridge = false;
    }
    this.mexcReady.add(symbol); this.mexcBuffers.set(symbol, []); this.mexcSnapshotCandidates.delete(symbol);
    const state = this.mexcBootstrap.get(symbol)!; state.state = 'SYNCHRONIZED'; state.completedAt = Date.now();
    state.snapshotVersion = snapshot.lastUpdateId; state.pendingDeltas = 0; state.lastFailure = null;
    return true;
  }

  private mexcSnapshotBuffersAfterGap(symbol: string, delta: MexcDepthDelta) {
    const buffer = this.mexcBuffers.get(symbol) ?? []; this.mexcBuffers.set(symbol, buffer.includes(delta) ? buffer : [delta]);
  }
  private applyMexcDelta(item: LiveMarketSubscription, delta: MexcDepthDelta): boolean {
    return this.coordinator.applyDelta('mexc', item.exchangeSymbol, { firstUpdateId: delta.fromVersion, finalUpdateId: delta.toVersion,
      bids: delta.bids, asks: delta.asks, exchangeTimestamp: delta.exchangeTimestamp });
  }
  private scheduleMexcSnapshotRetry(item: LiveMarketSubscription, ws: WebSocket) {
    const symbol = mapMexcSpotSymbol(item), key = `mexc-snapshot:${symbol}`;
    if (this.timers.has(key)) return;
    const timer = setTimeout(() => { this.timers.delete(key); if (!this.stopped.has('mexc') && this.sockets.get('mexc') === ws) this.enqueueMexcSnapshot(item, ws); }, 500);
    this.timers.set(key, timer);
  }

  private connected(exchange: ExchangeId) {
    const stats = this.sessions.get(exchange)!;
    stats.connections = 1;
    this.touchFeed(exchange);
    this.coordinator.setConnection(exchange, 'CONNECTED', stats.reconnects);
  }
  private recordMessage(exchange: ExchangeId, symbol: string) {
    const stats = this.sessions.get(exchange)!;
    stats.errors = stats.errors.slice(-100);
    this.touchFeed(exchange);
    // Count each received market event via a zero-duration processing sample hook.
    this.coordinator.recordExternalMessage(exchange, 0, symbol);
  }
  private fail(exchange: ExchangeId, error: unknown) {
    const text = message(error);
    if (exchange === 'bitget' && this.bitgetCurrentConnection && this.bitgetCurrentConnection.closedAt === null) {
      this.bitgetCurrentConnection.lastError = text;
      this.bitgetCurrentConnection.errors.push(text);
    }
    const stats = this.sessions.get(exchange)!;
    stats.errors = [...stats.errors, text].slice(-20);
    stats.feedHealthy = false;
    this.coordinator.setFeedHealth(exchange, false);
    this.sockets.get(exchange)?.terminate();
    console.error(`${exchange} market-data error: ${text}`);
  }
  private attachClose(exchange: ExchangeId, ws: WebSocket) {
    ws.on('error', (error) => this.fail(exchange, error));
    ws.on('close', (code, reason) => {
      if (exchange === 'bitget' && this.bitgetCurrentConnection) this.captureBitgetDisconnect(this.bitgetCurrentConnection, code, reason);
      if (this.sockets.get(exchange) === ws) this.sockets.delete(exchange);
      const healthTimer = this.feedHealthTimers.get(exchange);
      if (healthTimer) clearInterval(healthTimer);
      this.feedHealthTimers.delete(exchange);
      const heartbeatTimer = this.okxHeartbeatTimers.get(exchange);
      if (heartbeatTimer) clearInterval(heartbeatTimer);
      this.okxHeartbeatTimers.delete(exchange);
      this.okxPingSentAt.delete(exchange);
      if (exchange === 'kucoin') {
        const welcomeTimer = this.kucoinWelcomeTimers.get(exchange);
        if (welcomeTimer) clearTimeout(welcomeTimer);
        this.kucoinWelcomeTimers.delete(exchange);
        const kucoinHeartbeat = this.kucoinHeartbeatTimers.get(exchange);
        if (kucoinHeartbeat) clearInterval(kucoinHeartbeat);
        this.kucoinHeartbeatTimers.delete(exchange);
        this.kucoinPingSentAt.delete(exchange);
        this.kucoinLastPingAt.delete(exchange);
      }
      if (exchange === 'mexc') {
        const timer = this.mexcHeartbeatTimers.get(exchange);
        if (timer) clearInterval(timer);
        this.mexcHeartbeatTimers.delete(exchange); this.mexcPingSentAt.delete(exchange);
        for (const [symbol, state] of this.mexcBootstrap) {
          this.mexcReady.delete(symbol); this.mexcBuffers.set(symbol, []); this.mexcSnapshotCandidates.delete(symbol);
          state.state = 'SUBSCRIBING'; state.completedAt = null; state.pendingDeltas = 0;
        }
      }
      if (exchange === 'gate') {
        const timer = this.gateHeartbeatTimers.get(exchange);
        if (timer) clearInterval(timer);
        this.gateHeartbeatTimers.delete(exchange); this.gatePingSentAt.delete(exchange);
        for (const [symbol, state] of this.gateBootstrap) {
          this.gateReady.delete(symbol); this.gateBuffers.set(symbol, []); this.gateSnapshotCandidates.delete(symbol);
          state.state = 'SUBSCRIBING'; state.completedAt = null; state.pendingDeltas = 0;
        }
      }
      if (exchange === 'bitget') {
        const timer = this.bitgetHeartbeatTimers.get(exchange);
        if (timer) clearInterval(timer);
        this.bitgetHeartbeatTimers.delete(exchange); this.bitgetPingSentAt.delete(exchange);
        for (const [symbol, state] of this.bitgetBootstrap) {
          this.bitgetReady.delete(symbol); this.bitgetAwaitingBridge.delete(symbol); this.bitgetBuffers.set(symbol, []);
          state.state = 'SUBSCRIBING'; state.completedAt = null; state.sequence = null; state.pendingDeltas = 0;
        }
      }
      if (exchange === 'htx') {
        this.htxRequests.clear();
        for (const [symbol, state] of this.htxBootstrap) {
          this.htxReady.delete(symbol); this.htxBuffers.set(symbol, []); this.htxCandidates.delete(symbol);
          state.state = 'SUBSCRIBING'; state.completedAt = null; state.sequence = null; state.pendingDeltas = 0;
        }
      }
      if (exchange === 'crypto.com') {
        this.cryptoComResubscribing.clear(); this.cryptoComResubscribeRequests.clear();
        for (const state of this.cryptoComBootstrap.values()) {
          state.state = 'SUBSCRIBING'; state.completedAt = null;
        }
      }
      if (exchange === 'coinbase') {
        this.coinbaseResubscribePending.clear(); this.coinbaseAwaitingUnsubscribeAck.clear();
        this.coinbaseLastSequence = null;
        for (const state of this.coinbaseBootstrap.values()) {
          state.state = 'SUBSCRIBING'; state.completedAt = null; state.exchangeSequence = null;
          state.sequenceSource = 'LOCAL_ORDER';
        }
      }
      const stats = this.sessions.get(exchange)!;
      stats.connections = 0;
      stats.feedHealthy = false;
      if (this.stopped.has(exchange)) return;
      stats.reconnects += 1;
      this.coordinator.setConnection(exchange, 'DISCONNECTED', stats.reconnects);
      const attempt = this.reconnectAttempts.get(exchange) ?? 0;
      this.reconnectAttempts.set(exchange, attempt + 1);
      if (exchange === 'bitget' && this.bitgetCurrentConnection) {
        this.bitgetCurrentConnection.reconnectAttempt = attempt + 2;
        this.bitgetCurrentConnection.reconnectScheduledAt = Date.now();
        this.bitgetNextReconnectReason = this.bitgetCurrentConnection.reconnectReason;
      }
      stats.errors = [...stats.errors, `socket closed ${code}: ${reason.toString()}`].slice(-20);
      const base = Math.min(60_000, 1_000 * 2 ** Math.min(attempt, 6));
      const jitter = Math.floor(Math.random() * Math.max(1, base * 0.2));
      const timer = setTimeout(() => { this.timers.delete(exchange); this.connect(exchange); }, base + jitter);
      this.timers.set(exchange, timer);
    });
  }

  private touchFeed(exchange: ExchangeId) {
    const now = Date.now();
    this.lastFeedHeartbeat.set(exchange, now);
    const stats = this.sessions.get(exchange);
    if (stats) { stats.feedHealthy = true; stats.lastFeedHeartbeatTimestamp = now; }
    this.coordinator.setFeedHealth(exchange, true, now);
  }

  private scheduleReconnect(exchange: ExchangeId, reason: string) {
    const stats = this.sessions.get(exchange)!;
    stats.connections = 0;
    stats.feedHealthy = false;
    stats.errors = [...stats.errors, reason].slice(-20);
    stats.reconnects += 1;
    this.coordinator.setConnection(exchange, 'DISCONNECTED', stats.reconnects);
    const attempt = this.reconnectAttempts.get(exchange) ?? 0;
    this.reconnectAttempts.set(exchange, attempt + 1);
    const base = Math.min(60_000, 1_000 * 2 ** Math.min(attempt, 6));
    const jitter = Math.floor(Math.random() * Math.max(1, base * 0.2));
    const timer = setTimeout(() => { this.timers.delete(exchange); this.connect(exchange); }, base + jitter);
    this.timers.set(exchange, timer);
    console.error(`${exchange} market-data reconnect scheduled: ${reason}`);
  }

  private startFeedHealthMonitor(exchange: ExchangeId, ws: WebSocket) {
    const old = this.feedHealthTimers.get(exchange);
    if (old) clearInterval(old);
    const timeoutMs = positiveInt(process.env[`${exchange.toUpperCase()}_FEED_HEALTH_TIMEOUT_MS`] ?? process.env.FEED_HEALTH_TIMEOUT_MS, 45_000);
    const timer = setInterval(() => {
      if (this.sockets.get(exchange) !== ws || ws.readyState !== WebSocket.OPEN) return;
      const lastHeartbeat = this.lastFeedHeartbeat.get(exchange) ?? 0;
      if (Date.now() - lastHeartbeat <= timeoutMs) return;
      const stats = this.sessions.get(exchange)!;
      stats.feedHealthy = false;
      this.coordinator.setFeedHealth(exchange, false);
      if (exchange === 'bitget' && this.bitgetCurrentConnection && this.bitgetCurrentConnection.closedAt === null) {
        this.bitgetCurrentConnection.triggerReason = 'feed health timeout after ' + timeoutMs + 'ms';
      }
      // Force the normal reconnect/bootstrap path after transport liveness expires.
      ws.terminate();
    }, Math.min(1_000, timeoutMs));
    this.feedHealthTimers.set(exchange, timer);
  }
}

function levels(rows: unknown): OrderBookLevel[] {
  if (!Array.isArray(rows)) return [];
  return rows.map((row) => Array.isArray(row) ? { price: Number(row[0]), quantity: Number(row[1]) } : { price: NaN, quantity: NaN })
    .filter((level) => Number.isFinite(level.price) && level.price > 0 && Number.isFinite(level.quantity) && level.quantity >= 0);
}
function message(error: unknown) { return error instanceof Error ? error.message : String(error); }
function chunks<T>(values: T[], size: number): T[][] {
  const batches: T[][] = [];
  for (let index = 0; index < values.length; index += size) batches.push(values.slice(index, index + size));
  return batches;
}
function wait(ms: number) { return new Promise<void>((resolve) => setTimeout(resolve, ms)); }
function positiveInt(value: string | undefined, fallback: number) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}
