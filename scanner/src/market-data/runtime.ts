import { monitorEventLoopDelay } from 'node:perf_hooks';
import { createServer, type Server } from 'node:http';
import { readFileSync } from 'node:fs';
import { cpus, freemem, totalmem } from 'node:os';
import type { ExchangeId } from '../discovery/types';
import { loadLiveUniverse, type LiveUniverse } from './universe';
import { MarketDataCoordinator } from './coordinator';
import type { MarketDataResourceTelemetry } from './types';
import { LiveExchangeSessions } from './live-sessions';
import { MEXC_MAX_SUBSCRIPTIONS_PER_SOCKET } from './mexc-protocol';
import { ArbitrageMonitor } from '../arbitrage/monitor';
import { arbitrageConfigFromEnv, DEFAULT_ARBITRAGE_CONFIG } from '../arbitrage/config';
import { profitabilityLimitsFromEnv, type ProfitabilityLimits } from '../arbitrage/profitability';
import type { ArbitrageConfig } from '../arbitrage/detection';

import { isUsableOrderBook } from './validity';
export type RuntimeOptions = {
  reportPath: string;
  maxPairs: number;
  pairs?: string[];
  depthLevels?: number;
  port: number;
  exchanges?: ExchangeId[];
  profitability?: ProfitabilityLimits;
  requiredExchanges?: ExchangeId[];
  connectExchanges?: boolean;
  arbitrage?: ArbitrageConfig;
};

export type RuntimeStatus = 'STARTING' | 'RUNNING' | 'STOPPING' | 'STOPPED';

/** Isolated Phase B process boundary. It never starts or mutates the legacy port-4000 scanner. */
export class MarketDataRuntime {
  private server?: Server;
  private readonly delay = monitorEventLoopDelay({ resolution: 20 });
  private readonly startedCpu = process.cpuUsage();
  private lastCpuSample = process.cpuUsage();
  private lastCpuAt = Date.now();
  private lastSystemCpu = cpuSnapshot();
  private lastNetworkSample = networkSnapshot();
  private lastSystemSampleAt = Date.now();
  private status: RuntimeStatus = 'STARTING';
  private readonly sessions: LiveExchangeSessions;
  readonly universe: LiveUniverse;
  readonly coordinator: MarketDataCoordinator;
  private readonly arbitrage: ArbitrageMonitor;

  private constructor(universe: LiveUniverse, options: RuntimeOptions) {
    this.universe = universe;
    this.coordinator = new MarketDataCoordinator(universe.subscriptions, { depthLevels: options.depthLevels });
    this.arbitrage = new ArbitrageMonitor(this.coordinator, universe.subscriptions, options.arbitrage ?? DEFAULT_ARBITRAGE_CONFIG,
      options.profitability);
    this.sessions = new LiveExchangeSessions(universe.subscriptions, this.coordinator);
    this.options = options;
    this.delay.enable();
  }
  private readonly options: RuntimeOptions;

  static async create(options: RuntimeOptions): Promise<MarketDataRuntime> {
    const exchanges = options.exchanges ?? ['binance', 'bybit'];
    const eligibleSymbols = options.connectExchanges !== false && exchanges.includes('coinbase')
      ? { coinbase: await fetchCoinbaseOnlineProducts() } : undefined;
    const universe = await loadLiveUniverse(options.reportPath, { maxPairs: options.maxPairs, pairs: options.pairs,
      exchanges, requiredExchanges: options.requiredExchanges ?? exchanges, eligibleSymbols });
    const mexcCount = universe.subscriptions.filter((item) => item.exchange === 'mexc').length;
    if (mexcCount > MEXC_MAX_SUBSCRIPTIONS_PER_SOCKET) {
      throw new Error(`MEXC currently supports at most ${MEXC_MAX_SUBSCRIPTIONS_PER_SOCKET} Spot subscriptions per Phase B socket; selected ${mexcCount}`);
    }
    return new MarketDataRuntime(universe, options);
  }

  async start(): Promise<void> {
    if (this.status !== 'STARTING') throw new Error(`Cannot start runtime in ${this.status}`);
    this.server = createServer((request, response) => {
      response.setHeader('content-type', 'application/json; charset=utf-8');
      const url = new URL(request.url ?? '/', 'http://127.0.0.1');
      if (url.pathname === '/health' || url.pathname === '/telemetry') {
        response.end(JSON.stringify(this.telemetry()));
      } else if (url.pathname === '/test/reconnect' && request.method === 'POST'
        && process.env.ENABLE_WS_TEST_CONTROLS === 'true') {
        const exchange = url.searchParams.get('exchange');
        if (exchange !== 'binance' && exchange !== 'bybit' && exchange !== 'okx' && exchange !== 'kucoin' && exchange !== 'mexc' && exchange !== 'gate' && exchange !== 'bitget' && exchange !== 'htx' && exchange !== 'crypto.com' && exchange !== 'coinbase') {
          response.statusCode = 400;
          response.end(JSON.stringify({ error: 'exchange must be a supported Phase B exchange' }));
        } else {
          const interrupted = this.sessions.interrupt(exchange);
          response.statusCode = interrupted ? 202 : 409;
          response.end(JSON.stringify({ exchange, interrupted }));
        }
      } else {
        response.statusCode = 404;
        response.end(JSON.stringify({ error: 'not found' }));
      }
    });
    await new Promise<void>((resolve, reject) => {
      this.server!.once('error', reject);
      this.server!.listen(this.options.port, '127.0.0.1', () => resolve());
    });
    this.status = 'RUNNING';
    if (this.options.connectExchanges !== false) this.sessions.start();
    console.log(`Phase B market-data runtime: RUNNING on http://127.0.0.1:${this.options.port}`);
    console.log(`Phase A universe ${this.universe.sourceRunId}: ${this.universe.selectedPairCount}/${this.universe.candidatePairCount} pairs; ${this.universe.subscriptions.length} market subscriptions selected`);
    console.log(`Exchange adapters: ${this.options.connectExchanges === false ? 'disabled' : [...new Set(this.universe.subscriptions.map((item) => item.exchange))].join(' + ')}`);
  }

  telemetry(now = Date.now()) {
    const exchanges: Partial<Record<ExchangeId, ReturnType<MarketDataCoordinator['getExchangeTelemetry']>>> = {};
    for (const exchange of new Set(this.universe.subscriptions.map((item) => item.exchange))) {
      exchanges[exchange] = this.coordinator.getExchangeTelemetry(exchange, now);
    }
    const binanceSnapshotTelemetry = this.sessions.binanceSnapshotTelemetry();
    const binancePerSymbol = binanceSnapshotTelemetry.perSymbol as Record<string, { attempts?: number; retries?: number }>;
    return {
      status: this.status,
      phase: 'B',
      sourceRunId: this.universe.sourceRunId,
      sourceStatus: this.universe.sourceStatus,
      candidatePairs: this.universe.candidatePairCount,
      selectedPairs: this.universe.selectedPairCount,
      pairs: [...new Set(this.universe.subscriptions.map((item) => item.canonicalPair))],
      subscriptions: this.universe.subscriptions.length,
      books: this.coordinator.counts().books,
      sessionConnections: this.sessions.snapshot(),
      binanceSnapshotBootstrap: binanceSnapshotTelemetry,
      kucoinSnapshotBootstrap: this.sessions.kucoinSnapshotTelemetry(),
      mexcSnapshotBootstrap: this.sessions.mexcSnapshotTelemetry(),
      gateSnapshotBootstrap: this.sessions.gateSnapshotTelemetry(),
      bitgetSnapshotBootstrap: this.sessions.bitgetSnapshotTelemetry(),
      htxSnapshotBootstrap: this.sessions.htxSnapshotTelemetry(),
      cryptoComSnapshotBootstrap: this.sessions.cryptoComSnapshotTelemetry(),
      coinbaseSnapshotBootstrap: this.sessions.coinbaseSnapshotTelemetry(),
      markets: this.coordinator.subscriptions().map((subscription) => {
        const book = this.coordinator.getBook(subscription.exchange, subscription.exchangeSymbol, now);
        const diagnostics = this.coordinator.symbolDiagnostics(subscription.exchange, subscription.exchangeSymbol, now, undefined, book);
        const binanceBootstrap = subscription.exchange === 'binance' ? this.sessions.symbolBootstrapState(subscription.exchangeSymbol) : null;
        return {
          exchange: book.exchange, exchangeSymbol: book.exchangeSymbol, canonicalPair: book.canonicalPair,
          ...(subscription.exchange === 'coinbase' ? { sequenceSource: this.sessions.coinbaseSequenceState(subscription.exchangeSymbol)?.source,
            exchangeSequence: this.sessions.coinbaseSequenceState(subscription.exchangeSymbol)?.exchangeSequence ?? null } : {}),
          status: book.status, synchronized: book.synchronized, stale: book.stale,
          quiet: book.status === 'QUIET', feedHealth: book.feedHealth,
          sequence: book.sequence, sequenceValid: book.sequenceValid,
          bestBid: book.bestBid, bestBidQuantity: book.bestBidQuantity,
          bestAsk: book.bestAsk, bestAskQuantity: book.bestAskQuantity,
          exchangeTimestamp: book.exchangeTimestamp, receivedTimestamp: book.receivedTimestamp,
          ageMs: book.sequence === null ? null : Math.max(0, now - book.receivedTimestamp),
          usable: isUsableOrderBook(book, { connected: book.feedHealth === 'HEALTHY' }),
          diagnostics: {
            ...diagnostics,
            snapshotState: binanceBootstrap?.state ?? diagnostics.snapshotState,
            pending: binanceBootstrap ? ['QUEUED', 'SNAPSHOTTING', 'SYNCHRONIZING', 'RESYNCHRONIZING'].includes(binanceBootstrap.state) : diagnostics.pending,
            snapshotting: binanceBootstrap?.state === 'SNAPSHOTTING',
            synchronizing: binanceBootstrap?.state === 'SYNCHRONIZING' || diagnostics.synchronizing,
            failed: binanceBootstrap?.state === 'FAILED' || diagnostics.failed,
            snapshotAttempts: binanceBootstrap ? binancePerSymbol[subscription.exchangeSymbol.toUpperCase()]?.attempts ?? 0 : 0,
            snapshotRetries: binanceBootstrap ? binancePerSymbol[subscription.exchangeSymbol.toUpperCase()]?.retries ?? 0 : 0,
            snapshotFailureReason: binanceBootstrap?.lastFailure ?? diagnostics.lastFailureReason,
          },
        };
      }),
      marketStateCounts: (() => {
        const states = this.coordinator.subscriptions().map((subscription) => this.coordinator.symbolDiagnostics(subscription.exchange, subscription.exchangeSymbol, now));
        const synchronized = states.filter((state) => state.synchronizationState === 'SYNCHRONIZED').length;
        const quiet = states.filter((state) => state.synchronizationState === 'QUIET').length;
        const stale = states.filter((state) => state.stale).length;
        const failed = states.filter((state) => state.failed).length;
        const pending = states.length - synchronized - quiet - stale - failed;
        return { total: states.length, synchronized, quiet, stale, failed, pending };
      })(),
      exchanges,
      arbitrage: this.arbitrage.snapshot(now),
      resources: this.resourceTelemetry(now),
    };
  }

  resourceTelemetry(now = Date.now()): MarketDataResourceTelemetry {
    const current = process.cpuUsage();
    const elapsed = Math.max(1, now - this.lastCpuAt);
    const deltaMicros = (current.user - this.lastCpuSample.user) + (current.system - this.lastCpuSample.system);
    this.lastCpuSample = current;
    this.lastCpuAt = now;
    const memory = process.memoryUsage();
    const systemCpu = cpuSnapshot();
    const systemCpuPercent = cpuUsagePercent(this.lastSystemCpu, systemCpu);
    this.lastSystemCpu = systemCpu;
    const network = networkSnapshot();
    const networkElapsedSeconds = Math.max(0.001, (now - this.lastSystemSampleAt) / 1_000);
    const networkRxBytesPerSecond = network && this.lastNetworkSample ? Math.max(0, network.rxBytes - this.lastNetworkSample.rxBytes) / networkElapsedSeconds : null;
    const networkTxBytesPerSecond = network && this.lastNetworkSample ? Math.max(0, network.txBytes - this.lastNetworkSample.txBytes) / networkElapsedSeconds : null;
    this.lastNetworkSample = network;
    this.lastSystemSampleAt = now;
    const bookCount = this.coordinator.counts().books;
    return {
      sampledAt: now,
      processCpuPercent: Math.max(0, Math.round(deltaMicros / (elapsed * 10)) / 10),
      systemCpuPercent,
      systemMemoryTotalBytes: totalmem(), systemMemoryUsedBytes: totalmem() - freemem(),
      systemMemoryAvailableBytes: freemem(), networkRxBytesPerSecond, networkTxBytesPerSecond,
      rssBytes: memory.rss,
      heapUsedBytes: memory.heapUsed,
      eventLoopDelayMeanMs: Number.isFinite(this.delay.mean) ? this.delay.mean / 1e6 : null,
      eventLoopDelayMaxMs: Number.isFinite(this.delay.max) ? this.delay.max / 1e6 : null,
      activeWebSocketConnections: this.sessions.connectionCount(),
      subscriptions: this.universe.subscriptions.length,
      books: bookCount,
      estimatedBookBytes: bookCount * (this.coordinator.counts().depthLevels * 2 * 32),
    };
  }

  async stop(): Promise<void> {
    if (this.status === 'STOPPED') return;
    this.status = 'STOPPING';
    this.sessions.stop();
    this.arbitrage.stop();
    this.delay.disable();
    if (this.server) await new Promise<void>((resolve, reject) => this.server!.close((error) => error ? reject(error) : resolve()));
    this.status = 'STOPPED';
  }
}

async function fetchCoinbaseOnlineProducts(): Promise<string[]> {
  const response = await fetch('https://api.exchange.coinbase.com/products', { signal: AbortSignal.timeout(5_000) });
  if (!response.ok) throw new Error(`Coinbase public product catalog HTTP ${response.status}`);
  const value: unknown = await response.json();
  if (!Array.isArray(value)) throw new Error('Coinbase public product catalog response was not an array');
  const symbols = value.filter((product): product is { id: string; status: string } => typeof product === 'object' && product !== null
    && typeof (product as Record<string, unknown>).id === 'string' && typeof (product as Record<string, unknown>).status === 'string')
    .filter((product) => product.status === 'online').map((product) => product.id.toUpperCase());
  if (!symbols.length) throw new Error('Coinbase public product catalog contained no online Spot products');
  return symbols;
}

export function runtimeOptionsFromEnv(env = process.env): RuntimeOptions {
  const maxPairs = positiveInteger(env.MAX_LIVE_PAIRS ?? '3', 'MAX_LIVE_PAIRS');
  const port = positiveInteger(env.MARKET_DATA_PORT ?? '4100', 'MARKET_DATA_PORT');
  const depthLevels = positiveInteger(env.ORDERBOOK_DEPTH_LEVELS ?? '20', 'ORDERBOOK_DEPTH_LEVELS');
  const reportPath = env.PHASE_A_REPORT ?? 'data/discovery/latest.json';
  const pairs = env.LIVE_PAIRS?.split(',').map((pair) => pair.trim()).filter(Boolean);
  const exchanges = parseExchanges(env.MARKET_DATA_EXCHANGES ?? 'binance,bybit,okx,kucoin,gate,bitget,htx,crypto.com,coinbase');
  const requiredExchanges = parseRequiredExchanges(env.MARKET_DATA_REQUIRED_EXCHANGES ?? 'none', exchanges);
  return { reportPath, maxPairs, ...(pairs?.length ? { pairs } : {}), depthLevels, port, exchanges,
    requiredExchanges,
    arbitrage: arbitrageConfigFromEnv(env),
    profitability: profitabilityLimitsFromEnv(env),
    connectExchanges: (env.LIVE_MARKET_DATA ?? 'true').toLowerCase() !== 'false' };
}

function parseExchanges(value: string): ExchangeId[] {
  const allowed = new Set<ExchangeId>(['binance', 'bybit', 'okx', 'kucoin', 'mexc', 'gate', 'bitget', 'htx', 'crypto.com', 'coinbase']);
  const exchanges = [...new Set(value.split(',').map((exchange) => exchange.trim().toLowerCase()).filter(Boolean))] as ExchangeId[];
  if (!exchanges.length || exchanges.some((exchange) => !allowed.has(exchange))) {
    throw new Error('MARKET_DATA_EXCHANGES must be a comma-separated list of binance, bybit, okx, kucoin, mexc, gate, bitget, htx, crypto.com, and/or coinbase');
  }
  return exchanges;
}

function parseRequiredExchanges(value: string, requested: ExchangeId[]): ExchangeId[] {
  if (value.trim().toLowerCase() === 'none') return [];
  const required = parseExchanges(value);
  if (required.some((exchange) => !requested.includes(exchange))) {
    throw new Error('MARKET_DATA_REQUIRED_EXCHANGES must be a subset of MARKET_DATA_EXCHANGES or none');
  }
  return required;
}

function positiveInteger(value: string, name: string) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 65_535) throw new Error(`${name} must be an integer from 1 to 65535`);
  return parsed;
}

function cpuSnapshot() {
  return cpus().map((cpu) => ({ ...cpu.times }));
}
function cpuUsagePercent(before: ReturnType<typeof cpuSnapshot>, after: ReturnType<typeof cpuSnapshot>) {
  if (!before.length || before.length !== after.length) return null;
  let totalDelta = 0;
  let idleDelta = 0;
  for (let index = 0; index < before.length; index += 1) {
    const previous = before[index], current = after[index];
    totalDelta += Object.keys(current).reduce((sum, key) => sum + (current[key as keyof typeof current] - previous[key as keyof typeof previous]), 0);
    idleDelta += current.idle - previous.idle;
  }
  return totalDelta > 0 ? Math.round((1 - idleDelta / totalDelta) * 1_000) / 10 : 0;
}
function networkSnapshot() {
  try {
    const lines = readFileSync('/proc/net/dev', 'utf8').trim().split('\n').slice(2);
    let rxBytes = 0, txBytes = 0;
    for (const line of lines) {
      const [iface, counters] = line.trim().split(':');
      if (!counters || iface.trim() === 'lo') continue;
      const fields = counters.trim().split(/\s+/).map(Number);
      rxBytes += fields[0] ?? 0;
      txBytes += fields[8] ?? 0;
    }
    return { rxBytes, txBytes };
  } catch { return null; }
}
