import type { ExchangeId } from '../discovery/types';
import type { AdapterStatus, OrderBookLevel } from '../types';

export type MarketBookStatus = AdapterStatus | 'STALE' | 'SYNCHRONIZED' | 'SYNCHRONIZING' | 'QUIET' | 'UNHEALTHY';
export type FeedHealth = 'HEALTHY' | 'UNHEALTHY' | 'DISCONNECTED';

export type OrderBookState = {
  exchange: ExchangeId;
  canonicalAsset: string;
  canonicalQuote: string;
  canonicalPair: string;
  exchangeSymbol: string;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
  bestBid: number | null;
  bestBidQuantity: number | null;
  bestAsk: number | null;
  bestAskQuantity: number | null;
  sequence: number | string | null;
  sequenceValid: boolean;
  feedHealth: FeedHealth;
  lastFeedHeartbeatTimestamp: number | null;
  exchangeTimestamp: number | null;
  receivedTimestamp: number;
  processedTimestamp: number;
  status: MarketBookStatus;
  synchronized: boolean;
  stale: boolean;
  updateCount: number;
};

export type ExchangeMarketDataTelemetry = {
  exchange: ExchangeId;
  connections: number;
  connectedStreams: number;
  subscriptions: number;
  synchronizedBooks: number;
  staleBooks: number;
  quietBooks: number;
  unhealthyBooks: number;
  disconnectedBooks: number;
  errorBooks: number;
  reconnects: number;
  messages: number;
  messagesPerSecond: number;
  lastMessageTimestamp: number | null;
  averageProcessingLatencyMs: number | null;
  maxProcessingLatencyMs: number | null;
  sequenceGaps: number;
  resynchronizations: number;
  errors: string[];
};

export type MarketDataResourceTelemetry = {
  sampledAt: number;
  processCpuPercent: number | null;
  systemCpuPercent: number | null;
  systemMemoryTotalBytes: number;
  systemMemoryUsedBytes: number;
  systemMemoryAvailableBytes: number;
  networkRxBytesPerSecond: number | null;
  networkTxBytesPerSecond: number | null;
  rssBytes: number;
  heapUsedBytes: number;
  eventLoopDelayMeanMs: number | null;
  eventLoopDelayMaxMs: number | null;
  activeWebSocketConnections: number;
  subscriptions: number;
  books: number;
  estimatedBookBytes: number;
};
