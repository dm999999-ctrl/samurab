export type OrderBookLevel = { price: number; quantity: number };
export type NormalizedOrderBook = {
  exchange: string;
  symbol: string;
  exchangeTimestamp: number | null;
  receivedTimestamp: number;
  processedTimestamp: number;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
  sequence?: number | string | null;
};
export type AdapterStatus = 'STARTING'|'CONNECTED'|'SYNCING'|'LIVE'|'RESYNCING'|'DISCONNECTED'|'ERROR';
export type ConnectionDiagnostics = { error:string|null; closeCode:number|null; closeReason:string|null; reconnectAttempt:number; lastEvent:string|null };
export interface ExchangeAdapter {
  readonly exchange: string;
  readonly symbol: string;
  start(): void;
  stop(): void;
  getSnapshot(): NormalizedOrderBook;
  getStatus(): AdapterStatus;
  getDiagnostics(): ConnectionDiagnostics;
  getReconnectCount(): number;
  getSequenceStatus(): string;
}
