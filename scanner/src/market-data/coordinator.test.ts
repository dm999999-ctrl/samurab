import { describe, expect, it } from 'vitest';
import { MarketDataCoordinator } from './coordinator';
import type { LiveMarketSubscription } from './universe';

const item: LiveMarketSubscription = { exchange: 'binance', canonicalAsset: 'BTC', canonicalQuote: 'USDT', canonicalPair: 'BTC/USDT', exchangeSymbol: 'BTCUSDT' };
describe('in-memory multi-market coordinator', () => {
  it('constructs sorted, depth-limited synchronized books with latency metrics', () => {
    const c = new MarketDataCoordinator([item], { depthLevels: 2 });
    c.setConnection('binance', 'CONNECTED');
    expect(c.applySnapshot('binance','BTCUSDT',{sequence:10,exchangeTimestamp:1000,receivedTimestamp:1010,bids:[{price:99,quantity:1},{price:100,quantity:2},{price:98,quantity:3}],asks:[{price:102,quantity:1},{price:101,quantity:2},{price:103,quantity:3}],})).toBe(true);
    expect(c.getBook('binance','BTCUSDT',1050,100).bids).toEqual([{price:100,quantity:2},{price:99,quantity:1}]);
    expect(c.getBook('binance','BTCUSDT',1050,100).asks).toEqual([{price:101,quantity:2},{price:102,quantity:1}]);
    expect(c.isUsable('binance','BTCUSDT',1050,100)).toBe(true);
    expect(c.applyDelta('binance','BTCUSDT',{firstUpdateId:11,finalUpdateId:11,bids:[{price:99,quantity:0},{price:98,quantity:4}],asks:[],exchangeTimestamp:1040})).toBe(true);
    expect(c.getBook('binance','BTCUSDT',1050,100).bids[0]).toEqual({price:100,quantity:2});
    const stats = c.getExchangeTelemetry('binance',1050,100);
    expect(stats.synchronizedBooks).toBe(1);
    expect(stats.quietBooks).toBe(0);
    expect(stats.messages).toBe(0);
    expect(stats.averageProcessingLatencyMs).not.toBeNull();
  });
  it('returns the current sequence without materializing an order-book snapshot', () => {
    const c = new MarketDataCoordinator([{ ...item, exchange: 'kucoin', exchangeSymbol: 'BTC-USDT' }]);
    c.setConnection('kucoin', 'CONNECTED');
    expect(c.sequence('kucoin', 'BTC-USDT')).toBeNull();
    c.applySnapshot('kucoin', 'BTC-USDT', { sequence: 42, exchangeTimestamp: 1000,
      bids: [{ price: 99, quantity: 1 }], asks: [{ price: 101, quantity: 1 }] });
    expect(c.sequence('kucoin', 'BTC-USDT')).toBe(42);
  });
  it('invalidates sequence gaps and never exposes the previous snapshot as usable', () => {
    const c = new MarketDataCoordinator([item]);
    c.setConnection('binance','CONNECTED');
    c.applySnapshot('binance','BTCUSDT',{sequence:10,exchangeTimestamp:1000,receivedTimestamp:1000,bids:[{price:99,quantity:1}],asks:[{price:101,quantity:1}]});
    expect(c.applyDelta('binance','BTCUSDT',{firstUpdateId:12,finalUpdateId:12,bids:[],asks:[],exchangeTimestamp:1100})).toBe(false);
    expect(c.getBook('binance','BTCUSDT',1100,500).status).toBe('RESYNCING');
    expect(c.isUsable('binance','BTCUSDT',1100,500)).toBe(false);
    expect(c.getExchangeTelemetry('binance',1100,500).sequenceGaps).toBe(1);
  });
  it('records exchange-native prevSeqId gaps and invalidates the existing book', () => {
    const c = new MarketDataCoordinator([{ ...item, exchange: 'okx', exchangeSymbol: 'BTC-USDT' }]);
    c.setConnection('okx', 'CONNECTED');
    c.applySnapshot('okx','BTC-USDT',{sequence:10,exchangeTimestamp:1000,receivedTimestamp:1000,bids:[{price:99,quantity:1}],asks:[{price:101,quantity:1}]});
    c.markSequenceGap('okx','BTC-USDT',8,12,'OKX prevSeqId mismatch');
    expect(c.getBook('okx','BTC-USDT',1100,500).status).toBe('RESYNCING');
    expect(c.isUsable('okx','BTC-USDT',1100,500)).toBe(false);
    expect(c.getExchangeTelemetry('okx',1100,500).sequenceGaps).toBe(1);
  });
  it('keeps a healthy old book QUIET and usable, but disconnect invalidates it', () => {
    const c = new MarketDataCoordinator([item]);
    c.setConnection('binance','CONNECTED');
    c.applySnapshot('binance','BTCUSDT',{sequence:1,exchangeTimestamp:1000,receivedTimestamp:1000,bids:[{price:99,quantity:1}],asks:[{price:101,quantity:1}]});
    const quiet = c.getBook('binance','BTCUSDT',1300,200);
    expect(quiet.status).toBe('QUIET');
    expect(quiet.feedHealth).toBe('HEALTHY');
    expect(quiet.synchronized).toBe(true);
    expect(c.isUsable('binance','BTCUSDT',1300,200)).toBe(true);
    const stats = c.getExchangeTelemetry('binance',1300,200);
    expect(stats.synchronizedBooks).toBe(0);
    expect(stats.quietBooks).toBe(1);
    c.setConnection('binance','DISCONNECTED');
    expect(c.getBook('binance','BTCUSDT',1300,200).status).toBe('DISCONNECTED');
    expect(c.isUsable('binance','BTCUSDT',1300,200)).toBe(false);
  });
  it('diagnoses quiet per-symbol streams and records automatic freshness recovery', () => {
    const c = new MarketDataCoordinator([item]);
    c.setConnection('binance','CONNECTED');
    const received = Date.now() - 5_000;
    c.recordExternalMessage('binance', 0, 'BTCUSDT');
    c.applySnapshot('binance','BTCUSDT',{sequence:10,exchangeTimestamp:received,receivedTimestamp:received,bids:[{price:99,quantity:1}],asks:[{price:101,quantity:1}]});
    const quiet = c.symbolDiagnostics('binance','BTCUSDT',Date.now(),2_000);
    expect(quiet.everSynchronized).toBe(true);
    expect(quiet.synchronizationState).toBe('QUIET');
    expect(quiet.stale).toBe(false);
    expect(quiet.quietSince).toBe(received + 2_000);
    expect(c.isUsable('binance','BTCUSDT',Date.now(),2_000)).toBe(true);
    expect(quiet.messagesReceived).toBe(1);
    expect(quiet.updatesApplied).toBe(0);
    expect(quiet.sequenceGaps).toBe(0);
    expect(c.applyDelta('binance','BTCUSDT',{firstUpdateId:11,finalUpdateId:11,bids:[],asks:[],exchangeTimestamp:Date.now()})).toBe(true);
    const recovered = c.symbolDiagnostics('binance','BTCUSDT',Date.now(),2_000);
    expect(recovered.stale).toBe(false);
    expect(recovered.quietRecoveries).toBe(1);
    expect(recovered.updatesApplied).toBe(1);
    expect(recovered.lastFailureReason).toBeNull();
  });
  it('does not misclassify a symbol without its first snapshot as stale', () => {
    const c = new MarketDataCoordinator([item]);
    c.setConnection('binance','CONNECTED');
    expect(c.getBook('binance','BTCUSDT',Date.now(),2_000).status).toBe('CONNECTED');
    expect(c.symbolDiagnostics('binance','BTCUSDT').pending).toBe(true);
    expect(c.symbolDiagnostics('binance','BTCUSDT').stale).toBe(false);
  });
  it('invalidates a quiet book immediately when feed health is lost', () => {
    const c = new MarketDataCoordinator([item]);
    c.setConnection('binance','CONNECTED');
    const now = Date.now();
    c.applySnapshot('binance','BTCUSDT',{sequence:10,exchangeTimestamp:now,receivedTimestamp:now,bids:[{price:99,quantity:1}],asks:[{price:101,quantity:1}]});
    expect(c.getBook('binance','BTCUSDT',now+5_000,2_000).status).toBe('QUIET');
    c.setFeedHealth('binance',false,now+5_000);
    expect(c.getBook('binance','BTCUSDT',now+5_001,2_000).status).toBe('UNHEALTHY');
    expect(c.getBook('binance','BTCUSDT',now+5_001,2_000).sequenceValid).toBe(false);
    expect(c.isUsable('binance','BTCUSDT',now+5_001,2_000)).toBe(false);
    c.setFeedHealth('binance',true,now+5_002);
    expect(c.getBook('binance','BTCUSDT',now+5_002,2_000).status).toBe('RESYNCING');
  });
  it('requires a new snapshot after disconnect/reconnect before reusing the old book', () => {
    const c = new MarketDataCoordinator([item]);
    c.setConnection('binance','CONNECTED');
    const now = Date.now();
    c.applySnapshot('binance','BTCUSDT',{sequence:10,exchangeTimestamp:now,receivedTimestamp:now,bids:[{price:99,quantity:1}],asks:[{price:101,quantity:1}]});
    c.setConnection('binance','DISCONNECTED');
    expect(c.isUsable('binance','BTCUSDT')).toBe(false);
    c.setConnection('binance','STARTING');
    c.setConnection('binance','CONNECTED');
    expect(c.getBook('binance','BTCUSDT').status).toBe('RESYNCING');
    expect(c.isUsable('binance','BTCUSDT')).toBe(false);
    c.applySnapshot('binance','BTCUSDT',{sequence:11,exchangeTimestamp:Date.now(),receivedTimestamp:Date.now(),bids:[{price:99,quantity:1}],asks:[{price:101,quantity:1}]});
    expect(c.getBook('binance','BTCUSDT').status).toBe('SYNCHRONIZED');
    expect(c.isUsable('binance','BTCUSDT')).toBe(true);
  });
  it('rejects malformed or crossed snapshots', () => {
    const c = new MarketDataCoordinator([item]);
    expect(c.applySnapshot('binance','BTCUSDT',{sequence:1,exchangeTimestamp:1000,bids:[{price:102,quantity:1}],asks:[{price:101,quantity:1}]})).toBe(true);
    expect(c.isUsable('binance','BTCUSDT',1000,1000)).toBe(false);
  });
});
