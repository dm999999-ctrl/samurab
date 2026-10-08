import { afterEach, describe, expect, it } from 'vitest';
import WebSocket, { WebSocketServer } from 'ws';
import { gzipSync } from 'node:zlib';
import type { LiveMarketSubscription } from './universe';
import { MarketDataCoordinator } from './coordinator';
import { LiveExchangeSessions } from './live-sessions';

const servers: WebSocketServer[] = [];
const activeSessions: LiveExchangeSessions[] = [];
const originalFetch = globalThis.fetch;
const oldBybitUrl = process.env.BYBIT_WS_URL;
const oldBinanceUrl = process.env.BINANCE_WS_URL;
const oldOkxUrl = process.env.OKX_WS_URL;
const oldKucoinTokenUrl = process.env.KUCOIN_PUBLIC_TOKEN_URL;
const oldKucoinRestUrl = process.env.KUCOIN_REST_BASE_URL;
const oldMexcWsUrl = process.env.MEXC_WS_URL;
const oldMexcRestUrl = process.env.MEXC_REST_BASE_URL;
const oldGateWsUrl = process.env.GATE_WS_URL;
const oldGateRestUrl = process.env.GATE_REST_BASE_URL;
const oldBitgetWsUrl = process.env.BITGET_WS_URL;
const oldHtxWsUrl = process.env.HTX_WS_URL;
const oldCryptoComWsUrl = process.env.CRYPTO_COM_WS_URL;
const oldCoinbaseWsUrl = process.env.COINBASE_WS_URL;

afterEach(async () => {
  for (const session of activeSessions.splice(0)) session.stop();
  await Promise.all(servers.splice(0).map((server) => new Promise<void>((resolve) => server.close(() => resolve()))));
  globalThis.fetch = originalFetch;
  restoreEnv('BYBIT_WS_URL', oldBybitUrl);
  restoreEnv('BINANCE_WS_URL', oldBinanceUrl);
  restoreEnv('OKX_WS_URL', oldOkxUrl);
  restoreEnv('KUCOIN_PUBLIC_TOKEN_URL', oldKucoinTokenUrl);
  restoreEnv('KUCOIN_REST_BASE_URL', oldKucoinRestUrl);
  restoreEnv('MEXC_WS_URL', oldMexcWsUrl);
  restoreEnv('MEXC_REST_BASE_URL', oldMexcRestUrl);
  restoreEnv('GATE_WS_URL', oldGateWsUrl);
  restoreEnv('GATE_REST_BASE_URL', oldGateRestUrl);
  restoreEnv('BITGET_WS_URL', oldBitgetWsUrl);
  restoreEnv('HTX_WS_URL', oldHtxWsUrl);
  restoreEnv('CRYPTO_COM_WS_URL', oldCryptoComWsUrl);
  restoreEnv('COINBASE_WS_URL', oldCoinbaseWsUrl);
});

describe('Phase B live-session protocol integration', () => {
  it('bootstraps Coinbase Advanced Trade L2, reconnects on envelope sequence gaps, and recovers', async () => {
    const server = await localServer(); process.env.COINBASE_WS_URL = server.url;
    let connections = 0;
    server.wss.on('connection', (socket) => {
      connections += 1; let subscribed = 0;
      socket.on('message', (raw) => {
        const request = JSON.parse(raw.toString()) as { type: string; product_ids?: string[]; channel?: string };
        if (request.type !== 'subscribe') return;
        subscribed += 1;
        expect(request.product_ids).toEqual(['BTC-USDT']);
        expect(request.channel).toBe('level2');
        socket.send(JSON.stringify({ channel: 'subscriptions', sequence_num: connections === 1 ? 99 : 1, events: [{}] }));
        if (connections === 1 && subscribed === 1) {
          socket.send(coinbaseSnapshot(100, [['100', '2']], [['101', '3']]));
          socket.send(coinbaseUpdate(101, [['buy', '100', '4']]));
          setTimeout(() => socket.send(coinbaseUpdate(103, [['buy', '100', '7']])), 20);
        } else if (connections === 2) {
          socket.send(coinbaseSnapshot(2, [['100', '6']], [['101', '3']]));
          socket.send(coinbaseUpdate(3, [['buy', '100', '8']]));
          setTimeout(() => socket.close(1012, 'deterministic Coinbase reconnect'), 50);
        } else {
          socket.send(coinbaseSnapshot(2, [['100', '9']], [['101', '3']]));
          socket.send(coinbaseUpdate(3, [['buy', '100', '10']]));
        }
      });
    });
    const item = subscription('coinbase'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => connections === 3 && coordinator.getBook('coinbase', 'BTC-USDT').bestBidQuantity === 10);
    const book = coordinator.getBook('coinbase', 'BTC-USDT');
    expect(book.status).toBe('SYNCHRONIZED'); expect(coordinator.isUsable('coinbase', 'BTC-USDT')).toBe(true);
    expect(coordinator.getExchangeTelemetry('coinbase').sequenceGaps).toBe(1);
    expect(coordinator.getExchangeTelemetry('coinbase').reconnects).toBe(2);
    expect(sessions.coinbaseSnapshotTelemetry()).toMatchObject({ pairsRequested: 1, snapshotsCompleted: 1,
      sequenceValidation: 'ADVANCED_TRADE_L2_DATA_SEQUENCE_NUM; GAP_INVALIDATES_ALL_BOOKS_AND_RECONNECTS', lastEnvelopeSequence: 3 });
    expect(sessions.coinbaseSequenceState('BTC-USDT')).toMatchObject({ source: 'LOCAL_ORDER', exchangeSequence: null });
  });

  it('bootstraps Crypto.com Spot, detects pu gaps, resubscribes, responds to heartbeat, and recovers after reconnect', async () => {
    const server = await localServer(); process.env.CRYPTO_COM_WS_URL = server.url;
    let connections = 0;
    server.wss.on('connection', (socket) => {
      connections += 1;
      let subscriptions = 0;
      socket.on('message', (raw) => {
        const request = JSON.parse(raw.toString()) as { id: number; method: string; params?: { channels?: string[]; book_subscription_type?: string; book_update_frequency?: number } };
        if (request.method === 'public/respond-heartbeat') return;
        if (request.method === 'unsubscribe') {
          socket.send(JSON.stringify({ id: request.id, method: 'unsubscribe', code: 0 }));
          return;
        }
        if (request.method !== 'subscribe') return;
        subscriptions += 1;
        expect(request.params).toMatchObject({ channels: ['book.BTC_USDT.50'], book_subscription_type: 'SNAPSHOT_AND_UPDATE', book_update_frequency: 100 });
        if (subscriptions === 1 && connections === 1) {
          socket.send(cryptoComBook('book', 100, [['100', '2']], [['101', '3']]));
          socket.send(cryptoComBook('book.update', 101, [['100', '4']], [], 100));
          socket.send(JSON.stringify({ id: 777, method: 'public/heartbeat', code: 0 }));
          setTimeout(() => socket.send(cryptoComBook('book.update', 103, [['100', '7']], [], 102)), 20);
        } else if (connections === 1) {
          socket.send(cryptoComBook('book', 200, [['100', '6']], [['101', '3']]));
          socket.send(cryptoComBook('book.update', 201, [['100', '8']], [], 200));
          setTimeout(() => socket.close(1012, 'deterministic Crypto.com reconnect'), 50);
        } else {
          socket.send(cryptoComBook('book', 300, [['100', '9']], [['101', '3']]));
          socket.send(cryptoComBook('book.update', 301, [['100', '10']], [], 300));
        }
      });
    });
    const item = subscription('crypto.com'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => connections === 2 && coordinator.sequence('crypto.com', 'BTC_USDT') === 301);
    const book = coordinator.getBook('crypto.com', 'BTC_USDT');
    expect(book).toMatchObject({ status: 'SYNCHRONIZED', bestBidQuantity: 10 });
    expect(coordinator.getExchangeTelemetry('crypto.com').sequenceGaps).toBe(1);
    expect(coordinator.getExchangeTelemetry('crypto.com').reconnects).toBe(1);
    expect(coordinator.isUsable('crypto.com', 'BTC_USDT')).toBe(true);
    expect(sessions.cryptoComSnapshotTelemetry()).toMatchObject({ pairsRequested: 1, snapshotsCompleted: 1 });
  });

  it('retains HTX deltas when a refresh snapshot needs a newer bridge point', async () => {
    const server = await localServer(); process.env.HTX_WS_URL = server.url;
    let refreshes = 0;
    server.wss.on('connection', (socket) => socket.on('message', (raw) => {
      const request = JSON.parse(raw.toString()) as { sub?: string; req?: string; id: string };
      if (request.sub) { socket.send(htxGzip({ id: request.id, status: 'ok', subbed: request.sub })); return; }
      if (!request.req) return;
      refreshes += 1;
      if (refreshes === 1) {
        socket.send(htxDeltaFrame('101', '100', [['100', '2']], []));
        socket.send(htxGzip({ id: request.id, rep: request.req, status: 'ok', data: { seqNum: 99, bids: [['100', '1']], asks: [['101', '3']] } }));
      } else {
        socket.send(htxGzip({ id: request.id, rep: request.req, status: 'ok', data: { seqNum: 100, bids: [['100', '1']], asks: [['101', '3']] } }));
      }
    }));
    const item = subscription('htx'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => coordinator.sequence('htx', 'BTCUSDT') === '101');
    expect(refreshes).toBeGreaterThanOrEqual(2);
    expect(coordinator.getBook('htx', 'BTCUSDT').status).toBe('SYNCHRONIZED');
    expect(coordinator.getExchangeTelemetry('htx')).toMatchObject({ sequenceGaps: 0, resynchronizations: 1 });
    expect(coordinator.isUsable('htx', 'BTCUSDT')).toBe(true);
  });

  it('bootstraps HTX MBP from buffered deltas, detects a prevSeqNum gap, resynchronizes, and recovers after reconnect', async () => {
    const server = await localServer(); process.env.HTX_WS_URL = server.url;
    let connections = 0;
    server.wss.on('connection', (socket) => {
      connections += 1;
      const connection = connections; let refreshes = 0;
      socket.on('message', (raw) => {
        const request = JSON.parse(raw.toString()) as { sub?: string; req?: string; id: string };
        if (request.sub) {
          expect(request.sub).toBe('market.btcusdt.mbp.150');
          socket.send(htxGzip({ id: request.id, status: 'ok', subbed: request.sub }));
          return;
        }
        if (!request.req) return;
        refreshes += 1;
        if (connection === 1 && refreshes === 1) {
          socket.send(htxDeltaFrame('101', '100', [['100', '2']], []));
          socket.send(htxGzip({ id: request.id, rep: request.req, status: 'ok', data: { seqNum: 100, bids: [['100', '1']], asks: [['101', '3']] } }));
          socket.send(htxDeltaFrame('102', '101', [['100', '3']], []));
          setTimeout(() => socket.send(htxDeltaFrame('104', '103', [['100', '4']], [])), 20);
        } else if (connection === 1) {
          socket.send(htxGzip({ id: request.id, rep: request.req, status: 'ok', data: { seqNum: 103, bids: [['100', '5']], asks: [['101', '3']] } }));
          socket.send(htxDeltaFrame('104', '103', [['100', '6']], []));
          setTimeout(() => socket.close(1000, 'reconnect test'), 40);
        } else {
          socket.send(htxDeltaFrame('201', '200', [['100', '7']], []));
          socket.send(htxGzip({ id: request.id, rep: request.req, status: 'ok', data: { seqNum: 200, bids: [['100', '6']], asks: [['101', '3']] } }));
          socket.send(htxDeltaFrame('202', '201', [['100', '8']], []));
        }
      });
    });
    const item = subscription('htx'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => connections === 2 && coordinator.sequence('htx', 'BTCUSDT') === '202');
    expect(coordinator.getBook('htx', 'BTCUSDT')).toMatchObject({ status: 'SYNCHRONIZED', bestBidQuantity: 8 });
    expect(coordinator.getExchangeTelemetry('htx').sequenceGaps).toBe(1);
    expect(coordinator.getExchangeTelemetry('htx').reconnects).toBe(1);
    expect(coordinator.isUsable('htx', 'BTCUSDT')).toBe(true);
    expect(sessions.htxSnapshotTelemetry()).toMatchObject({ pairsRequested: 1, snapshotsCompleted: 1 });
  });

  it('retains Gate notifications across a stale REST base and bridges on the newer snapshot', async () => {
    const server = await localServer(); process.env.GATE_WS_URL = server.url; process.env.GATE_REST_BASE_URL = 'https://mock.gate.test';
    let snapshots = 0;
    globalThis.fetch = async () => {
      snapshots += 1;
      if (snapshots === 1) await new Promise((resolve) => setTimeout(resolve, 25));
      return gateSnapshotResponse(snapshots === 1 ? 100 : 102, snapshots + 1);
    };
    server.wss.on('connection', (socket) => socket.on('message', (raw) => {
      const request = JSON.parse(raw.toString()) as { id: number; channel: string; event: string };
      if (request.channel !== 'spot.order_book_update' || request.event !== 'subscribe') return;
      socket.send(JSON.stringify({ id: request.id, channel: request.channel, event: 'subscribe', error: null, result: { status: 'success' } }));
      setTimeout(() => socket.send(gateDeltaFrame(103, 103, [['100', '4']], [])), 5);
    }));
    const item = subscription('gate'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => coordinator.sequence('gate', 'BTC_USDT') === 103);
    expect(snapshots).toBeGreaterThanOrEqual(2);
    expect(coordinator.getBook('gate', 'BTC_USDT')).toMatchObject({ status: 'SYNCHRONIZED', bestBidQuantity: 4 });
    expect(coordinator.getExchangeTelemetry('gate')).toMatchObject({ sequenceGaps: 0, resynchronizations: 1 });
    expect(coordinator.isUsable('gate', 'BTC_USDT')).toBe(true);
  });

  it('buffers Gate updates, reconciles the REST base ID, and applies overlapping absolute-amount ranges', async () => {
    const server = await localServer();
    process.env.GATE_WS_URL = server.url; process.env.GATE_REST_BASE_URL = 'https://mock.gate.test';
    globalThis.fetch = async () => gateSnapshotResponse(100, 2);
    server.wss.on('connection', (socket) => socket.on('message', (raw) => {
      const request = JSON.parse(raw.toString()) as { id: number; channel: string; event: string; payload: string[] };
      if (request.channel !== 'spot.order_book_update' || request.event !== 'subscribe') return;
      expect(request.payload).toEqual(['BTC_USDT', '100ms']);
      socket.send(JSON.stringify({ id: request.id, channel: request.channel, event: 'subscribe', error: null, result: { status: 'success' } }));
      socket.send(gateDeltaFrame(90, 100, [['100', '99']], [])); // before baseID+1, discard
      socket.send(gateDeltaFrame(98, 101, [['100', '4']], [['101', '0']])); // bridge baseID+1
      socket.send(gateDeltaFrame(101, 103, [['100', '5']], [['102', '1']])); // overlap covers next expected ID
      socket.send(gateDeltaFrame(104, 104, [], [['101', '2']]));
    }));
    const item = subscription('gate'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => coordinator.getBook('gate', 'BTC_USDT').sequence === 104);
    const book = coordinator.getBook('gate', 'BTC_USDT');
    expect(book.status).toBe('SYNCHRONIZED'); expect(book.bestBidQuantity).toBe(5); expect(book.bestAsk).toBe(101);
    expect(coordinator.getExchangeTelemetry('gate').sequenceGaps).toBe(0); expect(coordinator.isUsable('gate', 'BTC_USDT')).toBe(true);
  });

  it('invalidates a Gate sequence gap and recovers from a newer REST snapshot', async () => {
    const server = await localServer();
    process.env.GATE_WS_URL = server.url; process.env.GATE_REST_BASE_URL = 'https://mock.gate.test';
    let snapshots = 0;
    globalThis.fetch = async () => { snapshots += 1; return gateSnapshotResponse(snapshots === 1 ? 100 : 103, snapshots + 1); };
    server.wss.on('connection', (socket) => socket.on('message', (raw) => {
      const request = JSON.parse(raw.toString()) as { id: number; channel: string; event: string };
      if (request.channel !== 'spot.order_book_update' || request.event !== 'subscribe') return;
      socket.send(JSON.stringify({ id: request.id, channel: request.channel, event: 'subscribe', error: null, result: { status: 'success' } }));
      if (snapshots === 0) {
        setTimeout(() => socket.send(gateDeltaFrame(100, 101, [['100', '4']], [])), 10);
        setTimeout(() => socket.send(gateDeltaFrame(104, 105, [['100', '7']], [])), 60);
      }
    }));
    const item = subscription('gate'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => coordinator.getExchangeTelemetry('gate').sequenceGaps === 1
      && coordinator.getBook('gate', 'BTC_USDT').sequence === 105);
    expect(snapshots).toBeGreaterThanOrEqual(2); expect(coordinator.getBook('gate', 'BTC_USDT').status).toBe('SYNCHRONIZED');
    expect(coordinator.getBook('gate', 'BTC_USDT').bestBidQuantity).toBe(7); expect(coordinator.isUsable('gate', 'BTC_USDT')).toBe(true);
  });

  it('reconnects Gate and rebuilds the book from a new snapshot and stream bridge', async () => {
    const server = await localServer();
    process.env.GATE_WS_URL = server.url; process.env.GATE_REST_BASE_URL = 'https://mock.gate.test';
    let snapshots = 0, connections = 0;
    globalThis.fetch = async () => { snapshots += 1; return gateSnapshotResponse(snapshots === 1 ? 100 : 200, 2); };
    server.wss.on('connection', (socket) => {
      connections += 1;
      socket.on('message', (raw) => {
        const request = JSON.parse(raw.toString()) as { id: number; channel: string; event: string };
        if (request.channel !== 'spot.order_book_update' || request.event !== 'subscribe') return;
        socket.send(JSON.stringify({ id: request.id, channel: request.channel, event: 'subscribe', error: null, result: { status: 'success' } }));
        setTimeout(() => socket.send(gateDeltaFrame(snapshots === 1 ? 101 : 201, snapshots === 1 ? 101 : 201, [['100', '4']], [])), 15);
        if (connections === 1) setTimeout(() => socket.close(1012, 'deterministic Gate reconnect'), 80);
      });
    });
    const item = subscription('gate'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => coordinator.getExchangeTelemetry('gate').reconnects === 1 && coordinator.getBook('gate', 'BTC_USDT').sequence === 201);
    expect(connections).toBe(2); expect(coordinator.getBook('gate', 'BTC_USDT').status).toBe('SYNCHRONIZED');
    expect(coordinator.isUsable('gate', 'BTC_USDT')).toBe(true);
  });

  it('bootstraps Bitget from its WebSocket snapshot and applies exact pseq-linked absolute-quantity updates', async () => {
    const server = await localServer(); process.env.BITGET_WS_URL = server.url;
    server.wss.on('connection', (socket) => socket.on('message', (raw) => {
      const text = raw.toString();
      if (text === 'ping') { socket.send('pong'); return; }
      const request = JSON.parse(text) as { op: string; args: Array<{ instType: string; topic: string; symbol: string }> };
      expect(request).toMatchObject({ op: 'subscribe', args: [{ instType: 'spot', topic: 'books', symbol: 'BTCUSDT' }] });
      socket.send(JSON.stringify({ event: 'subscribe', arg: request.args[0], code: '0' }));
      socket.send(bitgetBookFrame('snapshot', '9007199254740993', '0', [['100', '2']], [['101', '3']]));
      socket.send(bitgetBookFrame('update', '9007199254740995', '9007199254740992', [['100', '4']], [['101', '0'], ['102', '3']]));
      socket.send(bitgetBookFrame('update', '9007199254740996', '9007199254740995', [['100', '5']], [['102', '2']]));
    }));
    const item = subscription('bitget'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => coordinator.sequence('bitget', 'BTCUSDT') === '9007199254740996');
    const book = coordinator.getBook('bitget', 'BTCUSDT');
    expect(book.sequence).toBe('9007199254740996'); expect(book.bestBidQuantity).toBe(5); expect(book.bestAsk).toBe(102);
    expect(book.status).toBe('SYNCHRONIZED'); expect(coordinator.getExchangeTelemetry('bitget').sequenceGaps).toBe(0);
    expect(coordinator.isUsable('bitget', 'BTCUSDT')).toBe(true);
    const diagnostic = sessions.bitgetConnectionDiagnostics().current!;
    expect(diagnostic).toMatchObject({ attemptNumber: 1, phase: 'SYNCHRONIZED',
      subscriptionRequest: { status: 'ACKNOWLEDGED', requestedChannels: 1, requestedSymbols: ['BTCUSDT'], acknowledgedChannels: 1, acknowledgedSymbols: ['BTCUSDT'], ackResponses: 1 },
      receivedMessages: 4, receivedBookMessages: 3 });
    expect(diagnostic.startedAt).toBeGreaterThan(0); expect(diagnostic.openedAt).toBeGreaterThanOrEqual(diagnostic.startedAt);
    expect(diagnostic.firstBookUpdateAt).toBeGreaterThan(0);
  });

  it('retains Bitget post-sync close diagnostics across reconnect and recovery', async () => {
    const server = await localServer(); process.env.BITGET_WS_URL = server.url;
    let connections = 0;
    server.wss.on('connection', (socket) => {
      connections += 1;
      socket.on('message', (raw) => {
        const text = raw.toString();
        if (text === 'ping') { socket.send('pong'); return; }
        const request = JSON.parse(text) as { op: string; args: Array<{ instType: string; topic: string; symbol: string }> };
        if (request.op !== 'subscribe') return;
        socket.send(JSON.stringify({ event: 'subscribe', arg: request.args[0], code: '0' }));
        if (connections === 1) {
          socket.send(bitgetBookFrame('snapshot', '100', '0', [['100', '2']], [['101', '3']]));
          setTimeout(() => socket.close(1012, 'deterministic remote close'), 20);
        } else {
          socket.send(bitgetBookFrame('snapshot', '200', '0', [['100', '6']], [['101', '3']]));
          socket.send(bitgetBookFrame('update', '201', '200', [['100', '7']], []));
        }
      });
    });
    const item = subscription('bitget'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => coordinator.getExchangeTelemetry('bitget').reconnects === 1 && coordinator.sequence('bitget', 'BTCUSDT') === '201');
    const diagnostics = sessions.bitgetConnectionDiagnostics();
    expect(diagnostics.history).toHaveLength(1);
    expect(diagnostics.history[0]).toMatchObject({ closeCode: 1012, closeReason: 'deterministic remote close',
      disconnectAfterFirstBookUpdate: true, synchronizedBooksAtDisconnect: 1, disconnectPhase: 'AFTER_SYNCHRONIZATION',
      receivedMessages: 2, receivedBookMessages: 1, reconnectAttempt: 2, reconnectReason: 'socket closed 1012: deterministic remote close',
      subscriptionRequest: { status: 'ACKNOWLEDGED', requestedChannels: 1, requestedSymbols: ['BTCUSDT'], acknowledgedChannels: 1, acknowledgedSymbols: ['BTCUSDT'] },
      sequenceStateAtDisconnect: { BTCUSDT: { sequence: '100', synchronized: true, sequenceGaps: 0, resynchronizations: 0 } } });
    expect(diagnostics.history[0].openedAt).toBeGreaterThan(0);
    expect(diagnostics.history[0].closedAt).toBeGreaterThan(diagnostics.history[0].openedAt!);
    expect(diagnostics.current).toMatchObject({ attemptNumber: 2, reasonForConnect: 'socket closed 1012: deterministic remote close', phase: 'SYNCHRONIZED' });
    expect(coordinator.isUsable('bitget', 'BTCUSDT')).toBe(true);
  });

  it('retains Bitget subscription-channel errors in session diagnostics', async () => {
    const server = await localServer(); process.env.BITGET_WS_URL = server.url;
    let connections = 0;
    server.wss.on('connection', (socket) => {
      connections += 1;
      socket.on('message', (raw) => {
        const text = raw.toString();
        if (text === 'ping') { socket.send('pong'); return; }
        const request = JSON.parse(text) as { op: string; args: Array<{ instType: string; topic: string; symbol: string }> };
        if (request.op !== 'subscribe') return;
        if (connections === 1) {
          socket.send(JSON.stringify({ event: 'error', arg: request.args[0], code: '30001', msg: 'test channel rejected' }));
          return;
        }
        socket.send(JSON.stringify({ event: 'subscribe', arg: request.args[0], code: '0' }));
        socket.send(bitgetBookFrame('snapshot', '300', '0', [['100', '2']], [['101', '3']]));
      });
    });
    const item = subscription('bitget'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => coordinator.getExchangeTelemetry('bitget').reconnects === 1 && coordinator.getBook('bitget', 'BTCUSDT').synchronized);
    const first = sessions.bitgetConnectionDiagnostics().history[0];
    expect(first).toMatchObject({ disconnectPhase: 'SUBSCRIBING', disconnectAfterFirstBookUpdate: false,
      subscriptionRequest: { status: 'ERROR', channelErrors: [{ event: 'error', symbol: 'BTCUSDT', message: '30001: test channel rejected' }] },
      lastError: 'Bitget error BTCUSDT: 30001: test channel rejected', reconnectAttempt: 2 });
    expect(sessions.snapshot()[0].errors.some((error) => error.includes('socket closed'))).toBe(true);
    expect(sessions.bitgetConnectionDiagnostics().history).toHaveLength(1);
  });

  it('invalidates a Bitget pseq gap and reconnects for a fresh snapshot and recovery', async () => {
    const server = await localServer(); process.env.BITGET_WS_URL = server.url;
    let connections = 0;
    server.wss.on('connection', (socket) => {
      connections += 1;
      socket.on('message', (raw) => {
        if (raw.toString() === 'ping') { socket.send('pong'); return; }
        const request = JSON.parse(raw.toString()) as { op: string; args: Array<{ symbol: string }> };
        if (request.op !== 'subscribe') return;
        const symbol = request.args[0].symbol;
        socket.send(JSON.stringify({ event: 'subscribe', arg: request.args[0], code: '0' }));
        if (connections === 1) {
          socket.send(bitgetBookFrame('snapshot', '100', '0', [['100', '2']], [['101', '3']]));
          setTimeout(() => socket.send(bitgetBookFrame('update', '103', '102', [['100', '4']], [])), 10);
        } else {
          socket.send(bitgetBookFrame('snapshot', '200', '0', [['100', '6']], [['101', '3']]));
          setTimeout(() => socket.send(bitgetBookFrame('update', '201', '200', [['100', '7']], [])), 10);
        }
      });
    });
    const item = subscription('bitget'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => coordinator.getExchangeTelemetry('bitget').reconnects === 1 && coordinator.sequence('bitget', 'BTCUSDT') === '201');
    expect(connections).toBe(2); expect(coordinator.getExchangeTelemetry('bitget').sequenceGaps).toBe(1);
    expect(coordinator.getBook('bitget', 'BTCUSDT').status).toBe('SYNCHRONIZED');
    expect(coordinator.getBook('bitget', 'BTCUSDT').bestBidQuantity).toBe(7); expect(coordinator.isUsable('bitget', 'BTCUSDT')).toBe(true);
    const first = sessions.bitgetConnectionDiagnostics().history[0];
    expect(first).toMatchObject({ disconnectPhase: 'SNAPSHOT_BOOTSTRAP', disconnectAfterFirstBookUpdate: true,
      receivedMessages: 3, receivedBookMessages: 2, synchronizedBooksAtDisconnect: 0,
      sequenceStateAtDisconnect: { BTCUSDT: { bootstrapState: 'RESYNCHRONIZING', sequence: '100', synchronized: false,
        sequenceGaps: 1, resynchronizations: 1, lastFailure: expect.stringContaining('Bitget sequence discontinuity') } } });
    expect(first.triggerReason).toContain('Bitget sequence discontinuity');
    expect(first.reconnectReason).toContain('Bitget sequence discontinuity');
  });

  it('buffers MEXC protobuf deltas, bridges the REST snapshot, ignores stale events, and applies contiguous versions', async () => {
    const server = await localServer();
    process.env.MEXC_WS_URL = server.url; process.env.MEXC_REST_BASE_URL = 'https://mock.mexc.test';
    let release!: () => void; const gate = new Promise<void>((resolve) => { release = resolve; });
    globalThis.fetch = async () => { await gate; return new Response(JSON.stringify({ lastUpdateId: 100, bids: [['100', '2']], asks: [['101', '3']] }), { status: 200 }); };
    server.wss.on('connection', (socket) => socket.on('message', async (raw) => {
      const request = JSON.parse(raw.toString()) as { id: number; method: string };
      if (request.method !== 'SUBSCRIPTION') return;
      socket.send(JSON.stringify({ id: request.id, code: 0, msg: 'OK' }));
      socket.send(mexcWireFrame(95, 100, [['100', '99']], []));
      socket.send(mexcWireFrame(98, 100, [['100', '4']], [])); // bridge ends exactly at snapshot version
      release();
      setTimeout(() => socket.send(mexcWireFrame(101, 103, [['100', '4']], [['101', '0']])), 15);
      setTimeout(() => socket.send(mexcWireFrame(104, 105, [['100', '5']], [['102', '1']])), 30);
    }));
    const item = subscription('mexc'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => coordinator.getBook('mexc', 'BTCUSDT').sequence === 105);
    const book = coordinator.getBook('mexc', 'BTCUSDT');
    expect(book.status).toBe('SYNCHRONIZED'); expect(book.bestBidQuantity).toBe(5); expect(book.bestAsk).toBe(102);
    expect(coordinator.getExchangeTelemetry('mexc').sequenceGaps).toBe(0); expect(coordinator.isUsable('mexc', 'BTCUSDT')).toBe(true);
  });

  it('invalidates a MEXC version gap and recovers only after a fresh snapshot bridge', async () => {
    const server = await localServer();
    process.env.MEXC_WS_URL = server.url; process.env.MEXC_REST_BASE_URL = 'https://mock.mexc.test';
    let snapshots = 0, firstGapSent = false, gapSocket: WebSocket | undefined;
    globalThis.fetch = async () => { snapshots += 1; const version = snapshots === 1 ? 100 : 200;
      return new Response(JSON.stringify({ lastUpdateId: version, bids: [['100', String(snapshots + 1)]], asks: [['101', '3']] }), { status: 200 }); };
    server.wss.on('connection', (socket) => { gapSocket = socket; socket.on('message', (raw) => {
      const request = JSON.parse(raw.toString()) as { id: number; method: string };
      if (request.method !== 'SUBSCRIPTION') return;
      socket.send(JSON.stringify({ id: request.id, code: 0 }));
      if (!firstGapSent) { firstGapSent = true; setTimeout(() => socket.send(mexcWireFrame(102, 103, [['100', '4']], [])), 10); }
    }); });
    const item = subscription('mexc'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => coordinator.getExchangeTelemetry('mexc').sequenceGaps >= 1);
    expect(coordinator.getBook('mexc', 'BTCUSDT').status).not.toBe('SYNCHRONIZED');
    await waitFor(() => snapshots >= 2);
    gapSocket!.send(mexcWireFrame(198, 202, [['100', '7']], []));
    await waitFor(() => coordinator.getBook('mexc', 'BTCUSDT').sequence === 202);
    expect(coordinator.getBook('mexc', 'BTCUSDT').status).toBe('SYNCHRONIZED');
    expect(coordinator.getBook('mexc', 'BTCUSDT').bestBidQuantity).toBe(7);
    expect(snapshots).toBeGreaterThanOrEqual(2); expect(coordinator.isUsable('mexc', 'BTCUSDT')).toBe(true);
  });

  it('reconnects MEXC and requires a new snapshot bridge before restoring usability', async () => {
    const server = await localServer();
    process.env.MEXC_WS_URL = server.url; process.env.MEXC_REST_BASE_URL = 'https://mock.mexc.test';
    let snapshots = 0, connections = 0;
    globalThis.fetch = async () => { snapshots += 1; return new Response(JSON.stringify({ lastUpdateId: snapshots === 1 ? 100 : 200,
      bids: [['100', '2']], asks: [['101', '3']] }), { status: 200 }); };
    server.wss.on('connection', (socket) => {
      connections += 1;
      socket.on('message', (raw) => {
        const request = JSON.parse(raw.toString()) as { id: number; method: string };
        if (request.method !== 'SUBSCRIPTION') return;
        socket.send(JSON.stringify({ id: request.id, code: 0 }));
        setTimeout(() => socket.send(mexcWireFrame(snapshots === 1 ? 99 : 199, snapshots === 1 ? 101 : 201, [['100', '4']], [])), 20);
        if (connections === 1) setTimeout(() => socket.close(1012, 'test reconnect'), 80);
      });
    });
    const item = subscription('mexc'), coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator); activeSessions.push(sessions); sessions.start();
    await waitFor(() => coordinator.getExchangeTelemetry('mexc').reconnects === 1 && coordinator.getBook('mexc', 'BTCUSDT').sequence === 201);
    expect(connections).toBe(2); expect(coordinator.getBook('mexc', 'BTCUSDT').status).toBe('SYNCHRONIZED');
    expect(coordinator.isUsable('mexc', 'BTCUSDT')).toBe(true);
  });

  it('applies Bybit stream snapshots and update IDs without requiring contiguous IDs', async () => {
    const server = await localServer();
    process.env.BYBIT_WS_URL = server.url;
    server.wss.on('connection', (socket) => socket.on('message', () => {
      socket.send(JSON.stringify({ op: 'subscribe', success: true }));
      socket.send(JSON.stringify({ topic: 'orderbook.50.BTCUSDT', type: 'snapshot', cts: Date.now(), data: {
        u: 100, b: [['100', '2']], a: [['101', '3']],
      } }));
      setTimeout(() => socket.send(JSON.stringify({ topic: 'orderbook.50.BTCUSDT', type: 'delta', cts: Date.now(), data: {
        u: 103, b: [['100', '4']], a: [],
      } })), 20);
    }));

    const coordinator = new MarketDataCoordinator([subscription('bybit')]);
    const sessions = new LiveExchangeSessions([subscription('bybit')], coordinator);
    activeSessions.push(sessions);
    sessions.start();
    await waitFor(() => coordinator.getBook('bybit', 'BTCUSDT').sequence === 103);
    const book = coordinator.getBook('bybit', 'BTCUSDT');
    expect(book.status).toBe('SYNCHRONIZED');
    expect(book.bestBid).toBe(100);
    expect(book.bestBidQuantity).toBe(4);
    expect(book.bestAsk).toBe(101);
    expect(coordinator.isUsable('bybit', 'BTCUSDT')).toBe(true);
  });

  it('bridges Binance buffered depth events over the REST snapshot update ID', async () => {
    const server = await localServer();
    process.env.BINANCE_WS_URL = server.url;
    globalThis.fetch = async () => new Response(JSON.stringify({
      lastUpdateId: 100, bids: [['100', '2']], asks: [['101', '3']],
    }), { status: 200, headers: { 'content-type': 'application/json' } });
    server.wss.on('connection', (socket) => {
      socket.send(JSON.stringify({ stream: 'btcusdt@depth@100ms', data: {
        U: 101, u: 102, E: Date.now(), b: [['100', '4']], a: [['101', '0']],
      } }));
      setTimeout(() => socket.send(JSON.stringify({ stream: 'btcusdt@depth@100ms', data: {
        U: 103, u: 103, E: Date.now(), b: [], a: [['102', '5']],
      } })), 30);
    });

    const coordinator = new MarketDataCoordinator([subscription('binance')]);
    const sessions = new LiveExchangeSessions([subscription('binance')], coordinator);
    activeSessions.push(sessions);
    sessions.start();
    await waitFor(() => coordinator.getBook('binance', 'BTCUSDT').sequence === 103);
    const book = coordinator.getBook('binance', 'BTCUSDT');
    expect(book.status).toBe('SYNCHRONIZED');
    expect(book.bestBid).toBe(100);
    expect(book.bestBidQuantity).toBe(4);
    expect(book.bestAsk).toBe(102);
    expect(coordinator.isUsable('binance', 'BTCUSDT')).toBe(true);
  });

  it('synchronizes OKX Spot snapshots, accepts linked non-contiguous seqIds, and resubscribes after a prevSeqId gap', async () => {
    const server = await localServer();
    process.env.OKX_WS_URL = server.url;
    let subscriptions = 0;
    server.wss.on('connection', (socket) => socket.on('message', (raw) => {
      const request = JSON.parse(raw.toString()) as { op: string; args: Array<{ channel: string; instId: string }> };
      if (request.op !== 'subscribe') return;
      subscriptions += 1;
      const instrumentId = request.args[0].instId;
      socket.send(JSON.stringify({ event: 'subscribe', arg: { channel: 'books', instId: instrumentId } }));
      const sequence = subscriptions === 1 ? 100 : 109;
      socket.send(JSON.stringify({ arg: { channel: 'books', instId: instrumentId }, action: 'snapshot', data: [{
        asks: [['101', '3', '3', '2']], bids: [['100', '2', '2', '1']], ts: String(Date.now()), seqId: sequence,
      }] }));
      if (subscriptions === 1) {
        setTimeout(() => socket.send(JSON.stringify({ arg: { channel: 'books', instId: instrumentId }, action: 'update', data: [{
          asks: [], bids: [['100', '4', '4', '1']], ts: String(Date.now()), seqId: 105, prevSeqId: 100,
        }] })), 15);
        setTimeout(() => socket.send(JSON.stringify({ arg: { channel: 'books', instId: instrumentId }, action: 'update', data: [{
          asks: [], bids: [['100', '5', '5', '1']], ts: String(Date.now()), seqId: 109, prevSeqId: 106,
        }] })), 35);
      }
    }));

    const item = subscription('okx');
    const coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator);
    activeSessions.push(sessions);
    sessions.start();
    await waitFor(() => coordinator.getBook('okx', 'BTC-USDT').sequence === 109
      && coordinator.getExchangeTelemetry('okx').sequenceGaps === 1);
    const book = coordinator.getBook('okx', 'BTC-USDT');
    expect(book.status).toBe('SYNCHRONIZED');
    expect(book.bestBidQuantity).toBe(2);
    expect(coordinator.isUsable('okx', 'BTC-USDT')).toBe(true);
    expect(coordinator.getExchangeTelemetry('okx').sequenceGaps).toBe(1);
    expect(subscriptions).toBe(2);
  });

  it('reconnects an OKX socket and requires the new subscription snapshot before reuse', async () => {
    const server = await localServer();
    process.env.OKX_WS_URL = server.url;
    let connections = 0;
    server.wss.on('connection', (socket) => {
      connections += 1;
      socket.on('message', (raw) => {
        const request = JSON.parse(raw.toString()) as { op: string; args: Array<{ channel: string; instId: string }> };
        if (request.op !== 'subscribe') return;
        const instrumentId = request.args[0].instId;
        socket.send(JSON.stringify({ event: 'subscribe', arg: { channel: 'books', instId: instrumentId } }));
        socket.send(JSON.stringify({ arg: { channel: 'books', instId: instrumentId }, action: 'snapshot', data: [{
          asks: [['101', '3']], bids: [['100', '2']], ts: String(Date.now()), seqId: connections === 1 ? 100 : 200,
        }] }));
        if (connections === 1) setTimeout(() => socket.close(1012, 'deterministic reconnect test'), 20);
      });
    });

    const item = subscription('okx');
    const coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator);
    activeSessions.push(sessions);
    sessions.start();
    await waitFor(() => coordinator.getExchangeTelemetry('okx').reconnects === 1
      && coordinator.getBook('okx', 'BTC-USDT').sequence === 200);
    expect(connections).toBe(2);
    expect(coordinator.getBook('okx', 'BTC-USDT').status).toBe('SYNCHRONIZED');
    expect(coordinator.isUsable('okx', 'BTC-USDT')).toBe(true);
  });

  it('buffers KuCoin deltas until the REST snapshot arrives, then replays overlapping sequence ranges', async () => {
    const server = await localServer();
    kucoinTestEndpoints(server.url);
    let releaseSnapshot!: () => void;
    const snapshotGate = new Promise<void>((resolve) => { releaseSnapshot = resolve; });
    globalThis.fetch = async (input) => {
      if (String(input).includes('/bullet-public')) return kucoinTokenResponse(server.url);
      await snapshotGate;
      return kucoinSnapshotResponse('BTC-USDT', 100, 2);
    };
    server.wss.on('connection', (socket) => {
      socket.send(JSON.stringify({ id: 'connect-1', type: 'welcome' }));
      socket.on('message', (raw) => {
        const request = JSON.parse(raw.toString()) as { id: string; type: string; topic: string };
        if (request.type !== 'subscribe') return;
        socket.send(JSON.stringify({ id: request.id, type: 'ack' }));
        socket.send(kucoinDeltaFrame(100, 100, [['100', '9', '4000']])); // at snapshot sequence: discard
        socket.send(kucoinDeltaFrame(100, 101, [['100', '4', '9000']])); // overlapping range: apply
        socket.send(kucoinDeltaFrame(101, 103, [['100', '5', '1']])); // per-price sequence is not the message sequence
        releaseSnapshot();
      });
    });

    const item = subscription('kucoin');
    const coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator);
    activeSessions.push(sessions);
    sessions.start();
    await waitFor(() => coordinator.getBook('kucoin', 'BTC-USDT').sequence === 103);
    const book = coordinator.getBook('kucoin', 'BTC-USDT');
    expect(book.status).toBe('SYNCHRONIZED');
    expect(book.bestBidQuantity).toBe(5);
    expect(coordinator.getExchangeTelemetry('kucoin').sequenceGaps).toBe(0);
    expect(coordinator.isUsable('kucoin', 'BTC-USDT')).toBe(true);
  });

  it('detects a KuCoin sequence gap and recovers only from a newer REST snapshot', async () => {
    const server = await localServer();
    kucoinTestEndpoints(server.url);
    let snapshots = 0;
    globalThis.fetch = async (input) => {
      if (String(input).includes('/bullet-public')) return kucoinTokenResponse(server.url);
      snapshots += 1;
      return kucoinSnapshotResponse('BTC-USDT', snapshots === 1 ? 100 : 106, snapshots === 1 ? 2 : 6);
    };
    let socket: WebSocket | undefined;
    server.wss.on('connection', (client) => {
      socket = client;
      client.send(JSON.stringify({ id: 'connect-1', type: 'welcome' }));
      client.on('message', (raw) => {
        const request = JSON.parse(raw.toString()) as { id: string; type: string };
        if (request.type === 'subscribe') client.send(JSON.stringify({ id: request.id, type: 'ack' }));
      });
    });

    const item = subscription('kucoin');
    const coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator);
    activeSessions.push(sessions);
    sessions.start();
    await waitFor(() => coordinator.getBook('kucoin', 'BTC-USDT').sequence === 100);
    socket!.send(kucoinDeltaFrame(105, 106, [['100', '8', '7000']]));
    await waitFor(() => coordinator.getBook('kucoin', 'BTC-USDT').sequence === 106
      && coordinator.getExchangeTelemetry('kucoin').sequenceGaps === 1);
    expect(snapshots).toBe(2);
    expect(coordinator.getBook('kucoin', 'BTC-USDT').status).toBe('SYNCHRONIZED');
    expect(coordinator.getBook('kucoin', 'BTC-USDT').bestBidQuantity).toBe(6);
    expect(coordinator.isUsable('kucoin', 'BTC-USDT')).toBe(true);
  });

  it('reconnects KuCoin with a fresh public token and resubscribes for a new snapshot', async () => {
    const server = await localServer();
    kucoinTestEndpoints(server.url);
    let tokenRequests = 0, connections = 0;
    globalThis.fetch = async (input) => {
      if (String(input).includes('/bullet-public')) { tokenRequests += 1; return kucoinTokenResponse(server.url, `token-${tokenRequests}`); }
      return kucoinSnapshotResponse('BTC-USDT', connections === 1 ? 100 : 200, 2);
    };
    server.wss.on('connection', (client) => {
      connections += 1;
      client.send(JSON.stringify({ id: `connect-${connections}`, type: 'welcome' }));
      client.on('message', (raw) => {
        const request = JSON.parse(raw.toString()) as { id: string; type: string };
        if (request.type !== 'subscribe') return;
        client.send(JSON.stringify({ id: request.id, type: 'ack' }));
        if (connections === 1) setTimeout(() => client.close(1012, 'deterministic KuCoin reconnect'), 30);
      });
    });

    const item = subscription('kucoin');
    const coordinator = new MarketDataCoordinator([item]);
    const sessions = new LiveExchangeSessions([item], coordinator);
    activeSessions.push(sessions);
    sessions.start();
    await waitFor(() => coordinator.getExchangeTelemetry('kucoin').reconnects === 1
      && coordinator.getBook('kucoin', 'BTC-USDT').sequence === 200);
    expect(tokenRequests).toBe(2);
    expect(connections).toBe(2);
    expect(coordinator.getBook('kucoin', 'BTC-USDT').status).toBe('SYNCHRONIZED');
    expect(coordinator.isUsable('kucoin', 'BTC-USDT')).toBe(true);
  });
});

function subscription(exchange: 'binance' | 'bybit' | 'okx' | 'kucoin' | 'mexc' | 'gate' | 'bitget' | 'htx' | 'crypto.com' | 'coinbase'): LiveMarketSubscription {
  return { exchange, canonicalAsset: 'BTC', canonicalQuote: 'USDT', canonicalPair: 'BTC/USDT',
    exchangeSymbol: exchange === 'okx' || exchange === 'kucoin' || exchange === 'coinbase' ? 'BTC-USDT' : exchange === 'gate' || exchange === 'crypto.com' ? 'BTC_USDT' : 'BTCUSDT' };
}
function cryptoComBook(channel: 'book' | 'book.update', u: number, bids: unknown[], asks: unknown[], pu?: number) {
  const data = channel === 'book' ? { t: Date.now(), tt: Date.now(), u, bids, asks }
    : { t: Date.now(), tt: Date.now(), u, pu, update: { bids, asks } };
  return JSON.stringify({ id: -1, method: 'subscribe', code: 0, result: { channel, instrument_name: 'BTC_USDT',
    subscription: 'book.BTC_USDT.50', data: [data] } });
}
function coinbaseSnapshot(sequence: number | null, bids: unknown[], asks: unknown[]) {
  const updates = [...(bids as string[][]).map(([price_level, new_quantity]) => ({ side: 'bid', price_level, new_quantity })),
    ...(asks as string[][]).map(([price_level, new_quantity]) => ({ side: 'offer', price_level, new_quantity }))];
  return JSON.stringify({ channel: 'l2_data', sequence_num: sequence ?? 1, timestamp: new Date().toISOString(), events: [
    { type: 'snapshot', product_id: 'BTC-USDT', updates },
  ] });
}
function coinbaseUpdate(sequence: number | null, changes: unknown[][]) {
  const updates = changes.map(([side, price_level, new_quantity]) => ({ side: side === 'buy' ? 'bid' : 'offer', price_level, new_quantity }));
  return JSON.stringify({ channel: 'l2_data', sequence_num: sequence ?? 1, timestamp: new Date().toISOString(), events: [
    { type: 'update', product_id: 'BTC-USDT', updates },
  ] });
}
async function localServer(): Promise<{ wss: WebSocketServer; url: string }> {
  const wss = new WebSocketServer({ port: 0 });
  servers.push(wss);
  await new Promise<void>((resolve) => wss.once('listening', resolve));
  const address = wss.address();
  if (!address || typeof address === 'string') throw new Error('expected local WebSocket TCP address');
  return { wss, url: `ws://127.0.0.1:${address.port}` };
}
async function waitFor(predicate: () => boolean) {
  const until = Date.now() + 10_000;
  while (!predicate() && Date.now() < until) await new Promise((resolve) => setTimeout(resolve, 10));
  expect(predicate()).toBe(true);
}
function restoreEnv(name: string, value: string | undefined) {
  if (value === undefined) delete process.env[name]; else process.env[name] = value;
}
function kucoinTestEndpoints(wsUrl: string) {
  process.env.KUCOIN_PUBLIC_TOKEN_URL = 'https://mock.kucoin.test/api/v1/bullet-public';
  process.env.KUCOIN_REST_BASE_URL = 'https://mock.kucoin.test';
  void wsUrl;
}
function kucoinTokenResponse(endpoint: string, token = 'test-token') {
  return new Response(JSON.stringify({ code: '200000', data: { token, instanceServers: [{ endpoint, pingInterval: 60_000, pingTimeout: 10_000 }] } }), { status: 200 });
}
function kucoinSnapshotResponse(symbol: string, sequence: number, bidQuantity: number) {
  return new Response(JSON.stringify({ code: '200000', data: { symbol, sequence: String(sequence), time: Date.now(),
    bids: [['100', String(bidQuantity)]], asks: [['101', '3']] } }), { status: 200 });
}
function kucoinDeltaFrame(sequenceStart: number, sequenceEnd: number, bids: string[][]) {
  return JSON.stringify({ type: 'message', topic: '/market/level2:BTC-USDT', subject: 'trade.l2update', data: {
    symbol: 'BTC-USDT', sequenceStart, sequenceEnd, time: Date.now(), changes: { bids, asks: [] },
  } });
}
function protobufVarint(value: number): Uint8Array {
  const bytes: number[] = []; let number = BigInt(value);
  while (number >= 128n) { bytes.push(Number(number & 127n) | 128); number >>= 7n; }
  bytes.push(Number(number)); return Uint8Array.from(bytes);
}
function protobufJoin(...items: Uint8Array[]) { const output = new Uint8Array(items.reduce((length, item) => length + item.length, 0));
  let offset = 0; for (const item of items) { output.set(item, offset); offset += item.length; } return output; }
function protobufField(number: number, value: Uint8Array, wire: 0 | 2 = 2) {
  const tag = protobufVarint(number * 8 + wire);
  return wire === 0 ? protobufJoin(tag, value) : protobufJoin(tag, protobufVarint(value.length), value);
}
function protobufText(number: number, value: string) { return protobufField(number, new TextEncoder().encode(value)); }
function mexcWireFrame(from: number, to: number, bids: string[][], asks: string[][]) {
  const row = (values: string[]) => protobufJoin(protobufText(1, values[0]), protobufText(2, values[1]));
  const content = protobufJoin(...asks.map((value) => protobufField(1, row(value))), ...bids.map((value) => protobufField(2, row(value))),
    protobufText(4, String(from)), protobufText(5, String(to)));
  const stamp = protobufVarint(Date.now());
  return protobufJoin(protobufText(1, 'spot@public.aggre.depth.v3.api.pb@100ms'), protobufText(3, 'BTCUSDT'),
    protobufField(6, stamp, 0), protobufField(313, content));
}
function gateSnapshotResponse(id: number, bidQuantity: number) {
  return new Response(JSON.stringify({ id, current: Date.now(), update: Date.now(), bids: [['100', String(bidQuantity)]], asks: [['101', '3']] }), { status: 200 });
}
function gateDeltaFrame(first: number, last: number, bids: string[][], asks: string[][]) {
  return JSON.stringify({ time: Math.floor(Date.now() / 1_000), time_ms: Date.now(), channel: 'spot.order_book_update', event: 'update',
    result: { t: Date.now(), s: 'BTC_USDT', U: first, u: last, b: bids, a: asks } });
}
function bitgetBookFrame(action: 'snapshot' | 'update', sequence: string, previousSequence: string,
  bids: string[][], asks: string[][]) {
  return JSON.stringify({ action, arg: { instType: 'spot', topic: 'books', symbol: 'BTCUSDT' },
    data: [{ seq: sequence, pseq: previousSequence, ts: Date.now(), b: bids, a: asks }] });
}
function htxGzip(value: unknown) { return gzipSync(Buffer.from(JSON.stringify(value))); }
function htxDeltaFrame(sequence: string, previousSequence: string, bids: string[][], asks: string[][]) {
  return htxGzip({ ch: 'market.btcusdt.mbp.150', ts: Date.now(), tick: { seqNum: sequence, prevSeqNum: previousSequence, bids, asks } });
}
