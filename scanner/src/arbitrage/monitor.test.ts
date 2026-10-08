import { describe, expect, it } from 'vitest';
import { MarketDataCoordinator } from '../market-data/coordinator';
import type { LiveMarketSubscription } from '../market-data/universe';
import { ArbitrageMonitor } from './monitor';

const now = Date.now();
const subscriptions: LiveMarketSubscription[] = [
  { exchange: 'binance', canonicalAsset: 'BTC', canonicalQuote: 'USDT', canonicalPair: 'BTC/USDT', exchangeSymbol: 'BTCUSDT' },
  { exchange: 'bybit', canonicalAsset: 'BTC', canonicalQuote: 'USDT', canonicalPair: 'BTC/USDT', exchangeSymbol: 'BTCUSDT' },
];

describe('event-driven arbitrage monitor', () => {
  it('recalculates only on book changes and removes routes after invalidation', async () => {
    const coordinator = new MarketDataCoordinator(subscriptions);
    const monitor = new ArbitrageMonitor(coordinator, subscriptions, {
      feeRates: { binance: 0, bybit: 0 }, maxBookAgeMs: 60_000,
      minimumNetSpread: 0, minimumExecutableNotional: 1,
    });
    coordinator.setConnection('binance', 'CONNECTED');
    coordinator.setConnection('bybit', 'CONNECTED');
    coordinator.applySnapshot('binance', 'BTCUSDT', {
      sequence: 1, exchangeTimestamp: now, receivedTimestamp: now,
      bids: [{ price: 99, quantity: 1 }], asks: [{ price: 100, quantity: 1 }],
    });
    expect(monitor.snapshot(now).qualifyingOpportunityCount).toBe(0);
    coordinator.applySnapshot('bybit', 'BTCUSDT', {
      sequence: 1, exchangeTimestamp: now, receivedTimestamp: now,
      bids: [{ price: 105, quantity: 1 }], asks: [{ price: 106, quantity: 1 }],
    });
    await new Promise((resolve) => setTimeout(resolve, 70));
    expect(monitor.snapshot(now).opportunities[0]).toMatchObject({
      buyExchange: 'binance', sellExchange: 'bybit', referenceQuantity: 1, executableQuantity: 1,
      bestBuyAsk: 100, bestSellBid: 105, freshness: 'ACTIVE', estimatedNetProfit: 5,
    });
    expect(monitor.snapshot(now).profitabilityEvaluations).toBe(1);
    const recalculations = monitor.snapshot(now).pairRecalculations;
    coordinator.markSequenceGap('bybit', 'BTCUSDT', 2, 3, 'synthetic sequence gap');
    await new Promise((resolve) => setTimeout(resolve, 70));
    expect(monitor.snapshot(now).qualifyingOpportunityCount).toBe(0);
    expect(monitor.snapshot(now).pairRecalculations).toBe(recalculations + 1);
    monitor.stop();
  });

  it('coalesces rapid book changes per pair while evaluating the latest full book state', async () => {
    const coordinator = new MarketDataCoordinator(subscriptions);
    const monitor = new ArbitrageMonitor(coordinator, subscriptions, {
      feeRates: { binance: 0, bybit: 0 }, maxBookAgeMs: 60_000,
      minimumNetSpread: 0, minimumExecutableNotional: 1,
    });
    coordinator.setConnection('binance', 'CONNECTED');
    coordinator.setConnection('bybit', 'CONNECTED');
    coordinator.applySnapshot('binance', 'BTCUSDT', { sequence: 1, exchangeTimestamp: now, receivedTimestamp: now,
      bids: [{ price: 99, quantity: 1 }], asks: [{ price: 100, quantity: 1 }] });
    coordinator.applySnapshot('bybit', 'BTCUSDT', { sequence: 1, exchangeTimestamp: now, receivedTimestamp: now,
      bids: [{ price: 105, quantity: 1 }], asks: [{ price: 106, quantity: 1 }] });
    await new Promise((resolve) => setTimeout(resolve, 70));
    const before = monitor.snapshot(now);
    for (let sequence = 2; sequence <= 9; sequence += 1) {
      expect(coordinator.applyDelta('binance', 'BTCUSDT', { firstUpdateId: sequence, finalUpdateId: sequence,
        bids: [{ price: 99, quantity: sequence }], asks: [], exchangeTimestamp: now + sequence })).toBe(true);
    }
    await new Promise((resolve) => setTimeout(resolve, 70));
    const after = monitor.snapshot(now + 10);
    expect(after.bookChanges - before.bookChanges).toBe(8);
    expect(after.pairRecalculations - before.pairRecalculations).toBe(1);
    expect(after.processing.coalescedBookChanges - before.processing.coalescedBookChanges).toBe(7);
    expect(after.processing.stages.phaseCDetection.count).toBe(after.pairRecalculations);
    expect(after.opportunities[0]?.executableQuantity).toBe(1);
    monitor.stop();
  });
});
