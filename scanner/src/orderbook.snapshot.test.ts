import { describe, expect, it } from 'vitest';
import { LocalOrderBook } from './orderbook';

describe('bounded order-book snapshots', () => {
  it('returns the exact best levels without changing the full snapshot', () => {
    const book = new LocalOrderBook('binance', 'BTC/USDT');
    book.loadSnapshot(10, [
      { price: 99, quantity: 1 }, { price: 101, quantity: 2 }, { price: 100, quantity: 3 },
      { price: 98, quantity: 4 }, { price: 102, quantity: 5 },
    ], [
      { price: 104, quantity: 1 }, { price: 102, quantity: 2 }, { price: 103, quantity: 3 },
      { price: 105, quantity: 4 }, { price: 101, quantity: 5 },
    ], 1_000, 1_000);

    const fullBefore = book.snapshot;
    expect(book.getSnapshot(2).bids).toEqual([{ price: 102, quantity: 5 }, { price: 101, quantity: 2 }]);
    expect(book.getSnapshot(2).asks).toEqual([{ price: 101, quantity: 5 }, { price: 102, quantity: 2 }]);
    expect(book.snapshot).toEqual(fullBefore);

    expect(book.apply({ firstUpdateId: 11, finalUpdateId: 11,
      bids: [{ price: 101, quantity: 0 }, { price: 103, quantity: 6 }],
      asks: [{ price: 101, quantity: 0 }, { price: 100, quantity: 7 }], exchangeTimestamp: 1_100 }, 1_100)).toBe(true);
    expect(book.getSnapshot(2).bids).toEqual([{ price: 103, quantity: 6 }, { price: 102, quantity: 5 }]);
    expect(book.getSnapshot(2).asks).toEqual([{ price: 100, quantity: 7 }, { price: 102, quantity: 2 }]);
    expect(book.snapshot.bids).toHaveLength(5);
    expect(book.snapshot.asks).toHaveLength(5);
  });

  it('rejects invalid requested depth', () => {
    const book = new LocalOrderBook('binance', 'BTC/USDT');
    expect(() => book.getSnapshot(0)).toThrow('positive integer');
    expect(() => book.getSnapshot(1.5)).toThrow('positive integer');
  });
});
