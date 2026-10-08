import type { OrderBookState } from './types';

export type UsableBookPolicy = {
  connected: boolean;
};

export function isUsableOrderBook(book: OrderBookState, policy: UsableBookPolicy): boolean {
  if (!policy.connected || book.feedHealth !== 'HEALTHY' || !book.synchronized || book.stale
    || (book.status !== 'SYNCHRONIZED' && book.status !== 'QUIET')) return false;
  if (!book.sequenceValid || !Number.isFinite(book.receivedTimestamp) || !Number.isFinite(book.processedTimestamp)) return false;
  if (!Number.isFinite(book.exchangeTimestamp) || book.exchangeTimestamp === null || book.exchangeTimestamp <= 0) return false;
  if (!book.bids.length || !book.asks.length) return false;
  if (!book.bids.every(validLevel) || !book.asks.every(validLevel)) return false;
  if (book.bids[0].price !== book.bestBid || book.bids[0].quantity !== book.bestBidQuantity) return false;
  if (book.asks[0].price !== book.bestAsk || book.asks[0].quantity !== book.bestAskQuantity) return false;
  if (!(book.bestBid! > 0 && book.bestAsk! > 0) || book.bestBid! >= book.bestAsk!) return false;
  return true;
}

function validLevel(level: { price: number; quantity: number }): boolean {
  return Number.isFinite(level.price) && level.price > 0
    && Number.isFinite(level.quantity) && level.quantity > 0;
}
