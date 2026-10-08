import { describe, expect, it } from 'vitest';
import { createDiscoveryAdapters } from './adapters';
import { PublicApiError, fetchJson } from './http';

const replies: Record<string, unknown> = {
  'binance.info': { symbols: [{ symbol: 'BTCUSDT', baseAsset: 'BTC', quoteAsset: 'USDT', status: 'TRADING', isSpotTradingAllowed: true, filters: [] }, { symbol: 'BTCUSDT_PERP', baseAsset: 'BTC', quoteAsset: 'USDT', status: 'TRADING', isSpotTradingAllowed: true, filters: [] }] },
  'binance.ticker': [{ symbol: 'BTCUSDT', volume: '2', quoteVolume: '120000', lastPrice: '60000' }],
  'bybit.market': { retCode: 0, result: { list: [{ symbol: 'BTCUSDT', baseCoin: 'BTC', quoteCoin: 'USDT', status: 'Trading' }, { symbol: 'BTCUSDT-PERP', baseCoin: 'BTC', quoteCoin: 'USDT', status: 'PreLaunch' }] } },
  'bybit.ticker': { retCode: 0, result: { list: [{ symbol: 'BTCUSDT', volume24h: '2', turnover24h: '120000', lastPrice: '60000' }] } },
  'okx.market': { code: '0', data: [{ instType: 'SPOT', instId: 'BTC-USDT', baseCcy: 'BTC', quoteCcy: 'USDT', state: 'live', tickSz: '0.1', lotSz: '0.001' }, { instType: 'SWAP', instId: 'BTC-USDT-SWAP', baseCcy: 'BTC', quoteCcy: 'USDT', state: 'live' }] },
  'okx.ticker': { code: '0', data: [{ instId: 'BTC-USDT', vol24h: '2', volCcy24h: '120000', last: '60000' }] },
  'gate.market': [{ id: 'BTC_USDT', base: 'BTC', quote: 'USDT', trade_status: 'tradable', type: 'normal' }, { id: 'BTC_USDT', base: 'BTC', quote: 'USDT', trade_status: 'tradable', type: 'leveraged' }],
  'gate.ticker': [{ currency_pair: 'BTC_USDT', base_volume: '2', quote_volume: '120000', last: '60000' }],
  'mexc.market': { symbols: [{ symbol: 'BTCUSDT', baseAsset: 'BTC', quoteAsset: 'USDT', status: 1, isSpotTradingAllowed: true, filters: [] }, { symbol: 'BTCUSDT', baseAsset: 'BTC', quoteAsset: 'USDT', status: 0 }] },
  'mexc.ticker': [{ symbol: 'BTCUSDT', volume: '2', quoteVolume: '120000', lastPrice: '60000' }],
  'bitget.market': { code: '00000', data: [{ symbol: 'BTCUSDT', baseCoin: 'BTC', quoteCoin: 'USDT', status: 'online', pricePrecision: '1', quantityPrecision: '3' }, { symbol: 'BTCUSDT', baseCoin: 'BTC', quoteCoin: 'USDT', status: 'offline' }] },
  'bitget.ticker': { code: '00000', data: [{ symbol: 'BTCUSDT', baseVolume: '2', quoteVolume: '120000', lastPr: '60000' }] },
  'kucoin.market': { code: '200000', data: [{ symbol: 'BTC-USDT', baseCurrency: 'BTC', quoteCurrency: 'USDT', enableTrading: true }, { symbol: 'ETH-USDT', baseCurrency: 'ETH', quoteCurrency: 'USDT', enableTrading: false }] },
  'kucoin.ticker': { code: '200000', data: { ticker: [{ symbol: 'BTC-USDT', vol: '2', volValue: '120000', last: '60000' }] } },
  'htx.market': { status: 'ok', data: [{ symbol: 'btcusdt', 'base-currency': 'btc', 'quote-currency': 'usdt', state: 'online', 'api-trading': 'enabled' }, { symbol: 'ethusdt', 'base-currency': 'eth', 'quote-currency': 'usdt', state: 'suspended' }] },
  'htx.ticker': { status: 'ok', data: [{ symbol: 'btcusdt', amount: '2', vol: '120000', close: '60000' }] },
  'crypto.market': { code: 0, result: { data: [{ inst_type: 'CCY_PAIR', symbol: 'BTC_USDT', base_ccy: 'BTC', quote_ccy: 'USDT', tradable: true, product_type: 'DIGITAL_CURRENCIES' }, { inst_type: 'PERPETUAL_SWAP', symbol: 'BTC-PERP', base_ccy: 'BTC', quote_ccy: 'USDT', tradable: true }] } },
  'crypto.ticker': { code: 0, result: { data: [{ i: 'BTC_USDT', v: '2', vv: '120000', a: '60000', t: 1700000000 }] } },
  'coinbase.market': { products: [{ product_id: 'BTC-USDT', product_type: 'SPOT', base_currency_id: 'BTC', quote_currency_id: 'USDT', status: 'online', volume_24h: '2', approximate_quote_24h_volume: '120000', price: '60000' }], pagination: { has_next: false, next_cursor: '' } },
};

function fixtureFetch(url: string | URL | Request): Promise<Response> {
  const address = String(url);
  const key = address.includes('binance.vision') ? (address.includes('exchangeInfo') ? 'binance.info' : 'binance.ticker')
    : address.includes('bybit') ? (address.includes('instruments-info') ? 'bybit.market' : 'bybit.ticker')
    : address.includes('okx') ? (address.includes('/instruments?') ? 'okx.market' : 'okx.ticker')
    : address.includes('gateio') ? (address.includes('currency_pairs') ? 'gate.market' : 'gate.ticker')
    : address.includes('mexc') ? (address.includes('exchangeInfo') ? 'mexc.market' : 'mexc.ticker')
    : address.includes('bitget') ? (address.includes('/symbols') ? 'bitget.market' : 'bitget.ticker')
    : address.includes('kucoin') ? (address.includes('/symbols') ? 'kucoin.market' : 'kucoin.ticker')
    : address.includes('huobi') ? (address.includes('/symbols') ? 'htx.market' : 'htx.ticker')
    : address.includes('crypto.com') ? (address.includes('get-instruments') ? 'crypto.market' : 'crypto.ticker')
    : 'coinbase.market';
  return Promise.resolve(new Response(JSON.stringify(replies[key]), { status: 200, headers: { 'content-type': 'application/json' } }));
}

describe('public spot discovery adapters', () => {
  it('normalizes markets and ticker fields from mocked fixtures for all ten exchanges', async () => {
    const adapters = createDiscoveryAdapters({ fetchImpl: fixtureFetch as typeof fetch, retries: 0, now: () => 123 });
    const results = await Promise.all(adapters.map((adapter) => adapter.discover()));
    expect(results.map((result) => result.exchange)).toEqual(['binance','bybit','okx','gate','mexc','bitget','kucoin','htx','crypto.com','coinbase']);
    for (const result of results) {
      expect(result.status).toBe('AVAILABLE');
      expect(result.markets.length, result.exchange).toBeGreaterThan(0);
      expect(result.markets[0]).toMatchObject({ baseAsset: 'BTC', quoteAsset: 'USDT', marketType: 'spot', volumeQuote24h: 120000, lastPrice: 60000 });
    }
    expect(results.find((result) => result.exchange === 'bybit')?.markets.some((market) => market.exchangeSymbol.includes('PERP'))).toBe(false);
    expect(results.find((result) => result.exchange === 'crypto.com')?.markets.some((market) => market.exchangeSymbol.includes('PERP'))).toBe(false);
    expect(results.find((result) => result.exchange === 'coinbase')?.warnings).toEqual([]);
  });
});

describe('public JSON transport', () => {
  it('retains rate-limit status and HTTP status', async () => {
    const fetchImpl = async () => new Response('slow down', { status: 429 });
    await expect(fetchJson('https://example.test/api', { fetchImpl, retries: 0 })).rejects.toMatchObject<Partial<PublicApiError>>({ discoveryStatus: 'RATE_LIMITED', httpStatus: 429 });
  });
  it('does not silently accept malformed JSON', async () => {
    const fetchImpl = async () => new Response('{', { status: 200 });
    await expect(fetchJson('https://example.test/api', { fetchImpl, retries: 0 })).rejects.toThrow('Malformed JSON');
  });
});