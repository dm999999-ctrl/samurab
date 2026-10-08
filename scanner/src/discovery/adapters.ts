import { fetchJson, errorHealth, type JsonRequestOptions } from './http';
import { list, makeMarket, makeTickers, mergeTicker, numeric, object, precision, rowsAt, text, type MarketDraft, type TickerDraft } from './parse-utils';
import type { DiscoveryStatus, ExchangeDiscoveryResult, ExchangeId, NormalizedSpotMarket, SourceHealth } from './types';

export interface SpotDiscoveryAdapter { readonly exchange: ExchangeId; discover(): Promise<ExchangeDiscoveryResult>; }
type ParsedTicker = Omit<TickerDraft, 'symbol'>;
type AdapterDefinition = {
  exchange: ExchangeId;
  marketEndpoint: string;
  tickerEndpoint: string;
  fetchMarkets: (get: <T>(url: string) => Promise<T>) => Promise<unknown[]>;
  parseMarkets: (payload: unknown) => MarketDraft[];
  fetchTickers: (get: <T>(url: string) => Promise<T>) => Promise<unknown[]>;
  parseTickers: (rows: unknown[]) => Map<string, TickerDraft>;
};
type AdapterOptions = JsonRequestOptions & { now?: () => number; };

export function createDiscoveryAdapters(options: AdapterOptions = {}): SpotDiscoveryAdapter[] {
  const get = <T>(url: string) => fetchJson<T>(url, options);
  const definitions = definitionsFor(get);
  return definitions.map((definition) => createAdapter(definition, get, options.now ?? Date.now));
}
function createAdapter(definition: AdapterDefinition, get: <T>(url: string) => Promise<T>, now: () => number): SpotDiscoveryAdapter {
  return {
    exchange: definition.exchange,
    async discover() {
      const started = now();
      let rawMarkets: unknown[];
      let markets: MarketDraft[];
      try {
        rawMarkets = await definition.fetchMarkets(get);
        markets = definition.parseMarkets(rawMarkets);
      } catch (error) {
        const marketSource = errorHealth(error, definition.marketEndpoint) as SourceHealth;
        return { exchange: definition.exchange, status: marketSource.status, discoveredAt: now(), marketCount: 0, markets: [], marketSource, warnings: [marketSource.error ?? 'Market metadata request failed.'], durationMs: now() - started };
      }
      const marketSource: SourceHealth = { status: markets.length ? 'AVAILABLE' : 'EMPTY_RESULT', endpoint: definition.marketEndpoint };
      if (!markets.length) {
        return { exchange: definition.exchange, status: 'EMPTY_RESULT', discoveredAt: now(), marketCount: 0, markets: [], marketSource, warnings: [], durationMs: now() - started };
      }
      let tickerSource: SourceHealth;
      let tickers = new Map<string, TickerDraft>();
      const warnings: string[] = [];
      try {
        tickers = definition.parseTickers(await definition.fetchTickers(get));
        tickerSource = { status: tickers.size ? 'AVAILABLE' : 'EMPTY_RESULT', endpoint: definition.tickerEndpoint };
      } catch (error) {
        tickerSource = errorHealth(error, definition.tickerEndpoint) as SourceHealth;
        warnings.push(`Ticker/liquidity data incomplete: ${tickerSource.error ?? tickerSource.status}`);
      }
      const normalized: NormalizedSpotMarket[] = markets.map((market) => {
        const merged = mergeTicker(market, tickers);
        return {
          ...merged,
          canonicalAssetId: `asset:${merged.baseAsset}`,
          canonicalPair: `${merged.baseAsset}/${merged.quoteAsset}`,
          assetMapping: 'exact-symbol',
        };
      });
      return {
        exchange: definition.exchange,
        status: 'AVAILABLE',
        discoveredAt: now(),
        marketCount: normalized.length,
        markets: normalized,
        marketSource,
        tickerSource,
        warnings,
        durationMs: now() - started,
      };
    },
  };
}

function definitionsFor(get: <T>(url: string) => Promise<T>): AdapterDefinition[] {
  const binanceMarket = 'https://data-api.binance.vision/api/v3/exchangeInfo';
  const binanceTicker = 'https://data-api.binance.vision/api/v3/ticker/24hr';
  const bybitMarket = 'https://api.bybit.com/v5/market/instruments-info?category=spot';
  const bybitTicker = 'https://api.bybit.com/v5/market/tickers?category=spot';
  const okxMarket = 'https://www.okx.com/api/v5/public/instruments?instType=SPOT';
  const okxTicker = 'https://www.okx.com/api/v5/market/tickers?instType=SPOT';
  const gateMarket = 'https://api.gateio.ws/api/v4/spot/currency_pairs';
  const gateTicker = 'https://api.gateio.ws/api/v4/spot/tickers';
  const mexcMarket = 'https://api.mexc.com/api/v3/exchangeInfo';
  const mexcTicker = 'https://api.mexc.com/api/v3/ticker/24hr';
  const bitgetMarket = 'https://api.bitget.com/api/v2/spot/public/symbols';
  const bitgetTicker = 'https://api.bitget.com/api/v2/spot/market/tickers';
  const kucoinMarket = 'https://api.kucoin.com/api/v2/symbols';
  const kucoinTicker = 'https://api.kucoin.com/api/v1/market/allTickers';
  const htxMarket = 'https://api.huobi.pro/v1/common/symbols';
  const htxTicker = 'https://api.huobi.pro/market/tickers';
  const cryptoMarket = 'https://api.crypto.com/exchange/v1/public/get-instruments';
  const cryptoTicker = 'https://api.crypto.com/exchange/v1/public/get-tickers';
  const coinbaseMarket = 'https://api.coinbase.com/api/v3/brokerage/market/products?product_type=SPOT&limit=250';
  const coinbaseTicker = coinbaseMarket;

  return [
    {
      exchange: 'binance', marketEndpoint: binanceMarket, tickerEndpoint: binanceTicker,
      fetchMarkets: async (request) => list((await request<any>(binanceMarket)).symbols, 'Binance exchangeInfo symbols'),
      parseMarkets: (rows) => list(rows, 'Binance symbols').flatMap((item) => {
        const r = object(item, 'Binance symbol');
        if (r.status !== 'TRADING' || r.isSpotTradingAllowed === false || (Array.isArray(r.permissions) && r.permissions.length > 0 && !r.permissions.includes('SPOT'))) return [];
        const filters = Array.isArray(r.filters) ? r.filters : [];
        const price = filters.find((x: any) => x.filterType === 'PRICE_FILTER');
        const lot = filters.find((x: any) => x.filterType === 'LOT_SIZE');
        const notional = filters.find((x: any) => x.filterType === 'NOTIONAL' || x.filterType === 'MIN_NOTIONAL');
        const m = makeMarket('binance', r.symbol, r.baseAsset, r.quoteAsset, r.status, {
          pricePrecision: numeric(r.quoteAssetPrecision), quantityPrecision: numeric(r.baseAssetPrecision),
          ...(numeric(price?.tickSize) ? { tickSize: numeric(price.tickSize) } : {}),
          ...(numeric(lot?.stepSize) ? { lotSize: numeric(lot.stepSize) } : {}),
          ...(numeric(lot?.minQty) ? { minimumQuantity: numeric(lot.minQty) } : {}),
          ...(numeric(notional?.minNotional) ? { minimumNotional: numeric(notional.minNotional) } : {}),
        });
        return m ? [m] : [];
      }),
      fetchTickers: async (request) => list(await request<unknown>(binanceTicker), 'Binance 24h tickers'),
      parseTickers: (rows) => makeTickers(rows, 'symbol', (r) => ({
        volumeBase24h: numeric(r.volume), volumeQuote24h: numeric(r.quoteVolume), lastPrice: numeric(r.lastPrice), exchangeTimestamp: numeric(r.closeTime),
      })),
    },
    {
      exchange: 'bybit', marketEndpoint: bybitMarket, tickerEndpoint: bybitTicker,
      fetchMarkets: async (request) => { const p = object(await request(bybitMarket), 'Bybit instruments'); assertEnvelope(p.retCode === 0, 'Bybit retCode'); return list(p.result?.list, 'Bybit spot instrument list'); },
      parseMarkets: (rows) => list(rows, 'Bybit instruments').flatMap((item) => {
        const r = object(item, 'Bybit instrument'); if (r.status !== 'Trading') return [];
        const lot = r.lotSizeFilter ?? {}, price = r.priceFilter ?? {};
        const m = makeMarket('bybit', r.symbol, r.baseCoin, r.quoteCoin, r.status, {
          ...(precision(price.tickSize) !== undefined ? { pricePrecision: precision(price.tickSize) } : {}),
          ...(precision(lot.basePrecision) !== undefined ? { quantityPrecision: precision(lot.basePrecision) } : {}),
          ...(numeric(price.tickSize) ? { tickSize: numeric(price.tickSize) } : {}),
          ...(numeric(lot.qtyStep) ? { lotSize: numeric(lot.qtyStep) } : {}),
          ...(numeric(lot.minOrderQty) ? { minimumQuantity: numeric(lot.minOrderQty) } : {}),
          ...(numeric(lot.minOrderAmt) ? { minimumNotional: numeric(lot.minOrderAmt) } : {}),
        });
        return m ? [m] : [];
      }),
      fetchTickers: async (request) => { const p = object(await request(bybitTicker), 'Bybit tickers'); assertEnvelope(p.retCode === 0, 'Bybit ticker retCode'); return list(p.result?.list, 'Bybit spot tickers'); },
      parseTickers: (rows) => makeTickers(rows, 'symbol', (r) => ({
        volumeBase24h: numeric(r.volume24h), volumeQuote24h: numeric(r.turnover24h), turnover24h: numeric(r.turnover24h), lastPrice: numeric(r.lastPrice),
      })),
    },
    {
      exchange: 'okx', marketEndpoint: okxMarket, tickerEndpoint: okxTicker,
      fetchMarkets: async (request) => { const p = object(await request(okxMarket), 'OKX instruments'); assertEnvelope(String(p.code) === '0', 'OKX code'); return list(p.data, 'OKX spot instruments'); },
      parseMarkets: (rows) => list(rows, 'OKX instruments').flatMap((item) => {
        const r = object(item, 'OKX instrument'); if (r.instType !== 'SPOT' || r.state !== 'live') return [];
        const m = makeMarket('okx', r.instId, r.baseCcy, r.quoteCcy, r.state, {
          ...(precision(r.tickSz) !== undefined ? { pricePrecision: precision(r.tickSz) } : {}),
          ...(precision(r.lotSz) !== undefined ? { quantityPrecision: precision(r.lotSz) } : {}),
          ...(numeric(r.tickSz) ? { tickSize: numeric(r.tickSz) } : {}),
          ...(numeric(r.lotSz) ? { lotSize: numeric(r.lotSz) } : {}),
          ...(numeric(r.minSz) ? { minimumQuantity: numeric(r.minSz) } : {}),
        });
        return m ? [m] : [];
      }),
      fetchTickers: async (request) => { const p = object(await request(okxTicker), 'OKX tickers'); assertEnvelope(String(p.code) === '0', 'OKX ticker code'); return list(p.data, 'OKX spot tickers'); },
      parseTickers: (rows) => makeTickers(rows, 'instId', (r) => ({
        volumeBase24h: numeric(r.vol24h), volumeQuote24h: numeric(r.volCcy24h), turnover24h: numeric(r.volCcy24h), lastPrice: numeric(r.last), exchangeTimestamp: numeric(r.ts),
      })),
    },
    {
      exchange: 'gate', marketEndpoint: gateMarket, tickerEndpoint: gateTicker,
      fetchMarkets: async (request) => list(await request(gateMarket), 'Gate spot currency pairs'),
      parseMarkets: (rows) => list(rows, 'Gate currency pairs').flatMap((item) => {
        const r = object(item, 'Gate currency pair'); if (r.trade_status !== 'tradable' || (r.type && r.type !== 'normal')) return [];
        const m = makeMarket('gate', r.id, r.base, r.quote, r.trade_status, {
          ...(numeric(r.precision) !== undefined ? { pricePrecision: numeric(r.precision) } : {}),
          ...(numeric(r.amount_precision) !== undefined ? { quantityPrecision: numeric(r.amount_precision) } : {}),
          ...(numeric(r.min_base_amount) ? { minimumQuantity: numeric(r.min_base_amount) } : {}),
          ...(numeric(r.min_quote_amount) ? { minimumNotional: numeric(r.min_quote_amount) } : {}),
        });
        return m ? [m] : [];
      }),
      fetchTickers: async (request) => list(await request(gateTicker), 'Gate spot tickers'),
      parseTickers: (rows) => makeTickers(rows, 'currency_pair', (r) => ({
        volumeBase24h: numeric(r.base_volume), volumeQuote24h: numeric(r.quote_volume), turnover24h: numeric(r.quote_volume), lastPrice: numeric(r.last), exchangeTimestamp: numeric(r.create_time_ms),
      })),
    },
    {
      exchange: 'mexc', marketEndpoint: mexcMarket, tickerEndpoint: mexcTicker,
      fetchMarkets: async (request) => list((await request<any>(mexcMarket)).symbols, 'MEXC exchangeInfo symbols'),
      parseMarkets: (rows) => list(rows, 'MEXC symbols').flatMap((item) => {
        const r = object(item, 'MEXC symbol'); if (!(r.status === 1 || r.status === '1') || r.isSpotTradingAllowed === false) return [];
        if (Array.isArray(r.permissions) && r.permissions.length > 0 && !r.permissions.includes('SPOT')) return [];
        const filters = Array.isArray(r.filters) ? r.filters : [], price = filters.find((x: any) => x.filterType === 'PRICE_FILTER'), lot = filters.find((x: any) => x.filterType === 'LOT_SIZE'), notion = filters.find((x: any) => x.filterType === 'MIN_NOTIONAL');
        const m = makeMarket('mexc', r.symbol, r.baseAsset, r.quoteAsset, r.status, {
          ...(numeric(r.quotePrecision) !== undefined ? { pricePrecision: numeric(r.quotePrecision) } : {}),
          ...(numeric(r.baseAssetPrecision) !== undefined ? { quantityPrecision: numeric(r.baseAssetPrecision) } : {}),
          ...(numeric(price?.tickSize) ? { tickSize: numeric(price.tickSize) } : {}),
          ...(numeric(lot?.stepSize) ? { lotSize: numeric(lot.stepSize) } : {}),
          ...(numeric(lot?.minQty) ? { minimumQuantity: numeric(lot.minQty) } : {}),
          ...(numeric(notion?.minNotional) ? { minimumNotional: numeric(notion.minNotional) } : {}),
        });
        return m ? [m] : [];
      }),
      fetchTickers: async (request) => list(await request(mexcTicker), 'MEXC 24h tickers'),
      parseTickers: (rows) => makeTickers(rows, 'symbol', (r) => ({
        volumeBase24h: numeric(r.volume), volumeQuote24h: numeric(r.quoteVolume), turnover24h: numeric(r.quoteVolume), lastPrice: numeric(r.lastPrice), exchangeTimestamp: numeric(r.closeTime),
      })),
    },
    {
      exchange: 'bitget', marketEndpoint: bitgetMarket, tickerEndpoint: bitgetTicker,
      fetchMarkets: async (request) => { const p = object(await request(bitgetMarket), 'Bitget symbols'); assertEnvelope(String(p.code) === '00000', 'Bitget code'); return list(p.data, 'Bitget spot symbols'); },
      parseMarkets: (rows) => list(rows, 'Bitget symbols').flatMap((item) => {
        const r = object(item, 'Bitget symbol'); if (!['online','normal','trading'].includes(String(r.status).toLowerCase())) return [];
        const m = makeMarket('bitget', r.symbol, r.baseCoin, r.quoteCoin, r.status, {
          ...(numeric(r.pricePrecision) !== undefined ? { pricePrecision: numeric(r.pricePrecision) } : {}),
          ...(numeric(r.quantityPrecision) !== undefined ? { quantityPrecision: numeric(r.quantityPrecision) } : {}),
          ...(numeric(r.priceEndStep) && numeric(r.pricePlace) ? { tickSize: Number(`1e-${Number(r.pricePlace)}`) * Number(r.priceEndStep) } : {}),
          ...(numeric(r.sizeMultiplier) ? { lotSize: numeric(r.sizeMultiplier) } : {}),
          ...(numeric(r.minTradeAmount) ? { minimumQuantity: numeric(r.minTradeAmount) } : {}),
          ...(numeric(r.minTradeUSDT) ? { minimumNotional: numeric(r.minTradeUSDT) } : {}),
        });
        return m ? [m] : [];
      }),
      fetchTickers: async (request) => { const p = object(await request(bitgetTicker), 'Bitget tickers'); assertEnvelope(String(p.code) === '00000', 'Bitget ticker code'); return list(p.data, 'Bitget ticker list'); },
      parseTickers: (rows) => makeTickers(rows, 'symbol', (r) => ({
        volumeBase24h: numeric(r.baseVolume ?? r.baseVol ?? r.volume24h), volumeQuote24h: numeric(r.quoteVolume ?? r.quoteVol ?? r.turnover24h), turnover24h: numeric(r.turnover24h ?? r.quoteVolume), lastPrice: numeric(r.lastPr ?? r.lastPrice), exchangeTimestamp: numeric(r.ts),
      })),
    },
    {
      exchange: 'kucoin', marketEndpoint: kucoinMarket, tickerEndpoint: kucoinTicker,
      fetchMarkets: async (request) => { const p = object(await request(kucoinMarket), 'KuCoin symbols'); assertEnvelope(String(p.code) === '200000', 'KuCoin code'); return list(p.data, 'KuCoin spot symbols'); },
      parseMarkets: (rows) => list(rows, 'KuCoin symbols').flatMap((item) => {
        const r = object(item, 'KuCoin symbol'); if (r.enableTrading !== true) return [];
        const m = makeMarket('kucoin', r.symbol, r.baseCurrency, r.quoteCurrency, 'online', {
          ...(precision(r.priceIncrement) !== undefined ? { pricePrecision: precision(r.priceIncrement) } : {}),
          ...(precision(r.baseIncrement) !== undefined ? { quantityPrecision: precision(r.baseIncrement) } : {}),
          ...(numeric(r.priceIncrement) ? { tickSize: numeric(r.priceIncrement) } : {}),
          ...(numeric(r.baseIncrement) ? { lotSize: numeric(r.baseIncrement) } : {}),
          ...(numeric(r.baseMinSize) ? { minimumQuantity: numeric(r.baseMinSize) } : {}),
          ...(numeric(r.minFunds) ? { minimumNotional: numeric(r.minFunds) } : {}),
        });
        return m ? [m] : [];
      }),
      fetchTickers: async (request) => { const p = object(await request(kucoinTicker), 'KuCoin all tickers'); assertEnvelope(String(p.code) === '200000', 'KuCoin ticker code'); return list(p.data?.ticker, 'KuCoin ticker list'); },
      parseTickers: (rows) => makeTickers(rows, 'symbol', (r) => ({
        volumeBase24h: numeric(r.vol), volumeQuote24h: numeric(r.volValue), turnover24h: numeric(r.volValue), lastPrice: numeric(r.last), exchangeTimestamp: numeric(r.time),
      })),
    },
    {
      exchange: 'htx', marketEndpoint: htxMarket, tickerEndpoint: htxTicker,
      fetchMarkets: async (request) => { const p = object(await request(htxMarket), 'HTX symbols'); assertEnvelope(p.status === 'ok', 'HTX status'); return list(p.data, 'HTX spot symbols'); },
      parseMarkets: (rows) => list(rows, 'HTX symbols').flatMap((item) => {
        const r = object(item, 'HTX symbol'); if (r.state !== 'online' || r['api-trading'] === 'disabled') return [];
        const m = makeMarket('htx', r.symbol, r['base-currency'], r['quote-currency'], r.state, {
          ...(numeric(r['price-precision']) !== undefined ? { pricePrecision: numeric(r['price-precision']) } : {}),
          ...(numeric(r['amount-precision']) !== undefined ? { quantityPrecision: numeric(r['amount-precision']) } : {}),
          ...(numeric(r['min-order-amt']) ? { minimumQuantity: numeric(r['min-order-amt']) } : {}),
          ...(numeric(r['min-order-value']) ? { minimumNotional: numeric(r['min-order-value']) } : {}),
        });
        return m ? [m] : [];
      }),
      fetchTickers: async (request) => { const p = object(await request(htxTicker), 'HTX tickers'); assertEnvelope(p.status === 'ok', 'HTX ticker status'); return list(p.data, 'HTX ticker list'); },
      parseTickers: (rows) => makeTickers(rows, 'symbol', (r) => ({
        volumeBase24h: numeric(r.amount), volumeQuote24h: numeric(r.vol), turnover24h: numeric(r.vol), lastPrice: numeric(r.close),
      })),
    },
    {
      exchange: 'crypto.com', marketEndpoint: cryptoMarket, tickerEndpoint: cryptoTicker,
      fetchMarkets: async (request) => { const p = object(await request(cryptoMarket), 'Crypto.com instruments'); assertEnvelope(Number(p.code) === 0, 'Crypto.com code'); return list(p.result?.data, 'Crypto.com instrument list'); },
      parseMarkets: (rows) => list(rows, 'Crypto.com instruments').flatMap((item) => {
        const r = object(item, 'Crypto.com instrument'); if (r.inst_type !== 'CCY_PAIR' || r.tradable !== true) return [];
        if (r.product_type && !['DIGITAL_CURRENCIES','CRYPTO'].includes(String(r.product_type).toUpperCase())) return [];
        const m = makeMarket('crypto.com', r.symbol, r.base_ccy, r.quote_ccy, r.tradable ? 'tradable' : 'inactive', {
          ...(numeric(r.price_decimals) !== undefined ? { pricePrecision: numeric(r.price_decimals) } : {}),
          ...(numeric(r.quantity_decimals) !== undefined ? { quantityPrecision: numeric(r.quantity_decimals) } : {}),
          ...(numeric(r.price_tick_size) ? { tickSize: numeric(r.price_tick_size) } : {}),
          ...(numeric(r.qty_tick_size) ? { lotSize: numeric(r.qty_tick_size) } : {}),
        });
        return m ? [m] : [];
      }),
      fetchTickers: async (request) => { const p = object(await request(cryptoTicker), 'Crypto.com tickers'); assertEnvelope(Number(p.code) === 0, 'Crypto.com ticker code'); return list(p.result?.data, 'Crypto.com ticker list'); },
      parseTickers: (rows) => makeTickers(rows, 'i', (r) => ({
        volumeBase24h: numeric(r.v), volumeQuote24h: numeric(r.vv), turnover24h: numeric(r.vv), lastPrice: numeric(r.a), exchangeTimestamp: numeric(r.t),
      })),
    },
    {
      exchange: 'coinbase', marketEndpoint: coinbaseMarket, tickerEndpoint: coinbaseTicker,
      fetchMarkets: async (request) => fetchAllCoinbase(request),
      parseMarkets: (rows) => list(rows, 'Coinbase products').flatMap((item) => {
        const r = object(item, 'Coinbase product');
        if (r.product_type !== 'SPOT' || !r.base_currency_id || !r.quote_currency_id || r.trading_disabled === true || r.is_disabled === true || r.view_only === true || !['online','active',''].includes(String(r.status ?? '').toLowerCase())) return [];
        const m = makeMarket('coinbase', r.product_id, r.base_currency_id ?? r.base_currency, r.quote_currency_id ?? r.quote_currency, r.status ?? 'online', {
          ...(numeric(r.quote_increment) !== undefined ? { pricePrecision: precision(r.quote_increment) } : {}),
          ...(numeric(r.base_increment) !== undefined ? { quantityPrecision: precision(r.base_increment) } : {}),
          ...(numeric(r.quote_increment) ? { tickSize: numeric(r.quote_increment) } : {}),
          ...(numeric(r.base_increment) ? { lotSize: numeric(r.base_increment) } : {}),
          ...(numeric(r.base_min_size) ? { minimumQuantity: numeric(r.base_min_size) } : {}),
          ...(numeric(r.quote_min_size) ? { minimumNotional: numeric(r.quote_min_size) } : {}),
          ...(numeric(r.volume_24h) ? { volumeBase24h: numeric(r.volume_24h) } : {}),
          ...(numeric(r.approximate_quote_24h_volume) ? { volumeQuote24h: numeric(r.approximate_quote_24h_volume) } : {}),
          ...(numeric(r.price) ? { lastPrice: numeric(r.price) } : {}),
        });
        return m ? [m] : [];
      }),
      fetchTickers: async () => [],
      parseTickers: () => new Map(),
    },
  ];
}
async function fetchAllCoinbase(get: <T>(url: string) => Promise<T>): Promise<unknown[]> {
  const all: unknown[] = [];
  let url = 'https://api.coinbase.com/api/v3/brokerage/market/products?product_type=SPOT&limit=250';
  const seen = new Set<string>();
  for (let page = 0; page < 100; page += 1) {
    const p = object(await get<any>(url), 'Coinbase public products');
    const rows = list(p.products, 'Coinbase product list');
    all.push(...rows);
    const cursor = text(p.next_page);
    if (!cursor || seen.has(cursor)) break;
    seen.add(cursor);
    url = `https://api.coinbase.com/api/v3/brokerage/market/products?product_type=SPOT&limit=250&cursor=${encodeURIComponent(cursor)}`;
  }
  return all;
}
function assertEnvelope(valid: boolean, label: string): asserts valid {
  if (!valid) throw new Error(`Exchange returned unsuccessful ${label} response`);
}
