// Phase E reads only the normalized Phase B+C+D runtime. Never proxy to the legacy scanner.
const DEFAULT_SCANNER_URL = 'http://127.0.0.1:4100/telemetry';

function getScannerUrl(): string {
  const target = new URL(process.env.SCANNER_TELEMETRY_URL ?? DEFAULT_SCANNER_URL);
  if ((target.protocol !== 'http:' && target.protocol !== 'https:') || target.port !== '4100'
    || target.pathname !== '/telemetry' || target.username || target.password) {
    throw new Error('SCANNER_TELEMETRY_URL must target port 4100 /telemetry without embedded credentials');
  }
  return target.toString();
}

export async function GET(request: Request) {
  const url = new URL(request.url);
  if (process.env.NODE_ENV !== 'production' && url.searchParams.get('mock') === '1') {
    const now = Date.now();
    const opportunities = url.searchParams.get('empty') === '1' ? [] : [{ pair: 'BTC/USDT', asset: 'BTC', quote: 'USDT', buyExchange: 'binance', sellExchange: 'bybit',
      buyPrice: 100_000, buyVWAP: 100_020, sellPrice: 100_400, sellVWAP: 100_380,
      executableQuantity: 0.08, executableNotional: 8_001.6, grossSpread: 0.00359928,
      netSpread: 0.00159568, totalEstimatedSlippage: 0.0003992, estimatedFees: 16.032,
      estimatedNetProfit: 12.768, score: 72.4, timestamp: now, grossProfit: 28.8,
      buyFee: 8.0016, sellFee: 8.0304, totalFees: 16.032, estimatedSlippageCost: 3.2,
      buyQuoteCost: 8_001.6, sellQuoteProceeds: 8_030.4, buySlippage: 0.0002, sellSlippage: 0.0001992,
      buyBookAgeMs: 120, sellBookAgeMs: 95, freshness: 'ACTIVE',
      buyDepthUsed: [{ price: 100_000, quantity: 0.04, quoteAmount: 4_000 }, { price: 100_040, quantity: 0.04, quoteAmount: 4_001.6 }],
      sellDepthUsed: [{ price: 100_400, quantity: 0.04, quoteAmount: 4_016 }, { price: 100_360, quantity: 0.04, quoteAmount: 4_014.4 }] }];
    return Response.json({
      demo: true, status: 'RUNNING', phase: 'B', books: 18, subscriptions: 18,
      marketStateCounts: { synchronized: 18, quiet: 1, stale: 0, failed: 0, pending: 0 },
      exchanges: Object.fromEntries(['binance', 'bybit', 'okx', 'kucoin', 'gate', 'bitget', 'htx', 'crypto.com', 'coinbase'].map((exchange) =>
        [exchange, { connections: 1, subscriptions: 2, synchronizedBooks: 2, quietBooks: 0, staleBooks: 0, reconnects: 0, sequenceGaps: 0, messagesPerSecond: 4.2, errors: [] }])),
      resources: { processCpuPercent: 8.2, systemCpuPercent: 14.5, rssBytes: 256 * 1024 * 1024,
        systemMemoryUsedBytes: 5 * 1024 ** 3, systemMemoryTotalBytes: 12 * 1024 ** 3,
        eventLoopDelayMeanMs: 2.1, eventLoopDelayMaxMs: 5.6, activeWebSocketConnections: 9, subscriptions: 18 },
      arbitrage: { config: { maxBookAgeMs: 60_000 }, qualifyingOpportunityCount: opportunities.length, opportunities } });
  }
  try {
    const upstream = await fetch(getScannerUrl(), {
      cache: 'no-store',
      signal: AbortSignal.any([request.signal, AbortSignal.timeout(3_000)]),
    });
    return new Response(upstream.body, {
      status: upstream.status,
      headers: {
        'content-type': upstream.headers.get('content-type') ?? 'application/json',
        'cache-control': 'no-store',
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return Response.json({ error: `Scanner telemetry unavailable: ${message}` }, { status: 502 });
  }
}
