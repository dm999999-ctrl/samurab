# Phase A: market discovery and ranked asset universe

The discovery runner is a separate, read-only REST subsystem. It does not change the existing Binance/Bybit WebSocket scanner, use credentials, place orders, or claim order-book liquidity.

Run from `/home/ubuntu/scanner`:

```sh
pnpm discover
```

It writes an immutable run and an atomically updated latest report under `data/discovery/`. Set `DISCOVERY_OUTPUT_DIR` to change that path.

## Public sources

The adapters query public exchange API endpoints for spot market metadata and 24-hour ticker/product data:

| Exchange | Market metadata | 24-hour data |
| --- | --- | --- |
| Binance | Spot `exchangeInfo` | 24-hour ticker |
| Bybit | V5 instruments, `category=spot` | V5 tickers, `category=spot` |
| OKX | Public instruments, `instType=SPOT` | Market tickers, `instType=SPOT` |
| Gate.io | Spot currency pairs | Spot tickers |
| MEXC | Spot `exchangeInfo` | 24-hour ticker |
| Bitget | V2 spot symbols | V2 spot tickers |
| KuCoin | Spot symbols | All tickers |
| HTX | Common symbols | Market tickers |
| Crypto.com Exchange | Public instruments | Public tickers |
| Coinbase Advanced Trade | Public SPOT products (cursor-paged) | Product metadata when present |

Every exchange has its own status and source-health fields. A request/parse error is retained rather than silently interpreted as an empty catalog. The latest crawl should be checked for `coverageComplete`, per-exchange status, and ticker warnings before using the ranked list.

## Normalization and ranking

Only active spot listings are retained; futures, swaps, inactive listings, stablecoin bases, and heuristically recognized leveraged/synthetic suffixes are excluded from the ranked asset universe. Exchange-native instrument symbols are preserved. The small explicit alias map canonicalizes XBT→BTC and BCC→BCH; wrapped/bridged assets are not merged, and the configured ambiguous symbols are not ranked.

The default asset cap is 1,000 (`MASTER_UNIVERSE_TARGET`); lower eligible counts remain lower, with no padding. Assets and pairs require presence on at least two known exchanges by default (`MIN_EXCHANGE_COUNT`). Score is a deterministic weighted mean: exchange coverage 35%, within-exchange/quote 24h quote-volume percentile 35%, exchanges with positive reported volume 15%, configured major-quote availability 10%, and reported activity 5%. Set `SCORE_WEIGHT_COVERAGE`, `SCORE_WEIGHT_LIQUIDITY`, `SCORE_WEIGHT_LIQUID_EXCHANGES`, `SCORE_WEIGHT_QUOTES`, or `SCORE_WEIGHT_ACTIVITY` to adjust; unavailable components are omitted and remaining weights renormalized. Ties are resolved deterministically by coverage, pair count, liquidity, symbol, then asset id.

Volumes are not converted between quote currencies. `totalVolume24hByQuoteAsset` keeps units separate; combined and median totals are null for mixed quote currencies. This report is discovery metadata only: 24-hour reported turnover is not executable liquidity. Executable arbitrage still requires synchronized books, depth, fees, and freshness checks in the scanner.

## Limitations

- The list is a live snapshot, not a persistent database or scheduler; this package has no database/schema today.
- Exchange-reported 24-hour turnover fields and quote semantics differ. Scores are comparative heuristics, not calibrated probabilities.
- Alias mapping is deliberately conservative; identical tickers can represent unrelated assets and must not be merged without a verified identity mapping.
- A successful public endpoint returning no normalized rows is reported distinctly from a failed endpoint, but endpoint/schema changes can still require adapter maintenance. Coinbase market metadata is paginated; a single discovery run can observe exchange catalog changes while paging.
- Respect exchange rate limits. The runner applies bounded HTTP retries/timeouts; rerun deliberately rather than scheduling a tight polling loop.