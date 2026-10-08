import { randomUUID } from 'node:crypto';
import { mkdir, rename, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { runDiscovery } from './engine';

async function main() {
  const report = await runDiscovery({ runId: `discovery-${new Date().toISOString().replace(/[:.]/g, '-')}-${randomUUID().slice(0, 8)}` });
  const root = resolve(process.env.DISCOVERY_OUTPUT_DIR ?? 'data/discovery');
  const runPath = join(root, 'runs', `${report.runId}.json`);
  const latestPath = join(root, 'latest.json');
  await mkdir(dirname(runPath), { recursive: true });
  await atomicWrite(runPath, JSON.stringify(report, null, 2));
  await atomicWrite(latestPath, JSON.stringify(report, null, 2));
  const summary = {
    runId: report.runId, status: report.status,
    exchanges: report.exchanges.map(({ exchange, status, marketCount, warnings }) => ({ exchange, status, marketCount, warnings })),
    marketsDiscovered: report.marketsDiscovered,
    uniqueCanonicalAssetsDiscovered: report.uniqueCanonicalAssetsDiscovered,
    nonStableAssetsEligible: report.nonStableAssetsEligible,
    selectedAssetCount: report.selectedAssetCount,
    eligiblePairCount: report.pairCount,
    selectedTokenCount: report.selectedTokenIds?.length ?? 0,
    eligibleCanonicalMarketCount: report.eligibleMarketCount ?? 0,
    selectedCanonicalMarketCount: report.selectedTokenMarketCount ?? 0,
    projectedExchangeBookCount: report.projectedExchangeBookCount ?? 0,
    exactCrossExchangeMarketMatchCount: report.exactCrossExchangeMarketMatchCount ?? 0,
    marketsByQuoteAsset: Object.fromEntries([...new Set((report.selectedTokenMarkets ?? []).map((market) => market.quoteAsset))].sort().map((quote) => [quote, (report.selectedTokenMarkets ?? []).filter((market) => market.quoteAsset === quote).length])),
    marketsByExchange: Object.fromEntries(report.exchanges.map(({ exchange }) => [exchange, (report.selectedTokenMarkets ?? []).reduce((count, market) => count + (market.markets.some((listing) => listing.exchange === exchange) ? 1 : 0), 0)])),
    subscriptionsByExchange: Object.fromEntries(report.exchanges.map(({ exchange }) => [exchange, (report.selectedTokenMarkets ?? []).reduce((count, market) => count + market.markets.filter((listing) => listing.exchange === exchange).length, 0)])),
    marketPruningTiers: (report.marketPruningTiers ?? []).map(({ tier, marketCount, exchangeBookCount, quoteDistribution, possibleExactCrossExchangeComparisons, markets }) => ({
      tier, marketCount, exchangeBookCount, quoteDistribution, possibleExactCrossExchangeComparisons,
      markets: markets.map(({ canonicalPair, exchangeCount, exchanges }) => ({ canonicalPair, exchangeCount, exchanges })),
    })),
    topAssets: report.assets.slice(0, 20).map(({ rank, symbol, exchangeCount, arbitrageRelevanceScore }) => ({ rank, symbol, exchangeCount, score: Number(arbitrageRelevanceScore.toFixed(2)) })),
    output: latestPath,
  };
  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  if (report.status === 'FAILED') process.exitCode = 1;
}
async function atomicWrite(path: string, content: string) {
  const temporary = `${path}.${process.pid}.tmp`;
  await writeFile(temporary, content, { encoding: 'utf8', mode: 0o600 });
  await rename(temporary, path);
}
main().catch((error) => {
  process.stderr.write(`Discovery failed: ${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
