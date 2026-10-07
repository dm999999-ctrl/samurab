type Props = { params: Promise<{ symbol: string }> };

export default async function TokenArbitragePage({ params }: Props) {
  const { symbol } = await params;
  const normalized = decodeURIComponent(symbol).replace('-', '/').toUpperCase();
  return <main className="detail-page"><a href="/" className="back-link">← Back to scanner</a><div className="title-row detail-title"><div><h1>{normalized} arbitrage</h1><div className="subtitle">Token-specific market analysis</div></div><div className="livepill offline">● DATA NOT AVAILABLE</div></div>
    <section className="panel"><h2>No token market data available</h2><p className="subtitle">The connected scanner currently publishes BTC/USDT books for Binance and Bybit only. This route does not synthesize prices, exchange comparisons, or historical performance.</p><div className="health-row"><span>Requested market</span><span className="mono">{normalized}</span></div><div className="health-row"><span>Scanner market</span><span className="mono">BTC/USDT</span></div><div className="health-row"><span>Data source</span><span className="mono">Not available from current telemetry</span></div></section>
  </main>;
}
