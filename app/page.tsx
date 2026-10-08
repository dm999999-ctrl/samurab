'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';

type Fill = { price: number; quantity: number; quoteAmount: number };
type Opportunity = {
  pair: string; asset: string; quote: string; buyExchange: string; sellExchange: string;
  buyPrice: number; buyVWAP: number; sellPrice: number; sellVWAP: number;
  executableQuantity: number; executableNotional: number; grossSpread: number; netSpread: number;
  totalEstimatedSlippage: number; estimatedFees: number; estimatedNetProfit: number; score: number;
  timestamp: number; grossProfit: number; buyFee: number; sellFee: number; totalFees: number;
  estimatedSlippageCost: number; buyQuoteCost: number; sellQuoteProceeds: number;
  buySlippage: number; sellSlippage: number;
  buyBookAgeMs: number; sellBookAgeMs: number; freshness: 'ACTIVE' | 'QUIET';
  buyDepthUsed: Fill[]; sellDepthUsed: Fill[];
};
type Market = { exchange: string; status: string; synchronized: boolean; usable: boolean; quiet: boolean; stale: boolean; ageMs: number | null; bestBid: number | null; bestAsk: number | null };
type ExchangeHealth = { connections?: number; subscriptions?: number; synchronizedBooks?: number; quietBooks?: number; staleBooks?: number; reconnects?: number; sequenceGaps?: number; messagesPerSecond?: number; errors?: string[] };
type Telemetry = {
  demo?: boolean;
  status?: string; phase?: string; selectedPairs?: number; subscriptions?: number; books?: number;
  markets?: Market[]; marketStateCounts?: { synchronized?: number; quiet?: number; stale?: number; failed?: number; pending?: number };
  exchanges?: Record<string, ExchangeHealth>;
  arbitrage?: { qualifyingOpportunityCount?: number; opportunities?: Opportunity[]; config?: { maxBookAgeMs?: number }; profitabilityLimits?: Record<string, number> };
  resources?: { processCpuPercent?: number; systemCpuPercent?: number | null; rssBytes?: number; systemMemoryUsedBytes?: number; systemMemoryTotalBytes?: number; eventLoopDelayMeanMs?: number | null; eventLoopDelayMaxMs?: number | null; activeWebSocketConnections?: number; subscriptions?: number };
};
type Connection = 'CONNECTING' | 'CONNECTED' | 'DISCONNECTED';

const EXCHANGE_LABELS: Record<string, string> = { binance: 'Binance', bybit: 'Bybit', okx: 'OKX', kucoin: 'KuCoin', gate: 'Gate.io', bitget: 'Bitget', htx: 'HTX', 'crypto.com': 'Crypto.com', coinbase: 'Coinbase' };
const exchangeName = (value: string) => EXCHANGE_LABELS[value] ?? value;
const money = (value: number | null | undefined, quote = '') => value == null || !Number.isFinite(value) ? '—' : `${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 })}${quote ? ` ${quote}` : ''}`;
const pct = (value: number | null | undefined) => value == null || !Number.isFinite(value) ? '—' : `${(value * 100).toFixed(3)}%`;
const qty = (value: number | null | undefined) => value == null || !Number.isFinite(value) ? '—' : value.toLocaleString(undefined, { maximumFractionDigits: 8 });
const duration = (value: number | null | undefined) => value == null || !Number.isFinite(value) ? '—' : value < 1000 ? `${Math.round(value)} ms` : `${(value / 1000).toFixed(1)} s`;

export default function Dashboard() {
  const [telemetry, setTelemetry] = useState<Telemetry | null>(null);
  const [connection, setConnection] = useState<Connection>('CONNECTING');
  const [receivedAt, setReceivedAt] = useState(0);
  const [clock, setClock] = useState(Date.now());
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [filters, setFilters] = useState({ pair: '', buy: '', sell: '', profit: '', spread: '', notional: '', slippage: '', age: '' });

  const refresh = useCallback(async (signal: AbortSignal): Promise<boolean> => {
    try {
      const mockMode = typeof window !== 'undefined' && new URLSearchParams(window.location.search).get('mock') === '1';
      const response = await fetch(`/api/telemetry${mockMode ? '?mock=1' : ''}`, { cache: 'no-store', signal });
      if (!response.ok) throw new Error(`Scanner returned HTTP ${response.status}`);
      const snapshot = await response.json() as Telemetry;
      if (!snapshot || typeof snapshot !== 'object' || !snapshot.arbitrage) throw new Error('Scanner response has no Phase C/D telemetry');
      setTelemetry(snapshot); setReceivedAt(Date.now()); setConnection('CONNECTED'); return true;
    } catch (error) {
      if (error instanceof DOMException && error.name === 'AbortError') return false;
      setTelemetry(null); setConnection('DISCONNECTED'); return false;
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    let poll: number | undefined;
    let failures = 0;
    const schedule = async () => {
      const ok = await refresh(controller.signal);
      if (controller.signal.aborted) return;
      failures = ok ? 0 : failures + 1;
      const delay = ok ? 2_000 : Math.min(30_000, 2_000 * 2 ** Math.min(failures, 4));
      poll = window.setTimeout(schedule, delay);
    };
    void schedule();
    const tick = window.setInterval(() => setClock(Date.now()), 1_000);
    return () => { controller.abort(); if (poll !== undefined) window.clearTimeout(poll); window.clearInterval(tick); };
  }, [refresh]);

  useEffect(() => {
    if (!selectedKey) return;
    const closeOnEscape = (event: KeyboardEvent) => { if (event.key === 'Escape') setSelectedKey(null); };
    window.addEventListener('keydown', closeOnEscape);
    return () => window.removeEventListener('keydown', closeOnEscape);
  }, [selectedKey]);

  const maxAgeMs = telemetry?.arbitrage?.config?.maxBookAgeMs ?? 60_000;
  const dataFresh = connection === 'CONNECTED' && Date.now() - receivedAt < 5_000;
  const allOpportunities = useMemo(() => (telemetry?.arbitrage?.opportunities ?? []).filter((item) =>
    Number.isFinite(item.timestamp) && clock - item.timestamp >= -5_000 && opportunityAge(item, clock) <= maxAgeMs
    && item.estimatedNetProfit > 0 && item.freshness !== undefined)
    .sort((a, b) => b.estimatedNetProfit - a.estimatedNetProfit || b.score - a.score), [telemetry, clock, maxAgeMs]);
  const filtered = useMemo(() => allOpportunities.filter((item) =>
    item.pair.toLowerCase().includes(filters.pair.toLowerCase())
    && (!filters.buy || item.buyExchange === filters.buy)
    && (!filters.sell || item.sellExchange === filters.sell)
    && item.estimatedNetProfit >= numericFilter(filters.profit)
    && item.netSpread * 100 >= numericFilter(filters.spread)
    && item.executableNotional >= numericFilter(filters.notional)
    && item.totalEstimatedSlippage * 100 <= (filters.slippage ? numericFilter(filters.slippage) : Infinity)
    && (filters.age ? opportunityAge(item, clock) <= numericFilter(filters.age) * 1000 : true)),
  [allOpportunities, filters, clock]);
  const selected = allOpportunities.find((item) => key(item) === selectedKey);
  const selectedExpired = Boolean(selectedKey && !selected);
  const best = allOpportunities[0];
  const exchangeEntries = Object.entries(telemetry?.exchanges ?? {});
  const activeExchanges = exchangeEntries.filter(([, item]) => (item.connections ?? 0) > 0).length;
  const health = telemetry?.marketStateCounts;
  const expiredNote = selectedExpired ? 'The underlying route is no longer present in the current validated opportunity set. Its previous figures are hidden.' : '';

  const setFilter = (name: keyof typeof filters, value: string) => setFilters((current) => ({ ...current, [name]: value }));
  const clearFilters = () => setFilters({ pair: '', buy: '', sell: '', profit: '', spread: '', notional: '', slippage: '', age: '' });

  return <div className="dashboard-shell">
    <aside className="sidebar"><a className="brand" href="/"><span className="brandmark">↗</span><span>ARB / LIVE</span></a>
      <div className="side-section">SCANNER</div><a className="side-link active" href="#opportunities">Opportunities</a><a className="side-link" href="#health">Market health</a>
      <div className="side-foot"><span className="mono">PHASE B · C · D</span><br/>Public spot data only<br/>Read-only · no execution</div>
    </aside>
    <main className="main-area">
      <header className="topbar"><div><span className="eyebrow">MARKET INTELLIGENCE</span><span className="breadcrumb"> / LIVE ARBITRAGE</span></div><div className={`connection ${dataFresh ? 'is-live' : 'is-down'}`}><i/> {dataFresh ? 'SCANNER LIVE' : connection === 'CONNECTING' ? 'CONNECTING' : 'SCANNER OFFLINE'}</div></header>
      <div className="content-area">
        <div className="page-heading"><div><div className="eyebrow">CROSS-EXCHANGE · SPOT</div><h1>Arbitrage opportunities</h1><p>Validated executable routes, ranked by estimated net profit after fees and depth impact.</p>{telemetry?.demo && <span className="demo-banner">SYNTHETIC UI TEST DATA · NOT A LIVE OPPORTUNITY</span>}</div><div className="updated">{dataFresh ? `Updated ${new Date(receivedAt).toLocaleTimeString()}` : 'Waiting for scanner telemetry'}</div></div>
        <section className="summary-grid" aria-label="Scanner summary">
          <Summary label="Qualifying routes" value={dataFresh ? String(allOpportunities.length) : '—'} detail="Phase C + D validated" tone="green"/>
          <Summary label="Best estimated profit" value={dataFresh && best ? money(best.estimatedNetProfit, best.quote) : '—'} detail={best ? `${best.pair} · ${exchangeName(best.buyExchange)} → ${exchangeName(best.sellExchange)}` : 'No qualifying route'} tone="profit"/>
          <Summary label="Best net spread" value={dataFresh && best ? pct(best.netSpread) : '—'} detail="After configured taker fees"/>
          <Summary label="Usable / monitored books" value={dataFresh ? `${health?.synchronized ?? 0} / ${telemetry?.books ?? telemetry?.subscriptions ?? '—'}` : '—'} detail={`${health?.quiet ?? 0} quiet · ${health?.stale ?? 0} stale`}/>
          <Summary label="Active exchanges" value={dataFresh ? `${activeExchanges} / ${exchangeEntries.length}` : '—'} detail={dataFresh ? `${telemetry?.resources?.activeWebSocketConnections ?? '—'} WebSocket connections` : '—'}/>
          <Summary label="Scanner status" value={dataFresh ? (telemetry?.status ?? 'UNKNOWN') : 'OFFLINE'} detail="Phase B+C+D runtime · port 4100" tone={dataFresh ? 'green' : 'red'}/>
        </section>

        <section id="opportunities" className="opportunity-section">
          <div className="section-title"><div><h2>Live opportunity feed</h2><p>Only currently qualifying, fresh Phase C + D routes are shown.</p></div><span className="subtle">SORTED BY EST. NET PROFIT</span></div>
          <div className="filters" aria-label="Opportunity filters">
            <label>Pair<input value={filters.pair} onChange={(e) => setFilter('pair', e.target.value)} placeholder="e.g. BTC/USDT"/></label>
            <label>Buy exchange<select value={filters.buy} onChange={(e) => setFilter('buy', e.target.value)}><option value="">All</option>{exchangeEntries.map(([name]) => <option key={name} value={name}>{exchangeName(name)}</option>)}</select></label>
            <label>Sell exchange<select value={filters.sell} onChange={(e) => setFilter('sell', e.target.value)}><option value="">All</option>{exchangeEntries.map(([name]) => <option key={name} value={name}>{exchangeName(name)}</option>)}</select></label>
            <label>Min profit<input type="number" min="0" step="0.01" value={filters.profit} onChange={(e) => setFilter('profit', e.target.value)} placeholder="Quote amount"/></label>
            <label>Min net spread %<input type="number" min="0" step="0.01" value={filters.spread} onChange={(e) => setFilter('spread', e.target.value)} placeholder="%"/></label>
            <label>Min notional<input type="number" min="0" step="1" value={filters.notional} onChange={(e) => setFilter('notional', e.target.value)} placeholder="Quote amount"/></label>
            <label>Max slippage %<input type="number" min="0" step="0.01" value={filters.slippage} onChange={(e) => setFilter('slippage', e.target.value)} placeholder="%"/></label>
            <label>Max age (s)<input type="number" min="0" step="1" value={filters.age} onChange={(e) => setFilter('age', e.target.value)} placeholder="Seconds"/></label>
            <button className="clear-button" onClick={clearFilters}>Reset filters</button>
          </div>
          {dataFresh && filtered.length > 0 ? <div className="table-scroll"><table className="opportunity-table"><thead><tr><th>Pair / route</th><th>Buy price</th><th>Sell price</th><th>Qty</th><th>Notional</th><th>Buy VWAP</th><th>Sell VWAP</th><th>Gross spread</th><th>Slippage</th><th>Fees</th><th>Net spread</th><th>Est. net profit</th><th>Score</th><th>Age / state</th></tr></thead><tbody>{filtered.map((item) => <tr key={key(item)} className="opportunity-row" onClick={() => setSelectedKey(key(item))} onKeyDown={(event) => { if (event.key === 'Enter' || event.key === ' ') setSelectedKey(key(item)); }} tabIndex={0} aria-label={`View ${item.pair}, estimated net profit ${money(item.estimatedNetProfit, item.quote)}`}>
            <td><button className="opportunity-open" aria-label={`Open details for ${item.pair}`} onClick={(event) => { event.stopPropagation(); setSelectedKey(key(item)); }}><strong>{item.pair}</strong><span className="route-line">{exchangeName(item.buyExchange)} <b>→</b> {exchangeName(item.sellExchange)}</span></button></td><td>{money(item.buyPrice, item.quote)}</td><td>{money(item.sellPrice, item.quote)}</td><td>{qty(item.executableQuantity)} {item.asset}</td><td>{money(item.executableNotional, item.quote)}</td><td>{money(item.buyVWAP, item.quote)}</td><td>{money(item.sellVWAP, item.quote)}</td><td>{pct(item.grossSpread)}</td><td>{pct(item.totalEstimatedSlippage)}</td><td>{money(item.estimatedFees, item.quote)}</td><td>{pct(item.netSpread)}</td><td className="profit-cell">{money(item.estimatedNetProfit, item.quote)}</td><td><span className="score-badge">{item.score.toFixed(1)}</span></td><td><span>{duration(opportunityAge(item, clock))}</span><span className={`state-pill ${item.freshness.toLowerCase()}`}>{item.freshness}</span></td>
          </tr>)}</tbody></table></div> : <div className="empty-state"><div className="empty-icon">⌁</div><h3>{!dataFresh ? 'Scanner data unavailable' : allOpportunities.length === 0 ? 'No qualifying opportunity right now' : 'No routes match these filters'}</h3><p>{!dataFresh ? 'The dashboard cannot reach normalized Phase B+C+D telemetry at 127.0.0.1:4100. It will retry automatically.' : allOpportunities.length === 0 ? 'Routes appear only when both books are synchronized, healthy, fresh, sufficiently liquid, and remain positive after fees and estimated slippage.' : 'Adjust or reset the filters to see current validated routes.'}</p>{dataFresh && allOpportunities.length > 0 && <button className="clear-button" onClick={clearFilters}>Reset filters</button>}</div>}
          {dataFresh && filtered.length > 0 && <div className="table-caption">{filtered.length} of {allOpportunities.length} current qualifying {allOpportunities.length === 1 ? 'route' : 'routes'} · no orders are placed by this dashboard</div>}
        </section>

        <section id="health" className="health-section"><div className="section-title"><div><h2>Market health</h2><p>Secondary runtime health for the currently selected market universe.</p></div><span className="subtle">NORMALIZED BACKEND TELEMETRY</span></div>
          <div className="health-overview"><HealthMetric label="Synchronized" value={dataFresh ? String(health?.synchronized ?? 0) : '—'}/><HealthMetric label="Quiet" value={dataFresh ? String(health?.quiet ?? 0) : '—'}/><HealthMetric label="Invalid / stale" value={dataFresh ? String((health?.stale ?? 0) + (health?.failed ?? 0)) : '—'}/><HealthMetric label="Process CPU" value={dataFresh ? pct((telemetry?.resources?.processCpuPercent ?? 0) / 100) : '—'}/><HealthMetric label="System CPU" value={dataFresh && telemetry?.resources?.systemCpuPercent != null ? pct(telemetry.resources.systemCpuPercent / 100) : '—'}/><HealthMetric label="Process RSS" value={dataFresh ? bytes(telemetry?.resources?.rssBytes) : '—'}/><HealthMetric label="System memory" value={dataFresh ? `${bytes(telemetry?.resources?.systemMemoryUsedBytes)} / ${bytes(telemetry?.resources?.systemMemoryTotalBytes)}` : '—'}/><HealthMetric label="Event-loop max" value={dataFresh ? duration(telemetry?.resources?.eventLoopDelayMaxMs) : '—'}/></div>
          <div className="exchange-grid">{exchangeEntries.map(([name, item]) => <div className="exchange-card" key={name}><div className="exchange-head"><strong>{exchangeName(name)}</strong><span className={(item.errors?.length ?? 0) ? 'error-state' : 'ok-state'}>{(item.errors?.length ?? 0) ? 'ISSUES' : (item.connections ?? 0) > 0 ? 'CONNECTED' : 'OFFLINE'}</span></div><div className="exchange-meta"><span>{item.synchronizedBooks ?? 0} synced / {item.subscriptions ?? 0} books</span><span>{(item.messagesPerSecond ?? 0).toFixed(1)} msg/s · {item.reconnects ?? 0} reconnects</span></div>{(item.errors?.length ?? 0) > 0 && <div className="adapter-error">{item.errors?.[0]}</div>}</div>)}</div>
        </section>
        <footer className="disclaimer">Profitability is estimated from public order-book data and configured fee assumptions; it is not a guarantee of execution or return. This scanner is read-only and does not place trades.</footer>
      </div>
    </main>
    {selected && <><button className="drawer-scrim" aria-label="Close opportunity details" onClick={() => setSelectedKey(null)}/><OpportunityDrawer opportunity={selected} onClose={() => setSelectedKey(null)} now={clock}/></>}
    {selectedExpired && <><button className="drawer-scrim" aria-label="Close expired opportunity notice" onClick={() => setSelectedKey(null)}/><aside className="drawer expired-drawer" role="dialog" aria-modal="true" aria-labelledby="expired-title"><button className="drawer-close" onClick={() => setSelectedKey(null)} aria-label="Close">×</button><div className="expired-mark">!</div><div className="eyebrow">ROUTE REMOVED</div><h2 id="expired-title">Opportunity expired</h2><p>{expiredNote}</p><p className="drawer-note">A route must remain in the backend's current validated opportunity set to be shown as live.</p></aside></>}
  </div>;
}

function Summary({ label, value, detail, tone = '' }: { label: string; value: string; detail: string; tone?: string }) { return <article className="summary-card"><span className="summary-label">{label}</span><strong className={`summary-value ${tone}`}>{value}</strong><span className="summary-detail">{detail}</span></article>; }
function HealthMetric({ label, value }: { label: string; value: string }) { return <div className="health-metric"><span>{label}</span><strong>{value}</strong></div>; }

function OpportunityDrawer({ opportunity: item, onClose, now }: { opportunity: Opportunity; onClose: () => void; now: number }) {
  const maxLevel = Math.max(...item.buyDepthUsed.map((fill) => fill.quantity), ...item.sellDepthUsed.map((fill) => fill.quantity), 0);
  return <aside className="drawer" role="dialog" aria-modal="true" aria-labelledby="drawer-title"><button className="drawer-close" onClick={onClose} aria-label="Close details">×</button>
    <div className="drawer-kicker">VALIDATED OPPORTUNITY · {item.freshness}</div><h2 id="drawer-title">{item.pair}</h2><p className="drawer-route">{exchangeName(item.buyExchange)} <b>BUY</b><span>→</span>{exchangeName(item.sellExchange)} <b>SELL</b></p>
    <div className="drawer-profit"><span>Estimated net profit</span><strong>{money(item.estimatedNetProfit, item.quote)}</strong><small>{pct(item.netSpread)} net spread · age {duration(opportunityAge(item, now))}</small></div>
    <div className="drawer-stats"><div><span>Executable quantity</span><strong>{qty(item.executableQuantity)} {item.asset}</strong></div><div><span>Executable notional</span><strong>{money(item.executableNotional, item.quote)}</strong></div><div><span>Opportunity score</span><strong>{item.score.toFixed(2)}</strong></div><div><span>Gross spread</span><strong>{pct(item.grossSpread)}</strong></div></div>
    <div className="drawer-section"><h3>Buy leg · {exchangeName(item.buyExchange)}</h3><DetailRow label="Best ask" value={money(item.buyPrice, item.quote)}/><DetailRow label="Buy VWAP" value={money(item.buyVWAP, item.quote)}/><DetailRow label="Quantity" value={`${qty(item.executableQuantity)} ${item.asset}`}/><DetailRow label="Estimated slippage" value={pct(item.buySlippage)}/><DetailRow label="Trading fee" value={money(item.buyFee, item.quote)}/></div>
    <div className="drawer-section"><h3>Sell leg · {exchangeName(item.sellExchange)}</h3><DetailRow label="Best bid" value={money(item.sellPrice, item.quote)}/><DetailRow label="Sell VWAP" value={money(item.sellVWAP, item.quote)}/><DetailRow label="Quantity" value={`${qty(item.executableQuantity)} ${item.asset}`}/><DetailRow label="Estimated slippage" value={pct(item.sellSlippage)}/><DetailRow label="Trading fee" value={money(item.sellFee, item.quote)}/></div>
    <div className="drawer-section"><h3>Phase D profit calculation</h3><DetailRow label="Depth-adjusted gross profit" value={money(item.grossProfit, item.quote)}/><DetailRow label="Buy fee" value={`− ${money(item.buyFee, item.quote)}`}/><DetailRow label="Sell fee" value={`− ${money(item.sellFee, item.quote)}`}/><DetailRow label="Estimated net profit" value={money(item.estimatedNetProfit, item.quote)} strong/><p className="calculation-note">Depth impact is already reflected in the VWAP-based gross profit above. Estimated slippage cost ({money(item.estimatedSlippageCost, item.quote)}) is shown for transparency and is not subtracted twice.</p></div>
    <div className="drawer-section"><h3>Depth consumed</h3><p className="drawer-note">Exact level fills used by Phase D for {qty(item.executableQuantity)} {item.asset}. Bars show consumed quantity relative to the largest fill in these two legs.</p><Depth title={`Buy asks · ${exchangeName(item.buyExchange)}`} fills={item.buyDepthUsed} max={maxLevel} color="buy" quote={item.quote}/><Depth title={`Sell bids · ${exchangeName(item.sellExchange)}`} fills={item.sellDepthUsed} max={maxLevel} color="sell" quote={item.quote}/></div>
    <div className="drawer-foot">Both books were healthy and sequence-valid when this Phase D estimate was generated. Market depth can change before any external action.</div>
  </aside>;
}
function DetailRow({ label, value, strong = false }: { label: string; value: string; strong?: boolean }) { return <div className={`detail-row ${strong ? 'strong-row' : ''}`}><span>{label}</span><strong>{value}</strong></div>; }
function Depth({ title, fills, max, color, quote }: { title: string; fills: Fill[]; max: number; color: 'buy' | 'sell'; quote: string }) { return <div className="depth-block"><div className="depth-title">{title}<span>{fills.length} {fills.length === 1 ? 'level' : 'levels'}</span></div>{fills.map((fill, index) => <div className="depth-row" key={`${fill.price}-${index}`}><span className="depth-price">{money(fill.price, quote)}</span><span className="depth-bar-track"><i className={`depth-bar ${color}`} style={{ width: `${max > 0 ? Math.max(4, fill.quantity / max * 100) : 0}%` }}/></span><span className="depth-qty">{qty(fill.quantity)} {`(${qty(fill.quantity / Math.max(1e-12, fills.reduce((sum, level) => sum + level.quantity, 0)) * 100)}%)`}</span></div>)}</div>; }
function numericFilter(value: string) { const number = Number(value); return value.trim() && Number.isFinite(number) ? number : 0; }
function opportunityAge(item: Opportunity, now: number) { return Math.max(0, now - item.timestamp); }
function key(item: Opportunity) { return `${item.pair}:${item.buyExchange}:${item.sellExchange}`; }
function bytes(value?: number) { return value == null ? '—' : value >= 1024 ** 3 ? `${(value / 1024 ** 3).toFixed(2)} GB` : `${(value / 1024 ** 2).toFixed(0)} MB`; }
