'use client';

import { useEffect, useState } from 'react';

type Market = {
  exchange?: string; symbol?: string; status?: string; bookStatus?: string;
  sequenceStatus?: string; reconnects?: number; error?: string | null;
  closeCode?: number | null; closeReason?: string | null; reconnectAttempt?: number;
  freshness?: string; dataAgeMs?: number | null; processingLatencyMs?: number | null;
  bestBid?: number | null; bestAsk?: number | null; bidSize?: number | null; askSize?: number | null;
};
type Opportunity = {
  valid: boolean; status: string; buyExchange: string; sellExchange: string;
  executableQuantity: number; averageBuyPrice: number; averageSellPrice: number;
  estimatedNetSpreadPercent: number; estimatedNetProfit: number;
  buyDataAgeMs: number | null; sellDataAgeMs: number | null;
};
type Evaluation = { status?: string; reason?: string | null };
type Telemetry = {
  scannerStatus?: string; bookStatus?: string; exchanges?: Record<string, Market>;
  evaluations?: { binanceToBybit?: Evaluation; bybitToBinance?: Evaluation };
  opportunities?: Opportunity[];
};

const empty: Telemetry = {
  scannerStatus: 'DISCONNECTED', bookStatus: 'OFFLINE', exchanges: {
    binance: { status: 'DISCONNECTED', bookStatus: 'OFFLINE', sequenceStatus: 'UNKNOWN', freshness: 'STALE' },
    bybit: { status: 'DISCONNECTED', bookStatus: 'OFFLINE', sequenceStatus: 'UNKNOWN', freshness: 'STALE' },
  }, evaluations: {}, opportunities: [],
};

const price = (value?: number | null) => value == null ? '—' : `$${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
const age = (value?: number | null) => value == null ? '—' : `${Math.round(value)} ms`;
const quantity = (value?: number | null) => value == null ? '—' : value.toFixed(5);

export default function Dashboard() {
  const [data, setData] = useState<Telemetry>(empty);
  const [connection, setConnection] = useState<'CONNECTING' | 'CONNECTED' | 'DISCONNECTED'>('CONNECTING');

  useEffect(() => {
    let source: EventSource | undefined;
    let cancelled = false;
    fetch('/api/telemetry').then((response) => {
      if (!response.ok) throw new Error(`Telemetry returned HTTP ${response.status}`);
      return response.json() as Promise<Telemetry>;
    }).then((snapshot) => { if (!cancelled) setData(snapshot); }).catch(() => {
      // Keep explicit disconnected defaults until the event stream supplies real telemetry.
    });
    try {
      source = new EventSource('/api/events');
      source.onopen = () => { if (!cancelled) setConnection('CONNECTED'); };
      source.onmessage = (event) => {
        try { const snapshot = JSON.parse(event.data) as Telemetry; if (!cancelled) setData(snapshot); }
        catch { /* Ignore malformed events; the stream can deliver the next valid snapshot. */ }
      };
      source.onerror = () => { if (!cancelled) setConnection('DISCONNECTED'); };
    } catch { setConnection('DISCONNECTED'); }
    return () => { cancelled = true; source?.close(); };
  }, []);

  const binance = data.exchanges?.binance ?? empty.exchanges!.binance;
  const bybit = data.exchanges?.bybit ?? empty.exchanges!.bybit;
  const valid = (data.opportunities ?? []).filter((item) => item.valid && item.status === 'OPPORTUNITY');
  const live = data.scannerStatus === 'LIVE';

  return <div className="shell">
    <aside className="sidebar">
      <div className="brand"><div className="brandmark"/><span>ARB SCANNER</span></div>
      <div className="navlabel">Workspace</div>
      <nav className="nav"><a className="active" href="/"><span>Live scanner</span></a><a href="#health"><span>System health</span></a></nav>
      <div className="side-bottom"><span className="mono">PHASE 2 / TWO EXCHANGES</span><br/>Public market data only<br/>No trading credentials</div>
    </aside>
    <main className="main">
      <header className="topbar"><div className="crumb">MARKETS / <span>CROSS-EXCHANGE SCANNER</span></div><div className={`status ${live ? '' : 'down'}`}><i className="dot"/> {data.scannerStatus ?? 'DISCONNECTED'}</div></header>
      <div className="content">
        <div className="title-row"><div><h1>BTC/USDT arbitrage</h1><div className="subtitle">Binance ↔ Bybit · depth-aware execution estimates · taker fees included</div></div><div className="livepill">● {live ? 'BOTH BOOKS LIVE' : data.bookStatus ?? 'OFFLINE'}</div></div>
        <div className="cards">
          <Card label="Validated opportunities" value={String(valid.length)} meta="Positive estimated net return only" green={valid.length > 0}/>
          <Card label="Binance best bid / ask" value={`${price(binance.bestBid)} / ${price(binance.bestAsk)}`} meta={`${binance.freshness ?? 'STALE'} · age ${age(binance.dataAgeMs)}`}/>
          <Card label="Bybit best bid / ask" value={`${price(bybit.bestBid)} / ${price(bybit.bestAsk)}`} meta={`${bybit.freshness ?? 'STALE'} · age ${age(bybit.dataAgeMs)}`}/>
          <Card label="Reference quantity" value="0.010 BTC" meta="Configured by scanner service"/>
        </div>
        <Section title="Exchange order books" note="SEQUENCE AND FRESHNESS VALIDATED"/>
        <div className="below books"><Exchange name="Binance" data={binance}/><Exchange name="Bybit" data={bybit}/></div>
        <Section title="Executable opportunities" note={`${valid.length} VALID ROUTE${valid.length === 1 ? '' : 'S'}`}/>
        {valid.length ? <div className="table-wrap"><table><thead><tr><th>Route</th><th>Quantity</th><th>Buy avg</th><th>Sell avg</th><th>Net spread</th><th>Est. net value</th><th>Book age</th></tr></thead><tbody>{valid.map((item, index) => <tr key={`${item.buyExchange}-${item.sellExchange}-${index}`}><td className="pair">{item.buyExchange} → {item.sellExchange}</td><td className="mono">{item.executableQuantity.toFixed(6)} BTC</td><td className="mono">{price(item.averageBuyPrice)}</td><td className="mono">{price(item.averageSellPrice)}</td><td className="spread up">{(100 * item.estimatedNetSpreadPercent).toFixed(4)}%</td><td className="value up">{price(item.estimatedNetProfit)}</td><td className="mono">{age(Math.max(item.buyDataAgeMs ?? 0, item.sellDataAgeMs ?? 0))}</td></tr>)}</tbody></table></div> : <div className="panel">
          <h2>No validated opportunity</h2><p className="subtitle">The dashboard only lists opportunities when both books are synchronized, fresh, valid, sufficiently liquid, and net positive after fees and estimated execution impact.</p>
          <EvaluationRow label="Binance → Bybit evaluation" evaluation={data.evaluations?.binanceToBybit}/><EvaluationRow label="Bybit → Binance evaluation" evaluation={data.evaluations?.bybitToBinance}/>
        </div>}
        <p className="footnote">Estimated net returns use configured standard taker-fee assumptions. They are market-data estimates, not guaranteed returns or execution guarantees.</p>
        <div id="health" className="connection-note">Telemetry stream: {connection} · proxied from scanner <span className="mono">/telemetry</span> + <span className="mono">/events</span></div>
      </div>
    </main>
  </div>;
}

function Card({ label, value, meta, green = false }: { label: string; value: string; meta: string; green?: boolean }) {
  return <div className="card"><div className="card-label">{label}</div><div className={`card-value ${green ? 'up' : ''}`}>{value}</div><div className="card-meta">{meta}</div></div>;
}
function Section({ title, note }: { title: string; note: string }) {
  return <div className="section-head"><h2>{title}</h2><span className="section-note">{note}</span></div>;
}
function Exchange({ name, data }: { name: string; data: Market }) {
  const connected = data.status === 'LIVE' && data.freshness === 'LIVE';
  return <section className="panel"><div className="section-head section-top"><h2>{name}</h2><span className={`fresh ${connected ? '' : 'down'}`}><i className="dot"/>{data.status ?? 'DISCONNECTED'} · {data.freshness ?? 'STALE'}</span></div>
    <Row label="Best bid" value={price(data.bestBid)}/><Row label="Best ask" value={price(data.bestAsk)}/><Row label="Top bid / ask size" value={`${quantity(data.bidSize)} / ${quantity(data.askSize)} BTC`}/><Row label="Sequence state" value={data.sequenceStatus ?? 'UNKNOWN'}/><Row label="Data age / processing" value={`${age(data.dataAgeMs)} / ${age(data.processingLatencyMs)}`}/><Row label="Reconnect count" value={String(data.reconnects ?? 0)}/>
    {data.error && <div className="error-detail"><div>Error: {data.error}</div><div>Close code: {data.closeCode ?? '—'}</div><div>Close reason: {data.closeReason || '—'}</div><div>Reconnect attempt: {data.reconnectAttempt ?? '—'}</div></div>}
  </section>;
}
function Row({ label, value }: { label: string; value: string }) { return <div className="health-row"><span>{label}</span><span className="mono">{value}</span></div>; }
function EvaluationRow({ label, evaluation }: { label: string; evaluation?: Evaluation }) {
  return <Row label={label} value={`${evaluation?.status ?? 'WAITING FOR DATA'}${evaluation?.reason ? ` · ${evaluation.reason}` : ''}`}/>;
}
