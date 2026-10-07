import type { NormalizedOrderBook, OrderBookLevel } from './types';
export type BookState='SYNCING'|'LIVE'|'STALE'|'RESYNCING'|'DISCONNECTED'|'ERROR';
export type OrderBookDelta={firstUpdateId:number;finalUpdateId:number;bids:OrderBookLevel[];asks:OrderBookLevel[];exchangeTimestamp:number|null};
export class LocalOrderBook {
 private bids=new Map<number,number>(); private asks=new Map<number,number>(); private lastUpdateId:number|null=null; private state:BookState='SYNCING'; private exchangeTimestamp:number|null=null; private receivedTimestamp=0; private processedTimestamp=0;
 constructor(readonly exchange:string,readonly symbol:string){}
 loadSnapshot(id:number,bids:OrderBookLevel[],asks:OrderBookLevel[],received=Date.now(),exchangeTimestamp:number|null=null){this.bids.clear();this.asks.clear();bids.forEach(x=>this.set(this.bids,x));asks.forEach(x=>this.set(this.asks,x));this.lastUpdateId=id;this.receivedTimestamp=received;this.exchangeTimestamp=exchangeTimestamp;this.processedTimestamp=Date.now();this.state='SYNCING';}
 apply(d:OrderBookDelta,received=Date.now()):boolean{if(this.lastUpdateId===null)return false;if(d.finalUpdateId<=this.lastUpdateId)return true;if(d.firstUpdateId>this.lastUpdateId+1){this.state='RESYNCING';return false;}d.bids.forEach(x=>this.set(this.bids,x));d.asks.forEach(x=>this.set(this.asks,x));this.lastUpdateId=d.finalUpdateId;this.exchangeTimestamp=d.exchangeTimestamp;this.receivedTimestamp=received;this.processedTimestamp=Date.now();this.state='LIVE';return true;}
 markDisconnected(){this.state='DISCONNECTED';} markResyncing(){this.state='RESYNCING';} markLive(){this.state='LIVE';}
 get snapshot():NormalizedOrderBook{return {exchange:this.exchange,symbol:this.symbol,exchangeTimestamp:this.exchangeTimestamp,receivedTimestamp:this.receivedTimestamp,processedTimestamp:this.processedTimestamp,bids:this.levels(this.bids,true),asks:this.levels(this.asks,false),sequence:this.lastUpdateId};}
 get status(){return this.state;} get sequence(){return this.lastUpdateId;}
 private set(m:Map<number,number>,x:OrderBookLevel){if(!Number.isFinite(x.price)||x.price<=0||!Number.isFinite(x.quantity)||x.quantity<0)return;x.quantity===0?m.delete(x.price):m.set(x.price,x.quantity);}
 private levels(m:Map<number,number>,desc:boolean){return [...m].map(([price,quantity])=>({price,quantity})).sort((a,b)=>desc?b.price-a.price:a.price-b.price).slice(0,1000);}
}
export function dataAgeMs(book:NormalizedOrderBook,now=Date.now()){return book.exchangeTimestamp===null?null:Math.max(0,now-book.exchangeTimestamp);}
export function freshness(age:number|null,thresholds={aging:500,stale:2000}):'LIVE'|'AGING'|'STALE'{return age===null?'STALE':age<thresholds.aging?'LIVE':age<thresholds.stale?'AGING':'STALE';}
export function createBook(exchange:string,symbol:string,bids:OrderBookLevel[],asks:OrderBookLevel[],sequence:number|string|null=null,exchangeTimestamp:number|null=null,now=Date.now()):NormalizedOrderBook{return {exchange,symbol,exchangeTimestamp,receivedTimestamp:now,processedTimestamp:Date.now(),bids:bids.filter(x=>x.price>0&&x.quantity>0).sort((a,b)=>b.price-a.price),asks:asks.filter(x=>x.price>0&&x.quantity>0).sort((a,b)=>a.price-b.price),sequence};}
