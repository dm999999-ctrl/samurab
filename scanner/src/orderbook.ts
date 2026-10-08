import type { NormalizedOrderBook, OrderBookLevel } from './types';
export type BookState='SYNCING'|'LIVE'|'STALE'|'RESYNCING'|'DISCONNECTED'|'ERROR';
export type OrderBookDelta={firstUpdateId:number|string;finalUpdateId:number|string;bids:OrderBookLevel[];asks:OrderBookLevel[];exchangeTimestamp:number|null};
export class LocalOrderBook {
 private bids=new Map<number,number>(); private asks=new Map<number,number>(); private lastUpdateId:number|string|null=null; private state:BookState='SYNCING'; private exchangeTimestamp:number|null=null; private receivedTimestamp=0; private processedTimestamp=0;
 constructor(readonly exchange:string,readonly symbol:string){}
 loadSnapshot(id:number|string,bids:OrderBookLevel[],asks:OrderBookLevel[],received=Date.now(),exchangeTimestamp:number|null=null){this.bids.clear();this.asks.clear();bids.forEach(x=>this.set(this.bids,x));asks.forEach(x=>this.set(this.asks,x));this.lastUpdateId=id;this.receivedTimestamp=received;this.exchangeTimestamp=exchangeTimestamp;this.processedTimestamp=Date.now();this.state='SYNCING';}
 apply(d:OrderBookDelta,received=Date.now()):boolean{if(this.lastUpdateId===null)return false;if(compareSequence(d.finalUpdateId,this.lastUpdateId)<=0)return true;if(compareSequence(d.firstUpdateId,incrementSequence(this.lastUpdateId))>0){this.state='RESYNCING';return false;}d.bids.forEach(x=>this.set(this.bids,x));d.asks.forEach(x=>this.set(this.asks,x));this.lastUpdateId=d.finalUpdateId;this.exchangeTimestamp=d.exchangeTimestamp;this.receivedTimestamp=received;this.processedTimestamp=Date.now();this.state='LIVE';return true;}
 markDisconnected(){this.state='DISCONNECTED';} markResyncing(){this.state='RESYNCING';} markLive(){this.state='LIVE';}
 get snapshot():NormalizedOrderBook{return this.getSnapshot();}
 getSnapshot(depth=1000):NormalizedOrderBook{
  if(!Number.isSafeInteger(depth)||depth<1)throw new Error('Order-book snapshot depth must be a positive integer');
  const limit=Math.min(depth,1000);
  return {exchange:this.exchange,symbol:this.symbol,exchangeTimestamp:this.exchangeTimestamp,receivedTimestamp:this.receivedTimestamp,processedTimestamp:this.processedTimestamp,bids:this.levels(this.bids,true,limit),asks:this.levels(this.asks,false,limit),sequence:this.lastUpdateId};
 }
 get snapshotMetadata(){return {exchange:this.exchange,symbol:this.symbol,exchangeTimestamp:this.exchangeTimestamp,receivedTimestamp:this.receivedTimestamp,processedTimestamp:this.processedTimestamp,sequence:this.lastUpdateId};}
 get status(){return this.state;} get sequence(){return this.lastUpdateId;}
 private set(m:Map<number,number>,x:OrderBookLevel){if(!Number.isFinite(x.price)||x.price<=0||!Number.isFinite(x.quantity)||x.quantity<0)return;x.quantity===0?m.delete(x.price):m.set(x.price,x.quantity);}
 private levels(m:Map<number,number>,desc:boolean,limit=1000){
  if(m.size<=limit)return [...m].map(([price,quantity])=>({price,quantity})).sort((a,b)=>desc?b.price-a.price:a.price-b.price);
  const top:OrderBookLevel[]=[];
  for(const [price,quantity] of m){
   let low=0,high=top.length;
   while(low<high){const middle=(low+high)>>>1;const precedes=desc?price>top[middle].price:price<top[middle].price;if(precedes)high=middle;else low=middle+1;}
   if(low>=limit)continue;
   top.splice(low,0,{price,quantity});
   if(top.length>limit)top.pop();
  }
  return top;
 }
}
function compareSequence(left:number|string,right:number|string){const a=BigInt(left),b=BigInt(right);return a<b?-1:a>b?1:0;}
function incrementSequence(value:number|string):number|string{return typeof value==='number'?value+1:(BigInt(value)+1n).toString();}
export function dataAgeMs(book:NormalizedOrderBook,now=Date.now()){return book.exchangeTimestamp===null?null:Math.max(0,now-book.exchangeTimestamp);}
export function freshness(age:number|null,thresholds={aging:500,stale:2000}):'LIVE'|'AGING'|'STALE'{return age===null?'STALE':age<thresholds.aging?'LIVE':age<thresholds.stale?'AGING':'STALE';}
export function createBook(exchange:string,symbol:string,bids:OrderBookLevel[],asks:OrderBookLevel[],sequence:number|string|null=null,exchangeTimestamp:number|null=null,now=Date.now()):NormalizedOrderBook{return {exchange,symbol,exchangeTimestamp,receivedTimestamp:now,processedTimestamp:Date.now(),bids:bids.filter(x=>x.price>0&&x.quantity>0).sort((a,b)=>b.price-a.price),asks:asks.filter(x=>x.price>0&&x.quantity>0).sort((a,b)=>a.price-b.price),sequence};}
