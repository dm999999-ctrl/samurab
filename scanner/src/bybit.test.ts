import { describe,expect,it } from 'vitest';
import { BybitAdapter } from './bybit';

const snap=(u:number)=>JSON.stringify({topic:'orderbook.200.BTCUSDT',type:'snapshot',ts:1000,cts:990,data:{s:'BTCUSDT',b:[['100','2']],a:[['101','3']],u,seq:77}});
const delta=(u:number,b:string[][],a:string[][]=[])=>JSON.stringify({topic:'orderbook.200.BTCUSDT',type:'delta',ts:1100,cts:1090,data:{s:'BTCUSDT',b,a,u,seq:78}});
describe('Bybit public Spot order-book adapter parsing',()=>{
 it('loads streamed snapshot and applies insert/update/delete deltas',()=>{const a=new BybitAdapter(()=>{});a.receiveForTest(snap(50));expect(a.getStatus()).toBe('LIVE');expect(a.getSnapshot().bids[0]).toEqual({price:100,quantity:2});expect(a.getSnapshot().exchangeTimestamp).toBe(990);a.receiveForTest(delta(51,[['100','4'],['99','1']]));expect(a.getSnapshot().bids[0].quantity).toBe(4);a.receiveForTest(delta(52,[['100','0']]));expect(a.getSnapshot().bids[0].price).toBe(99);});
 it('replaces the book on a new snapshot and ignores an old delta',()=>{const a=new BybitAdapter(()=>{});a.receiveForTest(snap(50));a.receiveForTest(snap(60));a.receiveForTest(delta(59,[['98','5']]));expect(a.getSnapshot().sequence).toBe(60);expect(a.getSnapshot().bids[0].price).toBe(100);});
 it('fails closed on malformed order book messages',()=>{const a=new BybitAdapter(()=>{});a.receiveForTest('{bad');expect(a.getStatus()).toBe('ERROR');expect(a.getDiagnostics().error).toContain('malformed Bybit message');});
});
