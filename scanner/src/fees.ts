export type FeeCurrencyAssumption='QUOTE'|'BASE';
export type ExchangeFeeSchedule={exchange:string;symbol:string;marketType:'spot';makerFee:number;takerFee:number;feeCurrencyAssumption:FeeCurrencyAssumption;source:string;};
export const feeSchedules:Record<string,ExchangeFeeSchedule>={
 binance:{exchange:'binance',symbol:'BTC/USDT',marketType:'spot',makerFee:0.001,takerFee:Number(process.env.BINANCE_SPOT_TAKER_FEE??0.001),feeCurrencyAssumption:'QUOTE',source:'Standard public spot taker assumption; configurable, not account-specific.'},
 bybit:{exchange:'bybit',symbol:'BTC/USDT',marketType:'spot',makerFee:0.001,takerFee:Number(process.env.BYBIT_SPOT_TAKER_FEE??0.001),feeCurrencyAssumption:'BASE',source:'Bybit regular-user crypto spot fee assumption 0.10%; configurable, not account-specific.'},
};
