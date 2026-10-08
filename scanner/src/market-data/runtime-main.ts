import { MarketDataRuntime, runtimeOptionsFromEnv } from './runtime';

const runtime = await MarketDataRuntime.create(runtimeOptionsFromEnv());
await runtime.start();

let stopping = false;
async function stop(signal: string) {
  if (stopping) return;
  stopping = true;
  console.log(`Phase B market-data runtime: received ${signal}`);
  await runtime.stop();
  process.exitCode = 0;
}
process.once('SIGINT', () => { void stop('SIGINT'); });
process.once('SIGTERM', () => { void stop('SIGTERM'); });
