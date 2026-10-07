const scannerUrl = process.env.SCANNER_EVENTS_URL ?? 'http://127.0.0.1:4000/events';

export async function GET(request: Request) {
  try {
    const upstream = await fetch(scannerUrl, { cache: 'no-store', signal: request.signal });
    if (!upstream.ok || !upstream.body) {
      return new Response('Scanner event stream unavailable', { status: upstream.status || 502 });
    }
    return new Response(upstream.body, {
      headers: {
        'content-type': 'text/event-stream',
        'cache-control': 'no-cache, no-transform',
        connection: 'keep-alive',
        'x-accel-buffering': 'no',
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return new Response(`event: error\ndata: ${JSON.stringify({ error: message })}\n\n`, {
      status: 502,
      headers: { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' },
    });
  }
}
