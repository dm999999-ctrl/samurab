const scannerUrl = process.env.SCANNER_TELEMETRY_URL ?? 'http://127.0.0.1:4000/telemetry';

export async function GET() {
  try {
    const upstream = await fetch(scannerUrl, { cache: 'no-store' });
    return new Response(upstream.body, {
      status: upstream.status,
      headers: {
        'content-type': upstream.headers.get('content-type') ?? 'application/json',
        'cache-control': 'no-store',
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return Response.json({ error: `Scanner telemetry unavailable: ${message}` }, { status: 502 });
  }
}
