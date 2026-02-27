// app/api/sources/rss/seen/route.ts
import { Pool } from "pg";

export const runtime = "nodejs";
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

function mustEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

export async function POST(req: Request) {
  try {
    const secret = req.headers.get("x-cockpit-secret") || "";
    const expected = mustEnv("COCKPIT_PROCESS_SECRET"); // reuse
    if (secret !== expected) return new Response("Unauthorized", { status: 401 });

    const body = await req.json().catch(() => ({}));
    const sourceId = Number(body?.source_id);
    const keys: string[] = Array.isArray(body?.keys)
      ? body.keys.map((x: any) => String(x)).filter((s: string) => s.length > 0).slice(0, 200)
      : [];

    if (!Number.isFinite(sourceId) || sourceId <= 0) {
      return new Response("Bad source_id", { status: 400 });
    }

    if (!keys.length) return Response.json({ ok: true, seen: [] });

    const r = await pool.query(
      `
      select item_key
      from source_seen
      where source_id = $1 and item_key = any($2::text[])
      `,
      [sourceId, keys]
    );

    const seen = r.rows.map((x) => x.item_key);
    return Response.json({ ok: true, seen });
  } catch (e: any) {
    return new Response(String(e?.message ?? e), { status: 500 });
  }
}
