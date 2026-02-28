// app/api/sources/rss/route.ts
import { Pool } from "pg";

export const runtime = "nodejs";
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

function mustEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function clampLimit(n: any, def = 100) {
  const x = Number(n);
  if (!Number.isFinite(x) || x <= 0) return def;
  return Math.max(1, Math.min(500, Math.floor(x)));
}

export async function GET(req: Request) {
  try {
    const secret = req.headers.get("x-cockpit-secret") || "";
    const expected = mustEnv("COCKPIT_PROCESS_SECRET"); // reuse
    if (secret !== expected) return new Response("Unauthorized", { status: 401 });

    const u = new URL(req.url);
    const limit = clampLimit(u.searchParams.get("limit"), 100);

    const r = await pool.query(
      `
      select id, vertical, name, url, poll_interval_min, etag, last_modified
      from sources
      where enabled = true
        and type = 'rss'
        and (
          last_polled_at is null
          or last_polled_at <= now() - make_interval(mins => poll_interval_min)
        )
      order by last_polled_at nulls first, id asc
      limit $1
      `,
      [limit]
    );

    return Response.json({ ok: true, sources: r.rows });
  } catch (e: any) {
    return new Response(String(e?.message ?? e), { status: 500 });
  }
}
