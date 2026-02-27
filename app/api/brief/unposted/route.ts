// app/api/brief/unposted/route.ts
import { Pool } from "pg";

export const runtime = "nodejs";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

function mustEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function clampLimit(n: any, def = 5) {
  const x = Number(n);
  if (!Number.isFinite(x) || x <= 0) return def;
  return Math.max(1, Math.min(20, Math.floor(x)));
}

function normVertical(v: any) {
  const s = String(v ?? "").trim().toLowerCase();
  if (s === "coal" || s === "ree" || s === "policy") return s;
  return "ree";
}

export async function GET(req: Request) {
  try {
    const secret = req.headers.get("x-cockpit-secret") || "";
    const expected = mustEnv("COCKPIT_PROCESS_SECRET");
    if (secret !== expected) return new Response("Unauthorized", { status: 401 });

    const u = new URL(req.url);
    const vertical = normVertical(u.searchParams.get("vertical"));
    const limit = clampLimit(u.searchParams.get("limit"), 5);

    const r = await pool.query(
      `
      select
        id, raw_item_id, vertical, url, title, summary_1, bullets, why_it_matters,
        tags, entities, relevance_score, visibility, model, created_at
      from processed_items
      where vertical = $1
        and visibility = 'public'
        and posted_at is null
      order by created_at desc
      limit $2
      `,
      [vertical, limit]
    );

    return Response.json({ ok: true, vertical, limit, items: r.rows });
  } catch (e: any) {
    return new Response(String(e?.message ?? e), { status: 500 });
  }
}
