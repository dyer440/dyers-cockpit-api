// app/api/brief/mark-posted/route.ts
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
    const expected = mustEnv("COCKPIT_PROCESS_SECRET");
    if (secret !== expected) return new Response("Unauthorized", { status: 401 });

    const body = await req.json().catch(() => ({}));
    const ids: number[] = Array.isArray(body?.ids) ? body.ids.map((x: any) => Number(x)) : [];
    const clean = ids.filter((x) => Number.isFinite(x) && x > 0);

    if (!clean.length) return Response.json({ ok: true, updated: 0 });

    const r = await pool.query(
      `
      update processed_items
      set posted_at = now()
      where id = any($1::int[])
      `,
      [clean]
    );

    return Response.json({ ok: true, updated: r.rowCount });
  } catch (e: any) {
    return new Response(String(e?.message ?? e), { status: 500 });
  }
}
