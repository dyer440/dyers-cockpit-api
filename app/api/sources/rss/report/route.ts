// app/api/sources/rss/report/route.ts
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
    if (!Number.isFinite(sourceId) || sourceId <= 0) {
      return new Response("Bad source_id", { status: 400 });
    }

    const etag = typeof body?.etag === "string" ? body.etag : null;
    const lastModified = typeof body?.last_modified === "string" ? body.last_modified : null;
    const lastError = typeof body?.last_error === "string" ? body.last_error : null;

    const seenKeys: string[] = Array.isArray(body?.seen_keys)
      ? body.seen_keys.map((x: any) => String(x)).filter((s: string) => s.length > 0).slice(0, 200)
      : [];

    const client = await pool.connect();
    try {
      await client.query("begin");

      await client.query(
        `
        update sources
        set last_polled_at = now(),
            etag = $2,
            last_modified = $3,
            last_error = $4
        where id = $1
        `,
        [sourceId, etag, lastModified, lastError]
      );

      if (seenKeys.length) {
        // Bulk insert using unnest
        await client.query(
          `
          insert into source_seen (source_id, item_key)
          select $1, k
          from unnest($2::text[]) as k
          on conflict do nothing
          `,
          [sourceId, seenKeys]
        );
      }

      await client.query("commit");
    } catch (e) {
      try {
        await client.query("rollback");
      } catch {}
      throw e;
    } finally {
      client.release();
    }

    return Response.json({ ok: true, seen: seenKeys.length });
  } catch (e: any) {
    return new Response(String(e?.message ?? e), { status: 500 });
  }
}
