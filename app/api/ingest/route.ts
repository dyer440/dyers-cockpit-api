import { Pool } from "pg";
import crypto from "crypto";

export const runtime = "nodejs";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function requireEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const SECRET = requireEnv("COCKPIT_INGEST_SECRET");

export async function POST(req: Request) {
  try {
    const headerSecret = req.headers.get("x-cockpit-secret");
    if (headerSecret !== SECRET) {
      return new Response("Unauthorized", { status: 401 });
    }

    const body = await req.json();

    if (!body?.url || !body?.vertical) {
      return new Response("Bad Request: url and vertical required", {
        status: 400,
      });
    }

    const url = String(body.url).trim();
    const vertical = String(body.vertical).trim().toLowerCase();

    const urlHash = crypto
      .createHash("sha256")
      .update(url)
      .digest("hex");

    const result = await pool.query(
      `
      INSERT INTO raw_items
        (vertical, url, url_hash, source, source_channel_id, source_message_id, author_id, author_username, metadata)
      VALUES
        ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
      ON CONFLICT (url_hash) DO NOTHING
      RETURNING id
      `,
      [
        vertical,
        url,
        urlHash,
        body.source ?? "discord",
        body.source_channel_id ?? null,
        body.source_message_id ?? null,
        body.author_id ?? null,
        body.author_username ?? null,
        JSON.stringify({
          posted_at: body.posted_at ?? null,
        }),
      ]
    );

    const inserted = result.rowCount === 1;
    const id = inserted ? result.rows[0].id : null;

    return Response.json({ ok: true, inserted, id });
  } catch (err: any) {
    console.error(err);
    return new Response("Server Error", { status: 500 });
  }
}
