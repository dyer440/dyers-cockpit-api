import { Pool } from "pg";

export const runtime = "nodejs";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function mustEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const PROCESS_SECRET = mustEnv("COCKPIT_PROCESS_SECRET");
const OPENAI_API_KEY = mustEnv("OPENAI_API_KEY");
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";

function clampLimit(n: any, def = 20) {
  const x = Number(n);
  if (!Number.isFinite(x) || x <= 0) return def;
  return Math.max(1, Math.min(50, Math.floor(x)));
}

function stripHtml(html: string) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function fetchText(url: string) {
  const res = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (compatible; DyersCockpitBot/1.0; +https://dyerempire.com)",
    },
  });

  const ct = res.headers.get("content-type") || "";
  const body = await res.text();

  if (!res.ok) throw new Error(`Fetch failed ${res.status}`);

  return ct.includes("text/html") ? stripHtml(body) : body;
}

async function callOpenAI(inputText: string, url: string, vertical: string) {
  const prompt = `
You are a news analyst for ${vertical.toUpperCase()} markets.

Return strict JSON with keys:
title (string or null),
summary_1 (string, 1 sentence),
bullets (array of exactly 5 short bullets),
why_it_matters (string, 1-2 sentences),
tags (array of supply-chain tags from: Mining, Concentrate, MREC, Separation, Metal, Magnet, Recycling, Policy, Finance, Other),
relevance_score (integer 0-100),
visibility (one of: public, pro, internal)

Article URL: ${url}

Content:
${inputText.slice(0, 12000)}
`.trim();

  const r = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: OPENAI_MODEL,
      input: prompt,
      response_format: { type: "json_object" },
    }),
  });

  if (!r.ok) {
    const t = await r.text().catch(() => "");
    throw new Error(`OpenAI failed ${r.status}: ${t.slice(0, 400)}`);
  }

  const data: any = await r.json();

  // Try the most robust path first
  if (data?.output_json && typeof data.output_json === "object") return data.output_json;

  // Otherwise reconstruct output text
  const outputText =
    data?.output_text ??
    data?.output
      ?.map((o: any) =>
        (o?.content || [])
          .map((c: any) => (typeof c?.text === "string" ? c.text : ""))
          .join("")
      )
      .join("") ??
    "";

  try {
    return JSON.parse(outputText);
  } catch {
    throw new Error("Could not parse model JSON");
  }
}

async function processBatch(limit: number) {
  const picked = await pool.query(
    `
    select id, vertical, url
    from raw_items
    where status = 'new'
    order by created_at asc
    limit $1
    `,
    [limit]
  );

  const results: Array<{ id: number; ok: boolean; error?: string }> = [];

  for (const row of picked.rows) {
    const rawId = Number(row.id);
    const vertical = String(row.vertical || "").toLowerCase();
    const url = String(row.url || "");

    try {
      await pool.query(`update raw_items set status='processing' where id=$1`, [rawId]);

      const content = await fetchText(url);
      const out = await callOpenAI(content, url, vertical);

      // Minimal validation
      const summary_1 = String(out?.summary_1 ?? "").trim();
      const bullets = Array.isArray(out?.bullets) ? out.bullets : [];
      if (!summary_1) throw new Error("Model output missing summary_1");
      if (bullets.length !== 5) throw new Error("Model output bullets must be exactly 5");

      await pool.query(
        `
        insert into processed_items
          (raw_item_id, vertical, url, title, summary_1, bullets, why_it_matters, tags, entities, relevance_score, visibility, model)
        values
          ($1,$2,$3,$4,$5,$6::jsonb,$7,$8::jsonb,$9::jsonb,$10,$11,$12)
        on conflict (raw_item_id) do nothing
        `,
        [
          rawId,
          vertical,
          url,
          out?.title ?? null,
          summary_1,
          JSON.stringify(bullets),
          out?.why_it_matters ?? null,
          JSON.stringify(out?.tags ?? []),
          JSON.stringify(out?.entities ?? {}),
          Number(out?.relevance_score ?? 0),
          out?.visibility ?? "public",
          OPENAI_MODEL,
        ]
      );

      await pool.query(
        `update raw_items set status='processed', processed_at=now(), last_error=null where id=$1`,
        [rawId]
      );

      results.push({ id: rawId, ok: true });
    } catch (e: any) {
      const msg = String(e?.message ?? e);
      await pool.query(
        `update raw_items set status='error', last_error=$2 where id=$1`,
        [rawId, msg]
      );
      results.push({ id: rawId, ok: false, error: msg });
    }
  }

  return { picked: picked.rowCount, results };
}

export async function GET(req: Request) {
  const u = new URL(req.url);
  const secret = u.searchParams.get("secret");
  if (secret !== PROCESS_SECRET) return new Response("Unauthorized", { status: 401 });

  const limit = clampLimit(u.searchParams.get("limit"), 20);
  const out = await processBatch(limit);

  return Response.json({
    ok: true,
    mode: "cron_get",
    limit,
    picked: out.picked,
    processed: out.results.length,
    results: out.results,
  });
}

export async function POST(req: Request) {
  const secret = req.headers.get("x-cockpit-secret");
  if (secret !== PROCESS_SECRET) return new Response("Unauthorized", { status: 401 });

  const body = await req.json().catch(() => ({}));
  const limit = clampLimit(body?.limit, 20);
  const out = await processBatch(limit);

  return Response.json({
    ok: true,
    mode: "manual_post",
    limit,
    picked: out.picked,
    processed: out.results.length,
    results: out.results,
  });
}
