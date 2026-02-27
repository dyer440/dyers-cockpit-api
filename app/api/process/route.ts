// app/api/process/route.ts
import { Pool } from "pg";

export const runtime = "nodejs";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// ---------- helpers ----------
function getEnv(name: string) {
  return process.env[name] || "";
}

function mustEnvRuntime(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function clampLimit(n: any, def = 20) {
  const x = Number(n);
  if (!Number.isFinite(x) || x <= 0) return def;
  return Math.max(1, Math.min(50, Math.floor(x)));
}

function clampInt(n: any, def: number, min: number, max: number) {
  const x = Number(n);
  if (!Number.isFinite(x)) return def;
  return Math.max(min, Math.min(max, Math.floor(x)));
}

function toLowerSafe(s: any) {
  return String(s ?? "").trim().toLowerCase();
}

function stripHtml(html: string) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<noscript[\s\S]*?<\/noscript>/gi, " ")
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function withTimeout<T>(p: Promise<T>, ms: number, label = "timeout"): Promise<T> {
  let t: any;
  const timeout = new Promise<T>((_, rej) => {
    t = setTimeout(() => rej(new Error(`${label} after ${ms}ms`)), ms);
  });
  return Promise.race([p, timeout]).finally(() => clearTimeout(t));
}

// ---------- fetch ----------
async function fetchText(url: string) {
  const res = await withTimeout(
    fetch(url, {
      redirect: "follow",
      headers: {
        "User-Agent":
          "Mozilla/5.0 (compatible; DyersCockpitBot/1.0; +https://dyerempire.com)",
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.7",
      },
    }),
    20000,
    "fetch"
  );

  const ct = (res.headers.get("content-type") || "").toLowerCase();
  const body = await withTimeout(res.text(), 20000, "read_body");

  if (!res.ok) throw new Error(`Fetch failed ${res.status}`);

  // If HTML, strip it. Otherwise return raw body.
  const text = ct.includes("text/html") ? stripHtml(body) : body;

  // Keep a sane max; OpenAI prompt also slices further.
  return text.slice(0, 250_000);
}

// ---------- OpenAI ----------
const ALLOWED_TAGS = new Set([
  "Mining",
  "Concentrate",
  "MREC",
  "Separation",
  "Metal",
  "Magnet",
  "Recycling",
  "Policy",
  "Finance",
  "Other",
]);

const ALLOWED_VIS = new Set(["public", "pro", "internal"]);

function normalizeTags(tags: any): string[] {
  if (!Array.isArray(tags)) return ["Other"];

  const cleaned: string[] = [];

  for (const raw of tags) {
    const t = String(raw ?? "").trim();
    if (!t) continue;

    let match = "";
    ALLOWED_TAGS.forEach((x) => {
      if (!match && x.toLowerCase() === t.toLowerCase()) {
        match = x;
      }
    });

    if (match && cleaned.indexOf(match) === -1) {
      cleaned.push(match);
    }
  }

  return cleaned.length ? cleaned.slice(0, 8) : ["Other"];
}

function normalizeVisibility(v: any): "public" | "pro" | "internal" {
  const s = String(v ?? "").trim().toLowerCase();
  if (ALLOWED_VIS.has(s)) return s as any;
  return "public";
}

function normalizeBullets(b: any): string[] {
  const arr = Array.isArray(b) ? b : [];
  const cleaned = arr
    .map((x) => String(x ?? "").trim())
    .filter(Boolean)
    .map((x) => x.replace(/^\s*[-•]\s*/, "").trim())
    .slice(0, 5);

  if (cleaned.length !== 5) throw new Error("Model output bullets must be exactly 5");
  return cleaned;
}

function normalizeSummary(s: any) {
  const out = String(s ?? "").trim();
  if (!out) throw new Error("Model output missing summary_1");
  return out;
}

function normalizeWhy(w: any) {
  const out = String(w ?? "").trim();
  return out || null;
}

function normalizeTitle(t: any) {
  const out = String(t ?? "").trim();
  return out || null;
}

function normalizeEntities(e: any) {
  // Store as JSONB. Prefer arrays/objects; fallback to empty.
  if (Array.isArray(e) || (e && typeof e === "object")) return e;
  return {};
}

async function callOpenAI(opts: {
  apiKey: string;
  model: string;
  inputText: string;
  url: string;
  vertical: string;
}) {
  const { apiKey, model, inputText, url, vertical } = opts;

  const prompt = `
You are a news analyst for ${vertical.toUpperCase()} markets.

Return strict JSON with keys:
title (string or null),
summary_1 (string, 1 sentence),
bullets (array of exactly 5 short bullets),
why_it_matters (string, 1-2 sentences),
tags (array of supply-chain tags from: Mining, Concentrate, MREC, Separation, Metal, Magnet, Recycling, Policy, Finance, Other),
entities (object or array; keep it simple),
relevance_score (integer 0-100),
visibility (one of: public, pro, internal)

Article URL: ${url}

Content:
${inputText.slice(0, 12000)}
`.trim();

  const r = await withTimeout(
    fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model,
        input: prompt,
        // Responses API JSON mode
        text: { format: { type: "json_object" } },
      }),
    }),
    30000,
    "openai"
  );

  if (!r.ok) {
    const t = await r.text().catch(() => "");
    throw new Error(`OpenAI failed ${r.status}: ${t.slice(0, 400)}`);
  }

  const data: any = await r.json();

  // Preferred: Responses API may provide output_json
  if (data?.output_json && typeof data.output_json === "object") return data.output_json;

  // Fallback: attempt parse output_text / structured output blocks
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

// ---------- DB picking strategy ----------
// IMPORTANT: this prevents double-processing when multiple workers/loops overlap.
// We lock + mark status inside one transaction using SKIP LOCKED.
async function pickRawItems(limit: number): Promise<Array<{ id: number; vertical: string; url: string }>> {
  const client = await pool.connect();
  try {
    await client.query("begin");

    const q = await client.query(
      `
      with picked as (
        select id, vertical, url
        from raw_items
        where status = 'new'
        order by created_at asc
        for update skip locked
        limit $1
      )
      update raw_items r
      set status = 'processing'
      from picked
      where r.id = picked.id
      returning picked.id, picked.vertical, picked.url
      `,
      [limit]
    );

    await client.query("commit");
    return q.rows.map((r) => ({
      id: Number(r.id),
      vertical: String(r.vertical || ""),
      url: String(r.url || ""),
    }));
  } catch (e) {
    try {
      await client.query("rollback");
    } catch {}
    throw e;
  } finally {
    client.release();
  }
}

async function processBatch(limit: number, apiKey: string, model: string) {
  const rows = await pickRawItems(limit);

  const results: Array<{ id: number; ok: boolean; error?: string }> = [];

  for (const row of rows) {
    const rawId = Number(row.id);
    const vertical = toLowerSafe(row.vertical) || "ree";
    const url = String(row.url || "");

    try {
      const content = await fetchText(url);
      const out = await callOpenAI({ apiKey, model, inputText: content, url, vertical });

      const summary_1 = normalizeSummary(out?.summary_1);
      const bullets = normalizeBullets(out?.bullets);
      const title = normalizeTitle(out?.title);
      const why = normalizeWhy(out?.why_it_matters);
      const tags = normalizeTags(out?.tags);
      const entities = normalizeEntities(out?.entities);
      const relevance_score = clampInt(out?.relevance_score, 0, 0, 100);
      const visibility = normalizeVisibility(out?.visibility);

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
          title,
          summary_1,
          JSON.stringify(bullets),
          why,
          JSON.stringify(tags),
          JSON.stringify(entities),
          relevance_score,
          visibility,
          model,
        ]
      );

      await pool.query(
        `update raw_items set status='processed', processed_at=now(), last_error=null where id=$1`,
        [rawId]
      );

      results.push({ id: rawId, ok: true });
    } catch (e: any) {
      const msg = String(e?.message ?? e);

      // If the row is already 'processed' (race or retry), don't downgrade it.
      // Otherwise, mark error.
      await pool.query(
        `
        update raw_items
        set status = case when status = 'processed' then 'processed' else 'error' end,
            last_error = $2
        where id = $1
        `,
        [rawId, msg.slice(0, 5000)]
      );

      results.push({ id: rawId, ok: false, error: msg });
    }
  }

  return { picked: rows.length, results };
}

// ---------- handlers ----------
export async function GET(req: Request) {
  try {
    const u = new URL(req.url);

    const processSecret = mustEnvRuntime("COCKPIT_PROCESS_SECRET");
    const secret = u.searchParams.get("secret");
    if (secret !== processSecret) return new Response("Unauthorized", { status: 401 });

    const apiKey = mustEnvRuntime("OPENAI_API_KEY");
    const model = getEnv("OPENAI_MODEL") || "gpt-4.1-nano";

    const limit = clampLimit(u.searchParams.get("limit"), 20);
    const out = await processBatch(limit, apiKey, model);

    return Response.json({
      ok: true,
      mode: "cron_get",
      limit,
      picked: out.picked,
      processed: out.results.filter((r) => r.ok).length,
      results: out.results,
    });
  } catch (e: any) {
    return new Response(String(e?.message ?? e), { status: 500 });
  }
}

export async function POST(req: Request) {
  try {
    const processSecret = mustEnvRuntime("COCKPIT_PROCESS_SECRET");
    const secret = req.headers.get("x-cockpit-secret");
    if (secret !== processSecret) return new Response("Unauthorized", { status: 401 });

    const apiKey = mustEnvRuntime("OPENAI_API_KEY");
    const model = getEnv("OPENAI_MODEL") || "gpt-4.1-nano";

    const body = await req.json().catch(() => ({}));
    const limit = clampLimit(body?.limit, 20);

    const out = await processBatch(limit, apiKey, model);

    return Response.json({
      ok: true,
      mode: "manual_post",
      limit,
      picked: out.picked,
      processed: out.results.filter((r) => r.ok).length,
      results: out.results,
    });
  } catch (e: any) {
    return new Response(String(e?.message ?? e), { status: 500 });
  }
}
