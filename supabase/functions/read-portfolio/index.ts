// Reads brokerage screenshots and returns the holdings as structured data.
// The app shows the result for review; nothing is written until the user applies it.
import Anthropic from "npm:@anthropic-ai/sdk";
import { admin, cors, reply, requireUser } from "../_shared/http.ts";

const client = new Anthropic();
const MEDIA = ["image/jpeg", "image/png", "image/webp", "image/gif"] as const;
type Media = (typeof MEDIA)[number];

const num = { type: ["number", "null"] };
const str = { type: ["string", "null"] };
const SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["broker", "as_of", "base_currency", "cash", "total_value", "holdings", "notes"],
  properties: {
    broker: str,
    as_of: { ...str, description: "Date or time shown on the screenshot, ISO 8601 if possible" },
    base_currency: { ...str, description: "Currency of totals and cash, e.g. SAR or USD" },
    cash: num,
    total_value: num,
    notes: { type: "string", description: "Anything ambiguous or cut off, in one or two sentences" },
    holdings: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        required: ["symbol", "name", "exchange", "currency", "shares", "avg_cost", "last_price", "market_value", "confidence"],
        properties: {
          symbol: { type: "string", description: "Ticker as listed on its exchange, uppercase. For Tadawul use the 4-digit code followed by .SR, e.g. 1120.SR" },
          name: str,
          exchange: str,
          currency: str,
          shares: { ...num, description: "Number of shares/units held" },
          avg_cost: { ...num, description: "Average cost per share, in the holding's currency" },
          last_price: { ...num, description: "Current/last price per share shown" },
          market_value: num,
          confidence: { type: "string", enum: ["high", "medium", "low"] },
        },
      },
    },
  },
};

const INSTRUCTIONS =
  "These are screenshots of an investor's brokerage portfolio (one account, possibly several scrolled screens). " +
  "Extract every holding exactly as shown. Rules:\n" +
  "- Read numbers exactly; do not calculate or estimate a value that is not visible, use null instead.\n" +
  "- If only market value and price are shown, leave shares null.\n" +
  "- The same holding may appear on two overlapping screenshots: list it once.\n" +
  "- Map company names to their ticker only when you are confident; otherwise give your best ticker and confidence \"low\".\n" +
  "- Ignore watchlists, charts and news panels; only include positions actually held.";

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST") return reply({ code: "method_not_allowed" }, 405);
  if (!Deno.env.get("ANTHROPIC_API_KEY")) return reply({ code: "not_configured" }, 503);
  const user = await requireUser(req);
  if (!user) return reply({ code: "unauthorized" }, 401);

  let images: { media_type: string; data: string }[] = [];
  try { images = (await req.json()).images ?? []; } catch { return reply({ code: "bad_request" }, 400); }
  images = images.filter((i) => MEDIA.includes(i?.media_type as Media) && typeof i.data === "string" && i.data.length < 7_000_000).slice(0, 5);
  if (!images.length) return reply({ code: "bad_request", message: "Attach at least one JPEG, PNG or WebP screenshot." }, 400);

  const limit = Number(Deno.env.get("AI_DAILY_LIMIT") ?? "20");
  const { data: allowed, error: quotaError } = await admin().rpc("tl_ai_take", { p_user: user.id, p_limit: limit });
  if (quotaError) return reply({ code: "quota_error" }, 500);
  if (allowed === false) return reply({ code: "rate_limited" }, 429);

  try {
    const msg = await client.beta.messages.create({
      model: "claude-opus-5",
      max_tokens: 16000,
      thinking: { type: "adaptive" },
      output_config: { effort: "medium", format: { type: "json_schema", schema: SCHEMA } },
      betas: ["server-side-fallback-2026-07-01"],
      fallbacks: "default",
      messages: [{
        role: "user",
        content: [
          ...images.map((i) => ({ type: "image" as const, source: { type: "base64" as const, media_type: i.media_type as Media, data: i.data } })),
          { type: "text" as const, text: INSTRUCTIONS },
        ],
      }],
    });
    if (msg.stop_reason === "refusal") return reply({ code: "refused" }, 422);
    if (msg.stop_reason === "max_tokens") return reply({ code: "truncated" }, 502);
    const text = msg.content.flatMap((b) => (b.type === "text" ? [b.text] : [])).join("");
    return reply({ portfolio: JSON.parse(text) });
  } catch (e) {
    if (e instanceof SyntaxError) return reply({ code: "invalid_json" }, 502);
    if (e instanceof Anthropic.RateLimitError) return reply({ code: "rate_limited" }, 429);
    if (e instanceof Anthropic.AuthenticationError) return reply({ code: "not_configured" }, 503);
    if (e instanceof Anthropic.BadRequestError) { console.error(e.message); return reply({ code: "image_rejected" }, 400); }
    if (e instanceof Anthropic.APIError) { console.error("Anthropic API error", e.status, e.message); return reply({ code: "upstream_error" }, 502); }
    console.error(e);
    return reply({ code: "upstream_error" }, 502);
  }
});
