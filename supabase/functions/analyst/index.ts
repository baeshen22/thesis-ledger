// The "Ask my analyst" feature. The app sends the investor's own ledger data
// and question; this function calls Claude with the server's API key.
import Anthropic from "npm:@anthropic-ai/sdk";
import { admin, cors, reply, requireUser } from "../_shared/http.ts";

const SYSTEM =
  "You are the analyst inside Thesis Ledger, a personal investment decision journal. " +
  "Work only from the investor's own recorded positions, theses, rules and data supplied in the message. " +
  "Follow the output instructions in the message. You are not a licensed financial adviser and never claim certainty about future prices.";

const client = new Anthropic(); // reads ANTHROPIC_API_KEY from the function's secrets

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST") return reply({ code: "method_not_allowed" }, 405);
  if (!Deno.env.get("ANTHROPIC_API_KEY")) return reply({ code: "not_configured" }, 503);

  const user = await requireUser(req);
  if (!user) return reply({ code: "unauthorized" }, 401);

  let body: { prompt?: unknown; json?: unknown };
  try { body = await req.json(); } catch { return reply({ code: "bad_request" }, 400); }
  const prompt = typeof body.prompt === "string" ? body.prompt : "";
  if (!prompt.trim()) return reply({ code: "bad_request" }, 400);
  if (prompt.length > 60000) return reply({ code: "prompt_too_large" }, 413);

  // Per-user daily cap, so a public deployment cannot run up the owner's bill.
  const limit = Number(Deno.env.get("AI_DAILY_LIMIT") ?? "20");
  const { data: allowed, error: quotaError } = await admin().rpc("tl_ai_take", { p_user: user.id, p_limit: limit });
  if (quotaError) return reply({ code: "quota_error" }, 500);
  if (allowed === false) return reply({ code: "rate_limited" }, 429);

  try {
    const msg = await client.beta.messages.create({
      model: "claude-opus-5",
      max_tokens: 8000,
      thinking: { type: "adaptive" },
      output_config: { effort: "medium" },
      // On a policy decline the API retries on a fallback model inside the same call.
      betas: ["server-side-fallback-2026-07-01"],
      fallbacks: "default",
      system: SYSTEM + (body.json ? " Reply with only the JSON value requested: no prose, no code fences." : ""),
      messages: [{ role: "user", content: prompt }],
    });
    if (msg.stop_reason === "refusal") return reply({ code: "refused" }, 422);
    const text = msg.content.flatMap((b) => (b.type === "text" ? [b.text] : [])).join("\n").trim();
    if (!text) return reply({ code: "empty_completion" }, 502);
    return reply({ text, truncated: msg.stop_reason === "max_tokens" });
  } catch (e) {
    if (e instanceof Anthropic.RateLimitError) return reply({ code: "rate_limited" }, 429);
    if (e instanceof Anthropic.AuthenticationError) return reply({ code: "not_configured" }, 503);
    if (e instanceof Anthropic.APIError) { console.error("Anthropic API error", e.status, e.message); return reply({ code: "upstream_error" }, 502); }
    console.error(e);
    return reply({ code: "upstream_error" }, 502);
  }
});
