// Scheduled job (see supabase/schedule.sql). Deploy with --no-verify-jwt; it is protected by CRON_SECRET.
import { admin, reply } from "../_shared/http.ts";
import { hasMarketKey, refreshMarketData, refreshQuotes } from "../_shared/market.ts";

Deno.serve(async (req) => {
  const secret = Deno.env.get("CRON_SECRET");
  if (!secret || req.headers.get("x-cron-secret") !== secret) return reply({ code: "forbidden" }, 403);
  if (!hasMarketKey()) return reply({ code: "not_configured" }, 503);
  const { job } = await req.json().catch(() => ({ job: "quotes" }));
  const { data, error } = await admin().rpc("tl_tracked_symbols");
  if (error) return reply({ code: "db_error", message: error.message }, 500);
  const symbols = (data ?? []) as string[];
  if (job === "fundamentals") return reply({ refreshed: await refreshMarketData(symbols, 20, 10) });
  const q = await refreshQuotes(symbols, 200, 55); // stays under Finnhub's 60 calls/minute
  return reply({ tracked: symbols.length, quotes: Object.keys(q).length });
});
