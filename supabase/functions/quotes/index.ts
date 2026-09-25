// Called by the app every few minutes while it is open. Serves quotes from the shared
// cache and refreshes any older than ~4 minutes. New symbols also get company data.
import { cors, reply, requireUser } from "../_shared/http.ts";
import { hasMarketKey, refreshMarketData, refreshQuotes, validSymbol } from "../_shared/market.ts";

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (!hasMarketKey()) return reply({ code: "not_configured" }, 503);
  if (!(await requireUser(req))) return reply({ code: "unauthorized" }, 401);
  let symbols: string[] = [];
  try { symbols = (await req.json()).symbols ?? []; } catch { return reply({ code: "bad_request" }, 400); }
  symbols = [...new Set(symbols.filter(validSymbol))].slice(0, 60);
  const quotes = await refreshQuotes(symbols, 240, 30);
  await refreshMarketData(symbols, 24, 3); // fills in brand-new tickers quickly
  return reply({ quotes, asOf: new Date().toISOString() });
});
