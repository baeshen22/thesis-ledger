// Finnhub market data (free tier: 60 calls/minute, US listings).
import { admin } from "./http.ts";

const KEY = () => Deno.env.get("FINNHUB_API_KEY") ?? "";
const fh = async (path: string) => {
  const r = await fetch(`https://finnhub.io/api/v1${path}${path.includes("?") ? "&" : "?"}token=${KEY()}`);
  if (!r.ok) throw new Error(`finnhub ${r.status}`);
  return r.json();
};
export const hasMarketKey = () => !!KEY();
export const validSymbol = (s: unknown): s is string => typeof s === "string" && /^[A-Z0-9.\-]{1,12}$/.test(s);

/** Refreshes quotes older than `maxAgeSec`, at most `cap` API calls. Returns all known quotes for the symbols. */
export async function refreshQuotes(symbols: string[], maxAgeSec: number, cap: number) {
  const db = admin();
  const { data: known } = await db.from("quotes").select("*").in("symbol", symbols);
  const bySym = new Map((known ?? []).map((q) => [q.symbol, q]));
  const now = Date.now();
  const stale = symbols
    .filter((s) => { const q = bySym.get(s); return !q || now - Date.parse(q.updated_at) > maxAgeSec * 1000; })
    .sort((a, b) => (bySym.get(a) ? Date.parse(bySym.get(a)!.updated_at) : 0) - (bySym.get(b) ? Date.parse(bySym.get(b)!.updated_at) : 0))
    .slice(0, cap);
  const rows: Record<string, unknown>[] = [];
  await Promise.all(stale.map(async (s) => {
    try {
      const q = await fh(`/quote?symbol=${encodeURIComponent(s)}`);
      if (typeof q.c === "number" && q.c > 0) {
        rows.push({ symbol: s, price: q.c, prev_close: q.pc > 0 ? q.pc : null, change_pct: typeof q.dp === "number" ? q.dp : null, source: "finnhub", updated_at: new Date().toISOString() });
      }
    } catch { /* leave this symbol for the next run */ }
  }));
  if (rows.length) { await db.from("quotes").upsert(rows); rows.forEach((r) => bySym.set(r.symbol as string, r)); }
  return Object.fromEntries(symbols.filter((s) => bySym.has(s)).map((s) => [s, bySym.get(s)]));
}

/** Company profile, key metrics, next earnings, recent news and analyst rating trend for one symbol. */
export async function fetchMarketData(s: string) {
  const day = (d: Date) => d.toISOString().slice(0, 10);
  const today = new Date(), in120 = new Date(Date.now() + 120 * 864e5), ago7 = new Date(Date.now() - 7 * 864e5);
  const [profile, metric, earnings, news, recs] = await Promise.all([
    fh(`/stock/profile2?symbol=${s}`).catch(() => ({})),
    fh(`/stock/metric?symbol=${s}&metric=all`).catch(() => ({})),
    fh(`/calendar/earnings?symbol=${s}&from=${day(today)}&to=${day(in120)}`).catch(() => ({})),
    fh(`/company-news?symbol=${s}&from=${day(ago7)}&to=${day(today)}`).catch(() => []),
    fh(`/stock/recommendation?symbol=${s}`).catch(() => []),
  ]);
  const m = metric?.metric ?? {};
  const next = (earnings?.earningsCalendar ?? []).map((e: { date: string }) => e.date).filter(Boolean).sort()[0] ?? null;
  const bull = (r: Record<string, number>) => { const t = (r.strongBuy ?? 0) + (r.buy ?? 0) + (r.hold ?? 0) + (r.sell ?? 0) + (r.strongSell ?? 0); return t ? ((r.strongBuy ?? 0) + (r.buy ?? 0)) / t : null; };
  const recNow = Array.isArray(recs) ? recs[0] : null, rec3m = Array.isArray(recs) ? recs[3] : null;
  const bullNow = recNow ? bull(recNow) : null, bullThen = rec3m ? bull(rec3m) : null;
  return {
    company: profile?.name ?? null,
    exchange: profile?.exchange ?? null,
    currency: profile?.currency ?? null,
    industry: profile?.finnhubIndustry ?? null,
    shares: typeof profile?.shareOutstanding === "number" ? profile.shareOutstanding : null, // millions
    revGrowth: m.revenueGrowthTTMYoy ?? null,
    revGrowthQ: m.revenueGrowthQuarterlyYoy ?? null,
    grossMargin: m.grossMarginTTM ?? null,
    peTTM: m.peTTM ?? null,
    high52: m["52WeekHigh"] ?? null,
    low52: m["52WeekLow"] ?? null,
    nextEarnings: next,
    analystBuyPct: bullNow != null ? Math.round(bullNow * 100) : null,
    analystBuyPct3m: bullThen != null ? Math.round(bullThen * 100) : null,
    downgrades: bullNow != null && bullThen != null ? bullThen - bullNow >= 0.15 : null,
    news: (Array.isArray(news) ? news : []).slice(0, 4).map((n: { datetime: number; headline: string; url: string; source: string }) => ({
      date: new Date(n.datetime * 1000).toISOString().slice(0, 10), headline: n.headline, url: n.url, source: n.source,
    })),
    asOf: new Date().toISOString(),
  };
}

export async function refreshMarketData(symbols: string[], maxAgeHours: number, cap: number) {
  const db = admin();
  const { data: known } = await db.from("market_data").select("symbol, updated_at").in("symbol", symbols);
  const age = new Map((known ?? []).map((k) => [k.symbol, Date.parse(k.updated_at)]));
  const due = symbols.filter((s) => !age.has(s) || Date.now() - age.get(s)! > maxAgeHours * 3600e3)
    .sort((a, b) => (age.get(a) ?? 0) - (age.get(b) ?? 0)).slice(0, cap);
  const rows = [];
  for (const s of due) { try { rows.push({ symbol: s, data: await fetchMarketData(s), updated_at: new Date().toISOString() }); } catch { /* next run */ } }
  if (rows.length) await db.from("market_data").upsert(rows);
  return rows.length;
}
