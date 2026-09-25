# Thesis Ledger

An installable web app (PWA) that remembers why you bought each position, what would make you sell, and whether that reason is still true.

- Anyone can sign up with just an email code. Each person's portfolio is private to their account (Postgres row-level security).
- Installs to the home screen on iPhone and Android, and on desktop Chrome and Edge. Works offline, and syncs between devices when back online.
- "Try without an account" keeps data on the device only.

## What you need (all have free tiers)

| Service | Used for | Required? |
|---|---|---|
| GitHub (Pages) | Hosting the app at a public URL | Yes (or Netlify/Vercel) |
| Supabase | Sign-in, database, server functions | Yes, for accounts and sync |
| Anthropic API key | Screenshot import, "Ask my analyst", thesis drafting | Optional (paid per use) |
| Finnhub API key (finnhub.io, free) | Live prices every few minutes, company data, earnings dates, news, analyst ratings | Recommended |

## 1. Create the backend (Supabase), about 10 minutes

1. Create a project at https://supabase.com.
2. **SQL Editor → New query**: paste `supabase/schema.sql` and run it.
3. **Authentication → Emails → Magic Link** template: make sure the body contains `{{ .Token }}` so users get a 6-digit code. For example:
   `Your Thesis Ledger code is {{ .Token }}` (you can keep the link too). Codes work inside the installed iPhone app, where links would open Safari instead.
4. **Authentication → URL Configuration**: set *Site URL* to your app URL (from step 2 below, e.g. `https://YOURNAME.github.io/thesis-ledger/`) and add it under *Redirect URLs*.
5. **Project Settings → API**: copy the *Project URL* and the *anon / publishable* key into `config.js`. The anon key is meant to be public. Row-level security protects the data.
6. Supabase's built-in email sender is rate limited (a few emails per hour). Before inviting the public, add your own SMTP under **Authentication → Emails → SMTP** (for example Resend or Postmark).

### Server functions (analyst, prices, account deletion)

Install the Supabase CLI (https://supabase.com/docs/guides/cli), then from this folder:

```bash
supabase login
supabase link --project-ref YOUR_PROJECT_REF
supabase secrets set ANTHROPIC_API_KEY=sk-ant-...     # screenshot import + analyst
supabase secrets set FINNHUB_API_KEY=...              # live prices + company data
supabase secrets set AI_DAILY_LIMIT=20                # AI requests per user per day
supabase secrets set CRON_SECRET=$(openssl rand -hex 24)   # protects the scheduled job
supabase secrets set ALLOWED_ORIGIN=https://YOURNAME.github.io   # lock CORS to your site
supabase functions deploy analyst
supabase functions deploy read-portfolio
supabase functions deploy quotes
supabase functions deploy delete-account
supabase functions deploy refresh-market --no-verify-jwt
```

### Automatic updates (recommended)

The app already refreshes prices every 5 minutes while it is open (`refreshMinutes` in `config.js`). Every device that holds the same ticker gets the new price pushed instantly.

To keep prices and company data fresh when nobody has the app open (and as the basis for future push alerts):

1. **Database → Extensions**: enable `pg_cron` and `pg_net`.
2. Edit `supabase/schedule.sql`: put in your project ref and the same `CRON_SECRET`. Then run it in the SQL Editor.

This schedules two jobs:
- Prices every 5 minutes during US market hours.
- Company data every 30 minutes, working through the stalest tickers first. Each ticker is refreshed about once a day.

The Finnhub free tier allows 60 calls a minute. The jobs stay under that limit, so a few hundred tickers across all users works. Beyond that, move to a paid plan.

### What updates automatically

| Data | Source | How often |
|---|---|---|
| Price, previous close, daily change | Finnhub quotes | Every 5 min (app open), every 5 min in market hours (scheduler) |
| Company name, industry, shares outstanding | Finnhub profile | Daily |
| Revenue growth, gross margin, P/E, 52-week range | Finnhub basic financials | Daily |
| Next earnings date (becomes the "next catalyst" when yours has passed) | Finnhub earnings calendar | Daily |
| News headlines | Finnhub company news | Daily |
| Analyst buy ratio and "material downgrades" flag | Finnhub recommendation trends | Daily |
| Holdings, shares, average cost, cash | Your broker screenshot, read by Claude | Whenever you import |

Anything you type yourself in "Update current reality" stays in place until newer automatic data arrives. Revenue and EPS *estimates* and consensus price targets are paid data on Finnhub, so those remain manual.

Every change re-runs the whole engine: statuses, sell reviews, buy checks, the allocator and the exit map.

### Screenshot import

Tap **Import screenshot**, then add up to 5 screenshots of your broker's holdings screen, pasted or dropped. Claude reads each holding's shares, average cost and price. The app then shows the differences against your ledger:
- new positions
- bought more
- sold some
- positions not in the screenshot

Nothing is written until you tap **Apply**. Share changes become transactions priced so your average cost matches the broker. Low-confidence tickers are left unticked for you to check.

### About the iPhone Stocks app

Apple's Stocks app gets its prices from commercial market-data vendors that Apple licenses. Exchanges sell real-time data, and quotes can be delayed depending on the exchange. Apple provides no API or widget feed that another app can read, and iOS keeps apps sandboxed from each other, so a web app cannot "link" to Stocks. Thesis Ledger does the same thing Apple does, on a smaller scale: it pulls from a market-data provider (Finnhub). To change provider (for example Twelve Data or Polygon for broader or Saudi coverage), only `supabase/functions/_shared/market.ts` needs editing.

If you skip a key, that feature shows "not set up" in the app and everything else still works. To hide the buttons entirely, set `ai: false` or `quotes: false` in `config.js`.

**Cost note:** the analyst runs on *your* Anthropic key for every user. `AI_DAILY_LIMIT` caps questions per user per day. Also set a monthly spend limit in the Anthropic Console.

## 2. Publish the app (GitHub Pages)

1. Create a public repository, e.g. `thesis-ledger`, and upload the contents of this folder (including `.nojekyll`).
2. Go to **Settings → Pages → Build and deployment**, choose *Deploy from a branch*, then pick `main` and `/ (root)`.
3. After a minute the app is live at `https://YOURNAME.github.io/thesis-ledger/`. Share that link.

Any static host works the same way. For Netlify, drag the folder onto app.netlify.com/drop. HTTPS is required for install and offline support.

## 3. Install it

- **iPhone:** open the link in Safari, tap Share, then Add to Home Screen.
- **Android / desktop Chrome or Edge:** tap **Install app** in the top bar, or use the browser menu's Install option.

## Updating the app

Change the files, bump `VERSION` in `sw.js` (e.g. `tl-v3`), and push. Open apps show a "new version is ready — Reload" banner.

## Files

```
index.html, styles.css, app.js   the app (no build step)
config.js                        your Supabase URL/key and feature switches
sw.js, manifest.webmanifest      offline support and install metadata
icons/                           app icons
vendor/supabase.js               Supabase client 2.117.2 (bundled for offline use)
examples.json                    the optional example portfolio
supabase/schema.sql              tables, security policies, quotas
supabase/functions/              analyst + read-portfolio (Claude), quotes + refresh-market (Finnhub), delete-account
supabase/schedule.sql            optional scheduler for background refresh
```

## Not included yet

- **Push and email alerts.** These need a scheduled server job that refreshes prices daily and sends only material changes. The settings toggles are stored, ready for it.
- **Saudi (Tadawul) live prices.** Finnhub's free tier covers US listings. Tadawul tickers such as `2222.SR` import fine from screenshots, but their prices come from screenshots or the manual update, unless you switch to a provider that covers Tadawul.

Thesis Ledger is a decision journal, not investment advice.
