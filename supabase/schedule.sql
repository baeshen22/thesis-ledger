-- Keeps prices fresh even when nobody has the app open (needed for future push alerts).
-- 1) Database → Extensions: enable pg_cron and pg_net.
-- 2) Replace the two placeholders below, then run this in the SQL Editor.
--    YOUR-PROJECT-REF: from your Project URL.  YOUR-CRON-SECRET: the same value you set with
--    `supabase secrets set CRON_SECRET=...`.

select vault.create_secret('https://YOUR-PROJECT-REF.supabase.co', 'tl_project_url');
select vault.create_secret('YOUR-CRON-SECRET', 'tl_cron_secret');

create or replace function public.tl_call_refresh(job text) returns void
language sql security definer set search_path = public as $$
  select net.http_post(
    url     := (select decrypted_secret from vault.decrypted_secrets where name = 'tl_project_url') || '/functions/v1/refresh-market',
    headers := jsonb_build_object('Content-Type', 'application/json',
                                  'x-cron-secret', (select decrypted_secret from vault.decrypted_secrets where name = 'tl_cron_secret')),
    body    := jsonb_build_object('job', job));
$$;

-- Prices every 5 minutes while US markets can be open (13:00–21:59 UTC, Mon–Fri).
select cron.schedule('tl-quotes',       '*/5 13-21 * * 1-5', $$ select public.tl_call_refresh('quotes') $$);
-- Company data (growth, margins, earnings dates, news, analyst ratings): a batch every 30 minutes.
select cron.schedule('tl-fundamentals', '7,37 * * * *',       $$ select public.tl_call_refresh('fundamentals') $$);
