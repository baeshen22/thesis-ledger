-- Thesis Ledger database. Paste into Supabase → SQL Editor → Run.
-- Every record is one JSON document owned by one user. Row-level security
-- means a signed-in user can only ever read or write their own rows.

create table if not exists public.docs (
  user_id    uuid        not null default auth.uid() references auth.users (id) on delete cascade,
  col        text        not null check (col in ('settings','positions','watchlist','transactions','fundamentals','history','closed','alertState')),
  id         text        not null check (char_length(id) between 1 and 200),
  data       jsonb       not null check (pg_column_size(data) < 262144),
  updated_at timestamptz not null default now(),
  primary key (user_id, col, id)
);

alter table public.docs enable row level security;

drop policy if exists "read own docs"   on public.docs;
drop policy if exists "insert own docs" on public.docs;
drop policy if exists "update own docs" on public.docs;
drop policy if exists "delete own docs" on public.docs;
create policy "read own docs"   on public.docs for select to authenticated using (user_id = auth.uid());
create policy "insert own docs" on public.docs for insert to authenticated with check (user_id = auth.uid());
create policy "update own docs" on public.docs for update to authenticated using (user_id = auth.uid()) with check (user_id = auth.uid());
create policy "delete own docs" on public.docs for delete to authenticated using (user_id = auth.uid());

-- Cap each account at 5,000 records so one user cannot fill the database.
create or replace function public.tl_docs_limit() returns trigger
language plpgsql security definer set search_path = public as $$
begin
  if (select count(*) from public.docs where user_id = new.user_id) >= 5000 then
    raise exception 'Record limit reached (5,000). Delete old history or closed positions.';
  end if;
  return new;
end $$;
drop trigger if exists tl_docs_limit on public.docs;
create trigger tl_docs_limit before insert on public.docs for each row execute function public.tl_docs_limit();

-- Live sync between a user's devices.
do $$ begin
  alter publication supabase_realtime add table public.docs;
exception when duplicate_object then null; end $$;

-- Daily question quota for the AI analyst (server-side only; no policies = no client access).
create table if not exists public.ai_usage (
  user_id uuid not null references auth.users (id) on delete cascade,
  day     date not null,
  count   int  not null default 0,
  primary key (user_id, day)
);
alter table public.ai_usage enable row level security;

create or replace function public.tl_ai_take(p_user uuid, p_limit int) returns boolean
language plpgsql security definer set search_path = public as $$
declare c int;
begin
  insert into public.ai_usage (user_id, day, count) values (p_user, current_date, 1)
  on conflict (user_id, day) do update set count = public.ai_usage.count + 1
  returning count into c;
  return c <= p_limit;
end $$;
revoke all on function public.tl_ai_take(uuid, int) from public, anon, authenticated;
grant execute on function public.tl_ai_take(uuid, int) to service_role;

-- ---------------------------------------------------------------------------
-- Market data shared by all users (prices are public facts, not user data).
-- Readable by signed-in users, written only by the server functions.
-- ---------------------------------------------------------------------------
create table if not exists public.quotes (
  symbol     text primary key,
  price      numeric not null,
  prev_close numeric,
  change_pct numeric,
  source     text not null default 'finnhub',
  updated_at timestamptz not null default now()
);
alter table public.quotes enable row level security;
drop policy if exists "read quotes" on public.quotes;
create policy "read quotes" on public.quotes for select to authenticated using (true);
do $$ begin alter publication supabase_realtime add table public.quotes; exception when duplicate_object then null; end $$;

create table if not exists public.market_data (
  symbol     text primary key,
  data       jsonb not null,
  updated_at timestamptz not null default now()
);
alter table public.market_data enable row level security;
drop policy if exists "read market data" on public.market_data;
create policy "read market data" on public.market_data for select to authenticated using (true);
do $$ begin alter publication supabase_realtime add table public.market_data; exception when duplicate_object then null; end $$;

-- Every ticker any user holds or watches, for the scheduled refresher.
create or replace function public.tl_tracked_symbols() returns setof text
language sql security definer set search_path = public stable as $$
  select distinct id from public.docs where col in ('positions','watchlist') and id ~ '^[A-Z0-9.\-]{1,12}$'
$$;
revoke all on function public.tl_tracked_symbols() from public, anon, authenticated;
grant execute on function public.tl_tracked_symbols() to service_role;
