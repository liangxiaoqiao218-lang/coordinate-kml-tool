-- GeoKit Lab Browser E2E dedicated test Supabase schema.
--
-- Purpose:
--   Create the minimum database surface required by the current server.js
--   quota/read/consume paths for local browser E2E testing.
--
-- Safety:
--   - Dedicated test Supabase only.
--   - Contains no real users or live business data.
--   - Creates only the three E2E quota/config tables below.
--   - Enables RLS and does not add public bypass policies.

create table if not exists public.users (
  user_id text primary key,
  is_vip boolean not null default false,
  free_convert_count integer not null default 20 check (free_convert_count >= 0),
  free_judge_count integer not null default 20 check (free_judge_count >= 0),
  paid_convert_count integer not null default 0 check (paid_convert_count >= 0),
  paid_judge_count integer not null default 0 check (paid_judge_count >= 0),
  last_free_reset_date date not null default current_date,
  last_ip text not null default '',
  region text not null default '',
  user_agent text not null default '',
  device_info text not null default '',
  source_from text,
  source_page text,
  landing_url text,
  referrer text,
  admin_note text,
  last_seen_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists users_updated_at_idx
  on public.users (updated_at desc);

create index if not exists users_last_seen_at_idx
  on public.users (last_seen_at desc);

create table if not exists public.usage_logs (
  id bigserial primary key,
  user_id text not null references public.users (user_id) on delete cascade,
  ip text,
  region text,
  user_agent text,
  device_info text,
  feature_type text not null default 'convert'
    check (feature_type in ('convert', 'judge', 'visit', 'gold')),
  consume_type text not null default 'none'
    check (consume_type in ('free', 'paid', 'none')),
  before_balance jsonb,
  after_balance jsonb,
  success boolean not null default false,
  note text,
  error_reason text,
  created_at timestamptz not null default now()
);

create index if not exists usage_logs_user_id_idx
  on public.usage_logs (user_id);

create index if not exists usage_logs_created_at_idx
  on public.usage_logs (created_at desc);

create index if not exists usage_logs_user_created_at_idx
  on public.usage_logs (user_id, created_at desc);

create index if not exists usage_logs_daily_free_quota_idx
  on public.usage_logs (user_id, success, consume_type, created_at desc);

create index if not exists usage_logs_feature_created_at_idx
  on public.usage_logs (feature_type, created_at desc);

create table if not exists public.system_config (
  id text primary key,
  monthly_price integer not null default 99 check (monthly_price >= 0),
  monthly_judge_count integer not null default 50 check (monthly_judge_count >= 0),
  monthly_convert_count integer not null default 50 check (monthly_convert_count >= 0),
  add_price integer not null default 19 check (add_price >= 0),
  add_count integer not null default 10 check (add_count >= 0),
  free_judge_count integer not null default 20 check (free_judge_count >= 0),
  free_convert_count integer not null default 20 check (free_convert_count >= 0),
  updated_at timestamptz not null default now()
);

insert into public.system_config (
  id,
  monthly_price,
  monthly_judge_count,
  monthly_convert_count,
  add_price,
  add_count,
  free_judge_count,
  free_convert_count,
  updated_at
) values (
  'pricing',
  99,
  50,
  50,
  19,
  10,
  20,
  20,
  now()
)
on conflict (id) do update set
  monthly_price = excluded.monthly_price,
  monthly_judge_count = excluded.monthly_judge_count,
  monthly_convert_count = excluded.monthly_convert_count,
  add_price = excluded.add_price,
  add_count = excluded.add_count,
  free_judge_count = excluded.free_judge_count,
  free_convert_count = excluded.free_convert_count,
  updated_at = excluded.updated_at;

alter table public.users enable row level security;
alter table public.usage_logs enable row level security;
alter table public.system_config enable row level security;
