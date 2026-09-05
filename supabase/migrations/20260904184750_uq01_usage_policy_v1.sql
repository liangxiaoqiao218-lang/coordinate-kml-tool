begin;

alter table public.system_config
  add column if not exists free_trial_daily_max integer not null default 3 check (free_trial_daily_max >= 0),
  add column if not exists free_trial_lifetime_max integer not null default 12 check (free_trial_lifetime_max >= 0);

create table if not exists public.usage_free_trial_state (
  usage_identity text primary key,
  free_daily_day date not null,
  free_daily_used integer not null default 0 check (free_daily_used >= 0),
  free_lifetime_used integer not null default 0 check (free_lifetime_used >= 0),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.usage_charge_events (
  event_id uuid primary key default gen_random_uuid(),
  usage_identity text not null,
  service_operation_id uuid not null,
  usage_event_type text not null check (usage_event_type in (
    'SUCCESSFUL_NEW_COORDINATE_RESULT',
    'SUCCESSFUL_NEW_JUDGE_RESULT'
  )),
  charge_source text not null check (charge_source in ('free', 'paid_convert', 'paid_judge')),
  free_day date not null,
  result_snapshot jsonb not null,
  created_at timestamptz not null default now(),
  unique (usage_identity, service_operation_id, usage_event_type)
);

alter table public.usage_free_trial_state enable row level security;
alter table public.usage_charge_events enable row level security;

revoke all on table public.usage_free_trial_state from public, anon, authenticated;
revoke all on table public.usage_charge_events from public, anon, authenticated;
grant select, insert, update on table public.usage_free_trial_state to service_role;
grant select, insert on table public.usage_charge_events to service_role;

create or replace function public.uq01_get_usage_quota(
  p_usage_identity text,
  p_free_day date
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_state public.usage_free_trial_state%rowtype;
  v_paid_convert integer := 0;
  v_paid_judge integer := 0;
  v_daily_used integer := 0;
  v_daily_max integer := 3;
  v_lifetime_max integer := 12;
begin
  if nullif(btrim(p_usage_identity), '') is null or p_free_day is null then
    raise exception 'invalid usage quota input';
  end if;

  select coalesce(max(free_trial_daily_max), 3), coalesce(max(free_trial_lifetime_max), 12)
    into v_daily_max, v_lifetime_max
  from public.system_config where id = 'pricing';

  select * into v_state
  from public.usage_free_trial_state
  where usage_identity = p_usage_identity;

  select coalesce(paid_convert_count, 0), coalesce(paid_judge_count, 0)
    into v_paid_convert, v_paid_judge
  from public.users
  where user_id = p_usage_identity;

  if v_state.usage_identity is not null and v_state.free_daily_day = p_free_day then
    v_daily_used := v_state.free_daily_used;
  end if;

  return jsonb_build_object(
    'success', true,
    'free_daily_used', v_daily_used,
    'free_lifetime_used', coalesce(v_state.free_lifetime_used, 0),
    'free_trial_daily_max', v_daily_max,
    'free_trial_lifetime_max', v_lifetime_max,
    'free_daily_remaining', greatest(0, v_daily_max - v_daily_used),
    'free_lifetime_remaining', greatest(0, v_lifetime_max - coalesce(v_state.free_lifetime_used, 0)),
    'paid_convert_count', v_paid_convert,
    'paid_judge_count', v_paid_judge
  );
end;
$$;

create or replace function public.uq01_consume_usage_event(
  p_usage_identity text,
  p_service_operation_id uuid,
  p_usage_event_type text,
  p_free_day date
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_existing public.usage_charge_events%rowtype;
  v_state public.usage_free_trial_state%rowtype;
  v_paid_convert integer := 0;
  v_paid_judge integer := 0;
  v_source text;
  v_result jsonb;
  v_event_id uuid := gen_random_uuid();
  v_daily_max integer := 3;
  v_lifetime_max integer := 12;
begin
  if nullif(btrim(p_usage_identity), '') is null
     or p_service_operation_id is null
     or p_free_day is null
     or p_usage_event_type not in ('SUCCESSFUL_NEW_COORDINATE_RESULT', 'SUCCESSFUL_NEW_JUDGE_RESULT') then
    raise exception 'invalid usage event input';
  end if;

  select coalesce(max(free_trial_daily_max), 3), coalesce(max(free_trial_lifetime_max), 12)
    into v_daily_max, v_lifetime_max
  from public.system_config where id = 'pricing';

  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    p_usage_identity || ':' || p_service_operation_id::text || ':' || p_usage_event_type, 0
  ));

  select * into v_existing
  from public.usage_charge_events
  where usage_identity = p_usage_identity
    and service_operation_id = p_service_operation_id
    and usage_event_type = p_usage_event_type;
  if found then
    return v_existing.result_snapshot || jsonb_build_object('idempotent', true);
  end if;

  insert into public.usage_free_trial_state (
    usage_identity, free_daily_day, free_daily_used, free_lifetime_used
  ) values (p_usage_identity, p_free_day, 0, 0)
  on conflict (usage_identity) do nothing;

  select * into v_state
  from public.usage_free_trial_state
  where usage_identity = p_usage_identity
  for update;

  if v_state.free_daily_day <> p_free_day then
    v_state.free_daily_day := p_free_day;
    v_state.free_daily_used := 0;
  end if;

  select coalesce(paid_convert_count, 0), coalesce(paid_judge_count, 0)
    into v_paid_convert, v_paid_judge
  from public.users
  where user_id = p_usage_identity
  for update;

  if v_state.free_daily_used < v_daily_max and v_state.free_lifetime_used < v_lifetime_max then
    v_source := 'free';
    v_state.free_daily_used := v_state.free_daily_used + 1;
    v_state.free_lifetime_used := v_state.free_lifetime_used + 1;
  elsif p_usage_event_type = 'SUCCESSFUL_NEW_COORDINATE_RESULT' and v_paid_convert > 0 then
    v_source := 'paid_convert';
    v_paid_convert := v_paid_convert - 1;
    update public.users set paid_convert_count = v_paid_convert, updated_at = now()
    where user_id = p_usage_identity;
  elsif p_usage_event_type = 'SUCCESSFUL_NEW_JUDGE_RESULT' and v_paid_judge > 0 then
    v_source := 'paid_judge';
    v_paid_judge := v_paid_judge - 1;
    update public.users set paid_judge_count = v_paid_judge, updated_at = now()
    where user_id = p_usage_identity;
  else
    return jsonb_build_object(
      'success', false,
      'reason', 'limit_exceeded',
      'idempotent', false,
      'free_daily_used', v_state.free_daily_used,
      'free_lifetime_used', v_state.free_lifetime_used,
      'free_trial_daily_max', v_daily_max,
      'free_trial_lifetime_max', v_lifetime_max,
      'free_daily_remaining', greatest(0, v_daily_max - v_state.free_daily_used),
      'free_lifetime_remaining', greatest(0, v_lifetime_max - v_state.free_lifetime_used),
      'paid_convert_count', v_paid_convert,
      'paid_judge_count', v_paid_judge
    );
  end if;

  update public.usage_free_trial_state set
    free_daily_day = v_state.free_daily_day,
    free_daily_used = v_state.free_daily_used,
    free_lifetime_used = v_state.free_lifetime_used,
    updated_at = now()
  where usage_identity = p_usage_identity;

  v_result := jsonb_build_object(
    'success', true,
    'reason', 'ok',
    'idempotent', false,
    'event_id', v_event_id,
    'charge_source', v_source,
    'free_daily_used', v_state.free_daily_used,
    'free_lifetime_used', v_state.free_lifetime_used,
    'free_trial_daily_max', v_daily_max,
    'free_trial_lifetime_max', v_lifetime_max,
    'free_daily_remaining', greatest(0, v_daily_max - v_state.free_daily_used),
    'free_lifetime_remaining', greatest(0, v_lifetime_max - v_state.free_lifetime_used),
    'paid_convert_count', v_paid_convert,
    'paid_judge_count', v_paid_judge
  );

  insert into public.usage_charge_events (
    event_id, usage_identity, service_operation_id, usage_event_type,
    charge_source, free_day, result_snapshot
  ) values (
    v_event_id, p_usage_identity, p_service_operation_id, p_usage_event_type,
    v_source, p_free_day, v_result
  );

  return v_result;
end;
$$;

revoke execute on function public.uq01_get_usage_quota(text, date) from public, anon, authenticated;
revoke execute on function public.uq01_consume_usage_event(text, uuid, text, date) from public, anon, authenticated;
grant execute on function public.uq01_get_usage_quota(text, date) to service_role;
grant execute on function public.uq01_consume_usage_event(text, uuid, text, date) to service_role;

commit;
