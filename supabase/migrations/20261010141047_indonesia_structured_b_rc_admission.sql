begin;

create schema if not exists private;

create table if not exists private.indonesia_structured_b_batches (
  batch_id text primary key,
  project_ref text not null,
  source_ledger_sha256 text not null check (source_ledger_sha256 ~ '^[0-9a-f]{64}$'),
  authorized_image_sha256 text not null check (authorized_image_sha256 ~ '^[0-9a-f]{64}$'),
  max_attempts integer not null check (max_attempts > 0),
  total_cost_limit_cny numeric(12, 6) not null check (total_cost_limit_cny > 0),
  reserve_per_attempt_cny numeric(12, 6) not null check (reserve_per_attempt_cny > 0),
  dispatched_attempts integer not null check (dispatched_attempts >= 0 and dispatched_attempts <= max_attempts),
  reserved_cost_cny numeric(12, 6) not null check (reserved_cost_cny >= 0 and reserved_cost_cny <= total_cost_limit_cny),
  estimated_cost_cny numeric(12, 10) not null check (estimated_cost_cny >= 0),
  max_concurrency integer not null check (max_concurrency > 0),
  active_dispatches integer not null default 0 check (active_dispatches >= 0 and active_dispatches <= max_concurrency),
  status text not null default 'OPEN' check (status in ('OPEN', 'CLOSED')),
  created_at timestamptz not null default statement_timestamp(),
  updated_at timestamptz not null default statement_timestamp()
);

create table if not exists private.indonesia_structured_b_single_runs (
  run_id text primary key,
  batch_id text not null references private.indonesia_structured_b_batches(batch_id),
  project_ref text not null,
  authorized_image_sha256 text not null check (authorized_image_sha256 ~ '^[0-9a-f]{64}$'),
  max_claims integer not null check (max_claims = 1),
  max_dispatched integer not null check (max_dispatched = 1),
  claim_ttl_seconds integer not null check (claim_ttl_seconds = 600),
  run_window_seconds integer not null check (run_window_seconds = 1800),
  claim_count integer not null default 0 check (claim_count >= 0 and claim_count <= max_claims),
  dispatched_count integer not null default 0 check (dispatched_count >= 0 and dispatched_count <= max_dispatched),
  consumed boolean not null default false,
  status text not null default 'ARMED' check (status in ('ARMED', 'ACTIVE', 'COMPLETED', 'FAILED_PRE_PROVIDER', 'FAILED_AFTER_PROVIDER', 'OUTCOME_UNKNOWN', 'CLOSED')),
  activated_at timestamptz,
  deadline_at timestamptz,
  closed_at timestamptz,
  terminal_reason text,
  created_at timestamptz not null default statement_timestamp(),
  updated_at timestamptz not null default statement_timestamp(),
  check ((activated_at is null and deadline_at is null) or (activated_at is not null and deadline_at is not null and deadline_at > activated_at))
);

create table if not exists private.indonesia_structured_b_claims (
  claim_id uuid primary key default extensions.gen_random_uuid(),
  run_id text not null references private.indonesia_structured_b_single_runs(run_id),
  batch_id text not null references private.indonesia_structured_b_batches(batch_id),
  recognition_request_id uuid not null unique,
  image_sha256 text not null check (image_sha256 ~ '^[0-9a-f]{64}$'),
  token_sha256 text not null unique check (token_sha256 ~ '^[0-9a-f]{64}$'),
  status text not null check (status in ('RESERVED', 'DISPATCHED', 'COMPLETED', 'FAILED_PRE_PROVIDER', 'FAILED_AFTER_PROVIDER', 'OUTCOME_UNKNOWN')),
  claimed_at timestamptz not null,
  expires_at timestamptz not null,
  dispatched_at timestamptz,
  settled_at timestamptz,
  provider_call_count integer check (provider_call_count in (0, 1)),
  prompt_tokens integer check (prompt_tokens is null or prompt_tokens >= 0),
  completion_tokens integer check (completion_tokens is null or completion_tokens >= 0),
  total_tokens integer check (total_tokens is null or total_tokens >= 0),
  estimated_cost_cny numeric(12, 10) check (estimated_cost_cny is null or estimated_cost_cny >= 0),
  outcome_code text,
  created_at timestamptz not null default statement_timestamp(),
  updated_at timestamptz not null default statement_timestamp(),
  check (expires_at > claimed_at),
  check (total_tokens is null or (prompt_tokens is not null and completion_tokens is not null and total_tokens = prompt_tokens + completion_tokens))
);

create unique index if not exists indonesia_structured_b_one_claim_per_run_idx
  on private.indonesia_structured_b_claims(run_id);

alter table private.indonesia_structured_b_batches enable row level security;
alter table private.indonesia_structured_b_single_runs enable row level security;
alter table private.indonesia_structured_b_claims enable row level security;
revoke all on table private.indonesia_structured_b_batches from public, anon, authenticated, service_role;
revoke all on table private.indonesia_structured_b_single_runs from public, anon, authenticated, service_role;
revoke all on table private.indonesia_structured_b_claims from public, anon, authenticated, service_role;

insert into private.indonesia_structured_b_batches (
  batch_id, project_ref, source_ledger_sha256, authorized_image_sha256,
  max_attempts, total_cost_limit_cny, reserve_per_attempt_cny,
  dispatched_attempts, reserved_cost_cny, estimated_cost_cny, max_concurrency
) values (
  'indonesia-ab-20261010-v1',
  'thjojitdafxfarhxhoyo',
  '0ed002526e26a440f15ef20e82cfc1ee3c05099a3cb3b8bf5688c145d14e6aeb',
  '41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2',
  6, 5.000000, 0.816000, 2, 1.632000, 0.0192888000, 1
) on conflict (batch_id) do nothing;

insert into private.indonesia_structured_b_single_runs (
  run_id, batch_id, project_ref, authorized_image_sha256,
  max_claims, max_dispatched, claim_ttl_seconds, run_window_seconds
) values (
  'indonesia-b-rc-enablement-20261010-01',
  'indonesia-ab-20261010-v1',
  'thjojitdafxfarhxhoyo',
  '41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2',
  1, 1, 600, 1800
) on conflict (run_id) do nothing;

do $$
declare
  v_batch private.indonesia_structured_b_batches%rowtype;
  v_run private.indonesia_structured_b_single_runs%rowtype;
begin
  select * into strict v_batch
  from private.indonesia_structured_b_batches
  where batch_id = 'indonesia-ab-20261010-v1';

  if v_batch.project_ref is distinct from 'thjojitdafxfarhxhoyo'
    or v_batch.source_ledger_sha256 is distinct from '0ed002526e26a440f15ef20e82cfc1ee3c05099a3cb3b8bf5688c145d14e6aeb'
    or v_batch.authorized_image_sha256 is distinct from '41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2'
    or v_batch.max_attempts is distinct from 6
    or v_batch.total_cost_limit_cny is distinct from 5.000000
    or v_batch.reserve_per_attempt_cny is distinct from 0.816000
    or v_batch.dispatched_attempts is distinct from 2
    or v_batch.reserved_cost_cny is distinct from 1.632000
    or v_batch.estimated_cost_cny is distinct from 0.0192888000
    or v_batch.max_concurrency is distinct from 1
    or v_batch.active_dispatches is distinct from 0
    or v_batch.status is distinct from 'OPEN' then
    raise exception 'indonesia structured B frozen batch conflicts with existing data';
  end if;

  select * into strict v_run
  from private.indonesia_structured_b_single_runs
  where run_id = 'indonesia-b-rc-enablement-20261010-01';

  if v_run.batch_id is distinct from v_batch.batch_id
    or v_run.project_ref is distinct from v_batch.project_ref
    or v_run.authorized_image_sha256 is distinct from v_batch.authorized_image_sha256
    or v_run.max_claims is distinct from 1
    or v_run.max_dispatched is distinct from 1
    or v_run.claim_ttl_seconds is distinct from 600
    or v_run.run_window_seconds is distinct from 1800
    or v_run.claim_count is distinct from 0
    or v_run.dispatched_count is distinct from 0
    or v_run.consumed
    or v_run.status is distinct from 'ARMED'
    or v_run.activated_at is not null
    or v_run.deadline_at is not null then
    raise exception 'indonesia structured B frozen single run conflicts with existing data';
  end if;
end
$$;

create or replace function public.claim_indonesia_structured_b_single_run(
  p_project_ref text,
  p_run_id text,
  p_batch_id text,
  p_image_sha256 text,
  p_recognition_request_id uuid,
  p_token_sha256 text,
  p_activation_only boolean default false
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_now timestamptz;
  v_batch private.indonesia_structured_b_batches%rowtype;
  v_run private.indonesia_structured_b_single_runs%rowtype;
  v_expires_at timestamptz;
begin
  select * into v_batch from private.indonesia_structured_b_batches where batch_id = p_batch_id for update;
  select * into v_run from private.indonesia_structured_b_single_runs where run_id = p_run_id for update;
  v_now := clock_timestamp();
  if v_run.run_id is null or v_batch.batch_id is null then
    return jsonb_build_object('accepted', false, 'code', 'RC_RUN_NOT_FOUND');
  end if;
  if p_project_ref is distinct from 'thjojitdafxfarhxhoyo'
    or p_batch_id is distinct from 'indonesia-ab-20261010-v1'
    or v_batch.project_ref is distinct from p_project_ref
    or v_batch.source_ledger_sha256 is distinct from '0ed002526e26a440f15ef20e82cfc1ee3c05099a3cb3b8bf5688c145d14e6aeb'
    or v_run.project_ref is distinct from p_project_ref
    or v_run.batch_id is distinct from v_batch.batch_id
    or v_batch.authorized_image_sha256 is distinct from p_image_sha256
    or v_run.authorized_image_sha256 is distinct from p_image_sha256 then
    return jsonb_build_object('accepted', false, 'code', 'RC_IDENTITY_MISMATCH');
  end if;
  if v_run.status = 'ARMED' and v_run.activated_at is null then
    update private.indonesia_structured_b_single_runs
      set activated_at = v_now,
          deadline_at = v_now + make_interval(secs => run_window_seconds),
          status = 'ACTIVE',
          updated_at = v_now
      where run_id = p_run_id
      returning * into v_run;
  end if;
  if v_run.status <> 'ACTIVE' or v_run.deadline_at is null or v_now > v_run.deadline_at then
    return jsonb_build_object('accepted', false, 'code', 'RC_RUN_WINDOW_CLOSED');
  end if;
  if p_activation_only then
    return jsonb_build_object('accepted', true, 'action', 'ACTIVATED');
  end if;
  if p_recognition_request_id is null or p_token_sha256 !~ '^[0-9a-f]{64}$' then
    return jsonb_build_object('accepted', false, 'code', 'RC_CLAIM_INPUT_INVALID');
  end if;
  if v_batch.status <> 'OPEN'
    or v_run.claim_count <> 0
    or v_run.dispatched_count <> 0
    or v_run.consumed
    or v_batch.dispatched_attempts >= v_batch.max_attempts
    or v_batch.reserved_cost_cny + v_batch.reserve_per_attempt_cny > v_batch.total_cost_limit_cny
    or exists (select 1 from private.indonesia_structured_b_claims where run_id = p_run_id) then
    return jsonb_build_object('accepted', false, 'code', 'RC_CLAIM_ALREADY_CONSUMED_OR_BUDGET_CLOSED');
  end if;
  v_expires_at := least(v_now + make_interval(secs => v_run.claim_ttl_seconds), v_run.deadline_at);
  insert into private.indonesia_structured_b_claims (
    run_id, batch_id, recognition_request_id, image_sha256, token_sha256,
    status, claimed_at, expires_at
  ) values (
    p_run_id, p_batch_id, p_recognition_request_id, p_image_sha256, p_token_sha256,
    'RESERVED', v_now, v_expires_at
  );
  update private.indonesia_structured_b_single_runs
    set claim_count = claim_count + 1, updated_at = v_now
    where run_id = p_run_id;
  return jsonb_build_object('accepted', true, 'action', 'CLAIMED', 'expires_at', v_expires_at);
exception
  when unique_violation then
    return jsonb_build_object('accepted', false, 'code', 'RC_CLAIM_REPLAY_REJECTED');
end
$$;

create or replace function public.dispatch_indonesia_structured_b_single_run(
  p_project_ref text,
  p_run_id text,
  p_batch_id text,
  p_image_sha256 text,
  p_recognition_request_id uuid,
  p_token_sha256 text
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_now timestamptz;
  v_batch private.indonesia_structured_b_batches%rowtype;
  v_run private.indonesia_structured_b_single_runs%rowtype;
  v_claim private.indonesia_structured_b_claims%rowtype;
begin
  select * into v_batch from private.indonesia_structured_b_batches where batch_id = p_batch_id for update;
  select * into v_run from private.indonesia_structured_b_single_runs where run_id = p_run_id for update;
  select * into v_claim from private.indonesia_structured_b_claims
    where run_id = p_run_id
      and recognition_request_id = p_recognition_request_id
      and token_sha256 = p_token_sha256
    for update;
  v_now := clock_timestamp();
  if v_batch.batch_id is null or v_run.run_id is null or v_claim.claim_id is null then
    return jsonb_build_object('accepted', false, 'code', 'RC_DISPATCH_BINDING_NOT_FOUND');
  end if;
  if p_project_ref is distinct from 'thjojitdafxfarhxhoyo'
    or p_batch_id is distinct from 'indonesia-ab-20261010-v1'
    or v_batch.project_ref is distinct from p_project_ref
    or v_batch.source_ledger_sha256 is distinct from '0ed002526e26a440f15ef20e82cfc1ee3c05099a3cb3b8bf5688c145d14e6aeb'
    or v_run.project_ref is distinct from p_project_ref
    or v_run.batch_id is distinct from p_batch_id
    or v_claim.batch_id is distinct from p_batch_id
    or v_claim.image_sha256 is distinct from p_image_sha256
    or v_run.authorized_image_sha256 is distinct from p_image_sha256 then
    return jsonb_build_object('accepted', false, 'code', 'RC_DISPATCH_IDENTITY_MISMATCH');
  end if;
  if v_claim.status <> 'RESERVED'
    or v_now > v_claim.expires_at
    or v_run.status <> 'ACTIVE'
    or v_run.deadline_at is null
    or v_now > v_run.deadline_at
    or v_run.dispatched_count <> 0
    or v_run.consumed
    or v_batch.status <> 'OPEN'
    or v_batch.active_dispatches >= v_batch.max_concurrency
    or v_batch.dispatched_attempts >= v_batch.max_attempts
    or v_batch.reserved_cost_cny + v_batch.reserve_per_attempt_cny > v_batch.total_cost_limit_cny then
    return jsonb_build_object('accepted', false, 'code', 'RC_DISPATCH_CLOSED');
  end if;
  update private.indonesia_structured_b_claims
    set status = 'DISPATCHED', dispatched_at = v_now, updated_at = v_now
    where claim_id = v_claim.claim_id;
  update private.indonesia_structured_b_single_runs
    set dispatched_count = dispatched_count + 1, consumed = true, updated_at = v_now
    where run_id = p_run_id;
  update private.indonesia_structured_b_batches
    set dispatched_attempts = dispatched_attempts + 1,
        reserved_cost_cny = reserved_cost_cny + reserve_per_attempt_cny,
        active_dispatches = active_dispatches + 1,
        updated_at = v_now
    where batch_id = p_batch_id;
  return jsonb_build_object('accepted', true, 'action', 'DISPATCHED');
end
$$;

create or replace function public.settle_indonesia_structured_b_single_run(
  p_project_ref text,
  p_run_id text,
  p_batch_id text,
  p_recognition_request_id uuid,
  p_token_sha256 text,
  p_outcome text,
  p_provider_call_count integer,
  p_prompt_tokens integer default null,
  p_completion_tokens integer default null,
  p_total_tokens integer default null,
  p_estimated_cost_cny numeric default null
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_now timestamptz;
  v_run private.indonesia_structured_b_single_runs%rowtype;
  v_batch private.indonesia_structured_b_batches%rowtype;
  v_claim private.indonesia_structured_b_claims%rowtype;
begin
  select * into v_batch from private.indonesia_structured_b_batches where batch_id = p_batch_id for update;
  select * into v_run from private.indonesia_structured_b_single_runs where run_id = p_run_id for update;
  if v_run.run_id is null then return jsonb_build_object('accepted', false, 'code', 'RC_SETTLEMENT_RUN_NOT_FOUND'); end if;
  select * into v_claim from private.indonesia_structured_b_claims
    where run_id = p_run_id and recognition_request_id = p_recognition_request_id and token_sha256 = p_token_sha256
    for update;
  v_now := clock_timestamp();
  if v_batch.batch_id is null or v_claim.claim_id is null or p_project_ref is distinct from 'thjojitdafxfarhxhoyo'
    or p_batch_id is distinct from 'indonesia-ab-20261010-v1'
    or v_batch.source_ledger_sha256 is distinct from '0ed002526e26a440f15ef20e82cfc1ee3c05099a3cb3b8bf5688c145d14e6aeb'
    or v_batch.project_ref is distinct from p_project_ref or v_run.project_ref is distinct from p_project_ref
    or v_run.batch_id is distinct from p_batch_id or v_claim.batch_id is distinct from p_batch_id then
    return jsonb_build_object('accepted', false, 'code', 'RC_SETTLEMENT_IDENTITY_MISMATCH');
  end if;
  if p_outcome is null
    or p_outcome not in ('COMPLETED', 'FAILED_AFTER_PROVIDER', 'OUTCOME_UNKNOWN', 'FAILED_PRE_PROVIDER')
    or p_provider_call_count is null
    or p_provider_call_count not in (0, 1)
    or (p_total_tokens is not null and (p_prompt_tokens is null or p_completion_tokens is null or p_total_tokens <> p_prompt_tokens + p_completion_tokens)) then
    return jsonb_build_object('accepted', false, 'code', 'RC_SETTLEMENT_INPUT_INVALID');
  end if;
  if v_claim.status in ('COMPLETED', 'FAILED_PRE_PROVIDER', 'FAILED_AFTER_PROVIDER', 'OUTCOME_UNKNOWN') then
    if v_claim.status = p_outcome then return jsonb_build_object('accepted', true, 'action', 'SETTLED'); end if;
    return jsonb_build_object('accepted', false, 'code', 'RC_SETTLEMENT_ALREADY_FINAL');
  end if;
  if p_outcome = 'FAILED_PRE_PROVIDER' then
    if v_claim.status <> 'RESERVED' or p_provider_call_count <> 0 then
      return jsonb_build_object('accepted', false, 'code', 'RC_PRE_PROVIDER_SETTLEMENT_INVALID');
    end if;
    update private.indonesia_structured_b_claims
      set status = p_outcome, provider_call_count = 0, outcome_code = p_outcome,
          settled_at = v_now, updated_at = v_now
      where claim_id = v_claim.claim_id;
    update private.indonesia_structured_b_single_runs
      set status = p_outcome, terminal_reason = p_outcome, closed_at = v_now, updated_at = v_now
      where run_id = p_run_id;
    return jsonb_build_object('accepted', true, 'action', 'SETTLED');
  end if;
  if v_claim.status <> 'DISPATCHED' or p_provider_call_count <> 1 then
    return jsonb_build_object('accepted', false, 'code', 'RC_POST_DISPATCH_SETTLEMENT_INVALID');
  end if;
  update private.indonesia_structured_b_claims
    set status = p_outcome,
        provider_call_count = p_provider_call_count,
        prompt_tokens = p_prompt_tokens,
        completion_tokens = p_completion_tokens,
        total_tokens = p_total_tokens,
        estimated_cost_cny = p_estimated_cost_cny,
        outcome_code = p_outcome,
        settled_at = v_now,
        updated_at = v_now
    where claim_id = v_claim.claim_id;
  update private.indonesia_structured_b_batches
    set active_dispatches = greatest(0, active_dispatches - 1),
        estimated_cost_cny = estimated_cost_cny + coalesce(p_estimated_cost_cny, 0),
        updated_at = v_now
    where batch_id = v_batch.batch_id;
  update private.indonesia_structured_b_single_runs
    set status = p_outcome, terminal_reason = p_outcome, closed_at = v_now, updated_at = v_now
    where run_id = p_run_id;
  return jsonb_build_object('accepted', true, 'action', 'SETTLED');
end
$$;

create or replace function public.close_indonesia_structured_b_single_run(
  p_project_ref text,
  p_run_id text,
  p_batch_id text
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_now timestamptz;
  v_batch private.indonesia_structured_b_batches%rowtype;
  v_run private.indonesia_structured_b_single_runs%rowtype;
begin
  select * into v_batch from private.indonesia_structured_b_batches where batch_id = p_batch_id for update;
  select * into v_run from private.indonesia_structured_b_single_runs where run_id = p_run_id for update;
  v_now := clock_timestamp();
  if v_batch.batch_id is null or v_run.run_id is null
    or p_project_ref is distinct from 'thjojitdafxfarhxhoyo'
    or p_batch_id is distinct from 'indonesia-ab-20261010-v1'
    or v_batch.source_ledger_sha256 is distinct from '0ed002526e26a440f15ef20e82cfc1ee3c05099a3cb3b8bf5688c145d14e6aeb'
    or v_batch.project_ref is distinct from p_project_ref
    or v_run.project_ref is distinct from p_project_ref
    or v_run.batch_id is distinct from p_batch_id then
    return jsonb_build_object('accepted', false, 'code', 'RC_CLOSE_IDENTITY_MISMATCH');
  end if;
  update private.indonesia_structured_b_single_runs
    set status = 'CLOSED',
        terminal_reason = coalesce(terminal_reason, 'OPERATOR_CLOSED'),
        closed_at = coalesce(closed_at, v_now),
        updated_at = v_now
    where run_id = p_run_id;
  return jsonb_build_object('accepted', true, 'action', 'CLOSED');
end
$$;

revoke all on function public.claim_indonesia_structured_b_single_run(text, text, text, text, uuid, text, boolean) from public, anon, authenticated;
revoke all on function public.dispatch_indonesia_structured_b_single_run(text, text, text, text, uuid, text) from public, anon, authenticated;
revoke all on function public.settle_indonesia_structured_b_single_run(text, text, text, uuid, text, text, integer, integer, integer, integer, numeric) from public, anon, authenticated;
revoke all on function public.close_indonesia_structured_b_single_run(text, text, text) from public, anon, authenticated;

grant execute on function public.claim_indonesia_structured_b_single_run(text, text, text, text, uuid, text, boolean) to service_role;
grant execute on function public.dispatch_indonesia_structured_b_single_run(text, text, text, text, uuid, text) to service_role;
grant execute on function public.settle_indonesia_structured_b_single_run(text, text, text, uuid, text, text, integer, integer, integer, integer, numeric) to service_role;
grant execute on function public.close_indonesia_structured_b_single_run(text, text, text) to service_role;

commit;
