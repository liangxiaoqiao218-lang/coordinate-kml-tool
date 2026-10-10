begin;

insert into private.coordinate_three_case_rc_runs (
  run_id, batch_id, project_ref, manifest_sha256, max_claims, max_dispatched,
  max_concurrency, claim_ttl_seconds, run_window_seconds
) values (
  'coordinate-three-case-rc-20261011-r3', 'coordinate-three-case-rc-20261011-v3',
  'thjojitdafxfarhxhoyo', 'f511490c8e0b28d18dfd1867a2d50c28890e6011065c9b478bd01fd3ba04b7fe',
  3, 3, 1, 600, 1800
);

insert into private.coordinate_three_case_rc_cases (
  run_id, case_id, image_sha256, product_mode, budget_pool, reserve_cny
) values
  ('coordinate-three-case-rc-20261011-r3', 'indonesia', '41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2', 'indonesia_utm50s_structured_b', 'INDONESIA_HISTORICAL_6_CALL_5_CNY', 0.816000),
  ('coordinate-three-case-rc-20261011-r3', 'mgrs', 'af999328e3232af304e03901c5e8d58cad794ea588ee31709aef6668974f4004', '', 'MGRS_BFTM_SHARED_2_CNY', 1.000000),
  ('coordinate-three-case-rc-20261011-r3', 'bftm', '4567ce9889c47d65414b19e543c06b5322b86c53b76bb53e87da761bc0988e1d', '', 'MGRS_BFTM_SHARED_2_CNY', 1.000000);

create or replace function public.activate_coordinate_three_case_rc_run(
  p_project_ref text, p_run_id text, p_batch_id text, p_manifest_sha256 text
) returns jsonb language plpgsql security definer set search_path = '' as $$
declare
  v_now timestamptz := clock_timestamp();
  v_run private.coordinate_three_case_rc_runs%rowtype;
  v_legacy private.indonesia_structured_b_batches%rowtype;
  v_shared private.coordinate_three_case_rc_non_indonesia_budget%rowtype;
begin
  select * into v_run from private.coordinate_three_case_rc_runs where run_id = p_run_id for update;
  select * into v_legacy from private.indonesia_structured_b_batches where batch_id = 'indonesia-ab-20261010-v1' for update;
  select * into v_shared from private.coordinate_three_case_rc_non_indonesia_budget
    where batch_id = 'coordinate-three-case-rc-20261011-v1' for update;
  if v_run.run_id is null or v_legacy.batch_id is null or v_shared.batch_id is null then
    return jsonb_build_object('accepted', false, 'code', 'RC_REQUIRED_LEDGER_NOT_FOUND');
  end if;
  if p_project_ref is distinct from 'thjojitdafxfarhxhoyo'
    or p_run_id is distinct from 'coordinate-three-case-rc-20261011-r3'
    or p_batch_id is distinct from 'coordinate-three-case-rc-20261011-v3'
    or p_manifest_sha256 is distinct from 'f511490c8e0b28d18dfd1867a2d50c28890e6011065c9b478bd01fd3ba04b7fe'
    or v_run.project_ref is distinct from p_project_ref
    or v_run.batch_id is distinct from p_batch_id
    or v_run.manifest_sha256 is distinct from p_manifest_sha256
    or v_legacy.project_ref is distinct from p_project_ref
    or v_legacy.max_attempts is distinct from 6
    or v_legacy.total_cost_limit_cny is distinct from 5.000000
    or v_legacy.status is distinct from 'OPEN'
    or v_legacy.active_dispatches is distinct from 0
    or v_legacy.dispatched_attempts >= v_legacy.max_attempts
    or greatest(v_legacy.reserved_cost_cny, v_legacy.estimated_cost_cny) + 0.816000 > v_legacy.total_cost_limit_cny
    or v_shared.max_dispatched is distinct from 2
    or v_shared.total_cost_limit_cny is distinct from 2.000000
    or v_shared.active_dispatches is distinct from 0
    or v_shared.dispatched_count + 2 > v_shared.max_dispatched
    or greatest(v_shared.reserved_cost_cny, v_shared.estimated_cost_cny) + 2.000000 > v_shared.total_cost_limit_cny then
    return jsonb_build_object('accepted', false, 'code', 'RC_ACTIVATION_IDENTITY_OR_BUDGET_REJECTED');
  end if;
  if v_run.status = 'ARMED' then
    update private.coordinate_three_case_rc_runs set status = 'ACTIVE', activated_at = v_now,
      deadline_at = v_now + make_interval(secs => run_window_seconds), updated_at = v_now
      where run_id = p_run_id returning * into v_run;
    return jsonb_build_object('accepted', true, 'action', 'ACTIVATED');
  end if;
  if v_run.status = 'ACTIVE' and v_run.deadline_at is not null and v_now <= v_run.deadline_at then
    return jsonb_build_object('accepted', true, 'action', 'ALREADY_ACTIVE');
  end if;
  return jsonb_build_object('accepted', false, 'code', 'RC_RUN_NOT_ACTIVATABLE');
end $$;

create or replace function public.dispatch_coordinate_three_case_rc(
  p_project_ref text, p_run_id text, p_batch_id text, p_manifest_sha256 text,
  p_case_id text, p_image_sha256 text, p_recognition_request_id uuid, p_token_sha256 text, p_model text
) returns jsonb language plpgsql security definer set search_path = '' as $$
declare
  v_now timestamptz := clock_timestamp();
  v_run private.coordinate_three_case_rc_runs%rowtype;
  v_case private.coordinate_three_case_rc_cases%rowtype;
  v_claim private.coordinate_three_case_rc_claims%rowtype;
  v_legacy private.indonesia_structured_b_batches%rowtype;
  v_shared private.coordinate_three_case_rc_non_indonesia_budget%rowtype;
begin
  select * into v_run from private.coordinate_three_case_rc_runs where run_id = p_run_id for update;
  select * into v_case from private.coordinate_three_case_rc_cases where run_id = p_run_id and case_id = p_case_id for update;
  select * into v_claim from private.coordinate_three_case_rc_claims where run_id = p_run_id and case_id = p_case_id
    and recognition_request_id = p_recognition_request_id and token_sha256 = p_token_sha256 for update;
  if v_run.run_id is null or v_case.case_id is null or v_claim.claim_id is null then return jsonb_build_object('accepted', false, 'code', 'RC_DISPATCH_BINDING_NOT_FOUND'); end if;
  if p_project_ref is distinct from v_run.project_ref or p_batch_id is distinct from v_run.batch_id
    or p_manifest_sha256 is distinct from v_run.manifest_sha256 or p_image_sha256 is distinct from v_case.image_sha256
    or p_model is distinct from 'qwen3.8-flash' then
    return jsonb_build_object('accepted', false, 'code', 'RC_DISPATCH_IDENTITY_OR_MODEL_MISMATCH');
  end if;
  if v_run.status <> 'ACTIVE' or v_run.deadline_at is null or v_now > v_run.deadline_at
    or v_run.active_dispatches <> 0 or v_run.dispatched_count >= v_run.max_dispatched
    or v_case.status <> 'CLAIMED' or v_case.dispatch_count <> 0
    or v_claim.status <> 'RESERVED' or v_now > v_claim.expires_at then
    return jsonb_build_object('accepted', false, 'code', 'RC_DISPATCH_CLOSED_OR_CONCURRENT');
  end if;
  if p_case_id = 'indonesia' then
    select * into v_legacy from private.indonesia_structured_b_batches where batch_id = 'indonesia-ab-20261010-v1' for update;
    if v_legacy.batch_id is null or v_legacy.status <> 'OPEN' or v_legacy.active_dispatches <> 0
      or v_legacy.dispatched_attempts >= v_legacy.max_attempts
      or greatest(v_legacy.reserved_cost_cny, v_legacy.estimated_cost_cny) + v_case.reserve_cny > v_legacy.total_cost_limit_cny then
      return jsonb_build_object('accepted', false, 'code', 'RC_INDONESIA_HISTORICAL_BUDGET_REJECTED');
    end if;
    update private.indonesia_structured_b_batches set dispatched_attempts = dispatched_attempts + 1,
      reserved_cost_cny = greatest(reserved_cost_cny, estimated_cost_cny) + v_case.reserve_cny,
      active_dispatches = active_dispatches + 1,
      updated_at = v_now where batch_id = v_legacy.batch_id;
  else
    select * into v_shared from private.coordinate_three_case_rc_non_indonesia_budget
      where batch_id = 'coordinate-three-case-rc-20261011-v1' for update;
    if v_shared.batch_id is null or v_shared.active_dispatches <> 0 or v_shared.dispatched_count >= v_shared.max_dispatched
      or greatest(v_shared.reserved_cost_cny, v_shared.estimated_cost_cny) + v_case.reserve_cny > v_shared.total_cost_limit_cny then
      return jsonb_build_object('accepted', false, 'code', 'RC_NON_INDONESIA_SHARED_BUDGET_REJECTED');
    end if;
    update private.coordinate_three_case_rc_non_indonesia_budget set dispatched_count = dispatched_count + 1,
      reserved_cost_cny = greatest(reserved_cost_cny, estimated_cost_cny) + v_case.reserve_cny,
      active_dispatches = active_dispatches + 1,
      updated_at = v_now where batch_id = 'coordinate-three-case-rc-20261011-v1';
  end if;
  update private.coordinate_three_case_rc_claims set status = 'DISPATCHED', model = p_model,
    dispatched_at = v_now, updated_at = v_now where claim_id = v_claim.claim_id;
  update private.coordinate_three_case_rc_cases set status = 'DISPATCHED', dispatch_count = 1, updated_at = v_now
    where run_id = p_run_id and case_id = p_case_id;
  update private.coordinate_three_case_rc_runs set dispatched_count = dispatched_count + 1,
    active_dispatches = active_dispatches + 1, updated_at = v_now where run_id = p_run_id;
  return jsonb_build_object('accepted', true, 'action', 'DISPATCHED');
end $$;

create or replace function public.settle_coordinate_three_case_rc(
  p_project_ref text, p_run_id text, p_batch_id text, p_manifest_sha256 text,
  p_case_id text, p_recognition_request_id uuid, p_token_sha256 text, p_outcome text,
  p_provider_call_count integer, p_prompt_tokens integer default null,
  p_completion_tokens integer default null, p_total_tokens integer default null,
  p_estimated_cost_cny numeric default null, p_cost_evidence text default 'UNKNOWN'
) returns jsonb language plpgsql security definer set search_path = '' as $$
declare
  v_now timestamptz := clock_timestamp();
  v_run private.coordinate_three_case_rc_runs%rowtype;
  v_case private.coordinate_three_case_rc_cases%rowtype;
  v_claim private.coordinate_three_case_rc_claims%rowtype;
begin
  select * into v_run from private.coordinate_three_case_rc_runs where run_id = p_run_id for update;
  select * into v_case from private.coordinate_three_case_rc_cases where run_id = p_run_id and case_id = p_case_id for update;
  select * into v_claim from private.coordinate_three_case_rc_claims where run_id = p_run_id and case_id = p_case_id
    and recognition_request_id = p_recognition_request_id and token_sha256 = p_token_sha256 for update;
  if v_run.run_id is null or v_case.case_id is null or v_claim.claim_id is null then return jsonb_build_object('accepted', false, 'code', 'RC_SETTLEMENT_BINDING_NOT_FOUND'); end if;
  if p_project_ref is distinct from v_run.project_ref or p_batch_id is distinct from v_run.batch_id
    or p_manifest_sha256 is distinct from v_run.manifest_sha256
    or p_outcome not in ('COMPLETED', 'FAILED_PRE_PROVIDER', 'FAILED_AFTER_PROVIDER', 'OUTCOME_UNKNOWN')
    or p_provider_call_count not in (0, 1) or p_cost_evidence not in ('UNKNOWN', 'USAGE_REPORTED', 'NOT_INCURRED')
    or (p_cost_evidence = 'USAGE_REPORTED' and (p_estimated_cost_cny is null or p_total_tokens is null))
    or (p_cost_evidence <> 'USAGE_REPORTED' and p_estimated_cost_cny is not null)
    or (p_total_tokens is not null and (p_prompt_tokens is null or p_completion_tokens is null or p_total_tokens <> p_prompt_tokens + p_completion_tokens)) then
    return jsonb_build_object('accepted', false, 'code', 'RC_SETTLEMENT_CONTRACT_REJECTED');
  end if;
  if v_claim.status in ('COMPLETED', 'FAILED_PRE_PROVIDER', 'FAILED_AFTER_PROVIDER', 'OUTCOME_UNKNOWN') then
    if v_claim.status = p_outcome then return jsonb_build_object('accepted', true, 'action', 'SETTLED'); end if;
    return jsonb_build_object('accepted', false, 'code', 'RC_SETTLEMENT_CONFLICT');
  end if;
  if p_outcome = 'FAILED_PRE_PROVIDER' then
    if v_claim.status <> 'RESERVED' or p_provider_call_count <> 0 or p_cost_evidence <> 'NOT_INCURRED' then
      return jsonb_build_object('accepted', false, 'code', 'RC_PRE_PROVIDER_SETTLEMENT_REJECTED');
    end if;
  elsif v_claim.status <> 'DISPATCHED' or p_provider_call_count <> 1 then
    return jsonb_build_object('accepted', false, 'code', 'RC_POST_PROVIDER_SETTLEMENT_REJECTED');
  end if;
  update private.coordinate_three_case_rc_claims set status = p_outcome, provider_call_count = p_provider_call_count,
    prompt_tokens = p_prompt_tokens, completion_tokens = p_completion_tokens, total_tokens = p_total_tokens,
    estimated_cost_cny = p_estimated_cost_cny, cost_evidence = p_cost_evidence, settled_at = v_now, updated_at = v_now
    where claim_id = v_claim.claim_id;
  update private.coordinate_three_case_rc_cases set status = p_outcome, provider_call_count = p_provider_call_count,
    outcome = p_outcome, cost_evidence = p_cost_evidence, estimated_cost_cny = p_estimated_cost_cny,
    updated_at = v_now where run_id = p_run_id and case_id = p_case_id;
  if v_claim.status = 'DISPATCHED' then
    update private.coordinate_three_case_rc_runs set active_dispatches = greatest(0, active_dispatches - 1), updated_at = v_now where run_id = p_run_id;
    if p_case_id = 'indonesia' then
      update private.indonesia_structured_b_batches set active_dispatches = greatest(0, active_dispatches - 1),
        estimated_cost_cny = estimated_cost_cny + coalesce(p_estimated_cost_cny, 0), updated_at = v_now
        where batch_id = 'indonesia-ab-20261010-v1';
    else
      update private.coordinate_three_case_rc_non_indonesia_budget set active_dispatches = greatest(0, active_dispatches - 1),
        estimated_cost_cny = estimated_cost_cny + coalesce(p_estimated_cost_cny, 0), updated_at = v_now
        where batch_id = 'coordinate-three-case-rc-20261011-v1';
    end if;
  end if;
  return jsonb_build_object('accepted', true, 'action', 'SETTLED');
end $$;

create or replace function public.read_coordinate_three_case_rc_status(
  p_project_ref text, p_run_id text, p_batch_id text, p_manifest_sha256 text
) returns jsonb language plpgsql security definer set search_path = '' as $$
declare v_run private.coordinate_three_case_rc_runs%rowtype; v_legacy private.indonesia_structured_b_batches%rowtype;
  v_shared private.coordinate_three_case_rc_non_indonesia_budget%rowtype; v_cases jsonb;
begin
  select * into v_run from private.coordinate_three_case_rc_runs where run_id = p_run_id;
  if v_run.run_id is null or p_project_ref is distinct from v_run.project_ref or p_batch_id is distinct from v_run.batch_id
    or p_manifest_sha256 is distinct from v_run.manifest_sha256 then return jsonb_build_object('accepted', false, 'code', 'RC_STATUS_REJECTED'); end if;
  select * into v_legacy from private.indonesia_structured_b_batches where batch_id = 'indonesia-ab-20261010-v1';
  select * into v_shared from private.coordinate_three_case_rc_non_indonesia_budget
    where batch_id = 'coordinate-three-case-rc-20261011-v1';
  select jsonb_agg(jsonb_build_object('case_id', case_id, 'status', status, 'claim_count', claim_count,
    'dispatch_count', dispatch_count, 'provider_call_count', provider_call_count, 'outcome', outcome,
    'cost_evidence', cost_evidence, 'estimated_cost_cny', estimated_cost_cny) order by case_id)
    into v_cases from private.coordinate_three_case_rc_cases where run_id = p_run_id;
  return jsonb_build_object('accepted', true, 'action', 'STATUS', 'run_status', v_run.status,
    'run_activated', v_run.activated_at is not null, 'run_closed', v_run.status = 'CLOSED',
    'active_dispatches', v_run.active_dispatches, 'total_dispatched', v_run.dispatched_count,
    'indonesia_historical_dispatched', v_legacy.dispatched_attempts,
    'indonesia_historical_reserved_cny', v_legacy.reserved_cost_cny,
    'non_indonesia_dispatched', v_shared.dispatched_count,
    'non_indonesia_reserved_cny', v_shared.reserved_cost_cny,
    'non_indonesia_estimated_cost_cny', v_shared.estimated_cost_cny, 'cases', coalesce(v_cases, '[]'::jsonb));
end $$;

revoke all on function public.activate_coordinate_three_case_rc_run(text, text, text, text) from public, anon, authenticated;
revoke all on function public.dispatch_coordinate_three_case_rc(text, text, text, text, text, text, uuid, text, text) from public, anon, authenticated;
revoke all on function public.settle_coordinate_three_case_rc(text, text, text, text, text, uuid, text, text, integer, integer, integer, integer, numeric, text) from public, anon, authenticated;
revoke all on function public.read_coordinate_three_case_rc_status(text, text, text, text) from public, anon, authenticated;
grant execute on function public.activate_coordinate_three_case_rc_run(text, text, text, text) to service_role;
grant execute on function public.dispatch_coordinate_three_case_rc(text, text, text, text, text, text, uuid, text, text) to service_role;
grant execute on function public.settle_coordinate_three_case_rc(text, text, text, text, text, uuid, text, text, integer, integer, integer, integer, numeric, text) to service_role;
grant execute on function public.read_coordinate_three_case_rc_status(text, text, text, text) to service_role;

commit;
