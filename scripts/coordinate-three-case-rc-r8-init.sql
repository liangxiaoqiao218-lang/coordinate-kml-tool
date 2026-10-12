-- One RC-only initialization transaction, including the explicitly approved 0 -> 1 visitor grant.
-- Operational SQL for the one approved migration; do not run historical R7 migration again.
begin;
do $$
declare
  b private.indonesia_structured_b_batches%rowtype;
  u public.users%rowtype;
  n integer;
begin
  select * into b from private.indonesia_structured_b_batches where batch_id='indonesia-ab-20261010-v1' for update;
  select * into u from public.users where user_id='rc-b-release-20261010-4f125a9' for update;
  if b.batch_id is null or b.project_ref is distinct from 'thjojitdafxfarhxhoyo'
    or b.status is distinct from 'OPEN' or b.max_attempts is distinct from 6
    or b.total_cost_limit_cny is distinct from 5.000000 or b.dispatched_attempts is distinct from 4
    or b.reserved_cost_cny is distinct from 3.264000 or b.active_dispatches is distinct from 0
    or greatest(b.reserved_cost_cny,b.estimated_cost_cny)+0.816000>b.total_cost_limit_cny
    or u.user_id is null or u.is_vip is distinct from false
    or u.free_convert_count is distinct from 0 or u.paid_convert_count is distinct from 0
    or exists(select 1 from private.coordinate_three_case_rc_runs where status='ACTIVE')
    or exists(select 1 from private.coordinate_three_case_rc_runs where run_id='coordinate-three-case-rc-20261012-r8') then
    raise exception 'RC_R8_APPROVED_SNAPSHOT_MISMATCH';
  end if;
  update public.users set free_convert_count=1
    where user_id='rc-b-release-20261010-4f125a9' and is_vip=false and free_convert_count=0 and paid_convert_count=0;
  get diagnostics n=row_count;
  if n<>1 then raise exception 'RC_R8_VISITOR_GRANT_REJECTED'; end if;
end $$;

-- Exact-column/expression lookup, not name guesses or broad CHECK removal.
do $$
declare col text; old_value text; names text[];
begin
  foreach col in array array['max_claims','max_dispatched','run_window_seconds'] loop
    old_value := case when col='run_window_seconds' then '1800' else '3' end;
    select array_agg(c.conname) into names from pg_catalog.pg_constraint c
    where c.conrelid='private.coordinate_three_case_rc_runs'::regclass and c.contype='c'
      and c.conkey=array[(select a.attnum from pg_catalog.pg_attribute a where a.attrelid=c.conrelid and a.attname=col)]::smallint[]
      and regexp_replace(pg_catalog.pg_get_expr(c.conbin,c.conrelid,true),'[[:space:]()]','','g')=col||'='||old_value;
    if coalesce(cardinality(names),0)<>1 then raise exception 'RC_R8_RUN_CONSTRAINT_IDENTITY_MISMATCH'; end if;
    execute format('alter table private.coordinate_three_case_rc_runs drop constraint %I',names[1]);
  end loop;
end $$;
alter table private.coordinate_three_case_rc_runs add constraint coordinate_rc_r8_run_limits check (
  (run_id='coordinate-three-case-rc-20261012-r8' and max_claims=1 and max_dispatched=1 and run_window_seconds=900)
  or (run_id<>'coordinate-three-case-rc-20261012-r8' and max_claims=3 and max_dispatched=3 and run_window_seconds=1800)
);
insert into private.coordinate_three_case_rc_runs
  (run_id,batch_id,project_ref,manifest_sha256,max_claims,max_dispatched,max_concurrency,claim_ttl_seconds,run_window_seconds)
values ('coordinate-three-case-rc-20261012-r8','coordinate-three-case-rc-20261012-v8','thjojitdafxfarhxhoyo',
  '5448e91db16ec517b97bf47b976778c1f9d0cb6004e6056fb762d05070417e1b',1,1,1,600,900);
insert into private.coordinate_three_case_rc_cases(run_id,case_id,image_sha256,product_mode,budget_pool,reserve_cny)
values ('coordinate-three-case-rc-20261012-r8','indonesia','41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2',
  'indonesia_utm50s_structured_b','INDONESIA_HISTORICAL_6_CALL_5_CNY',0.816000);

create or replace function public.activate_coordinate_three_case_rc_run(
  p_project_ref text,p_run_id text,p_batch_id text,p_manifest_sha256 text
) returns jsonb language plpgsql security definer set search_path='' as $$
declare r private.coordinate_three_case_rc_runs%rowtype; b private.indonesia_structured_b_batches%rowtype;
  v_now timestamptz:=clock_timestamp();
begin
  select * into r from private.coordinate_three_case_rc_runs where run_id=p_run_id for update;
  select * into b from private.indonesia_structured_b_batches where batch_id='indonesia-ab-20261010-v1' for update;
  if p_project_ref is distinct from 'thjojitdafxfarhxhoyo' or p_run_id is distinct from 'coordinate-three-case-rc-20261012-r8'
    or p_batch_id is distinct from 'coordinate-three-case-rc-20261012-v8'
    or p_manifest_sha256 is distinct from '5448e91db16ec517b97bf47b976778c1f9d0cb6004e6056fb762d05070417e1b'
    or r.run_id is null or r.batch_id is distinct from p_batch_id or r.project_ref is distinct from p_project_ref
    or r.manifest_sha256 is distinct from p_manifest_sha256 then
    return jsonb_build_object('accepted',false,'code','RC_R8_ACTIVATION_BINDING_REJECTED');
  end if;
  if r.status='ACTIVE' and r.deadline_at>v_now then return jsonb_build_object('accepted',true,'action','ALREADY_ACTIVE'); end if;
  if r.status is distinct from 'ARMED' or b.batch_id is null or b.project_ref is distinct from p_project_ref
    or b.status is distinct from 'OPEN' or b.max_attempts is distinct from 6 or b.total_cost_limit_cny is distinct from 5.000000
    or b.dispatched_attempts is distinct from 4 or b.reserved_cost_cny is distinct from 3.264000 or b.active_dispatches is distinct from 0
    or greatest(b.reserved_cost_cny,b.estimated_cost_cny)+0.816000>b.total_cost_limit_cny
    or not exists(select 1 from public.users where user_id='rc-b-release-20261010-4f125a9'
      and is_vip=false and free_convert_count=1 and paid_convert_count=0) then
    return jsonb_build_object('accepted',false,'code','RC_R8_ACTIVATION_SNAPSHOT_REJECTED');
  end if;
  update private.coordinate_three_case_rc_runs set status='ACTIVE',activated_at=v_now,
    deadline_at=v_now+make_interval(secs=>run_window_seconds),updated_at=v_now where run_id=p_run_id;
  return jsonb_build_object('accepted',true,'action','ACTIVATED');
end $$;
revoke all on function public.activate_coordinate_three_case_rc_run(text,text,text,text) from public,anon,authenticated;
grant execute on function public.activate_coordinate_three_case_rc_run(text,text,text,text) to service_role;
commit;
