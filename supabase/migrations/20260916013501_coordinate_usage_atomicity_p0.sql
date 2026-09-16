begin;

create schema if not exists private;
revoke all on schema private from public, anon, authenticated;

create table private.coordinate_recognition_commits (
  recognition_request_id uuid primary key,
  user_id text not null references public.users(user_id) on delete restrict,
  session_binding_sha256 text not null check (session_binding_sha256 ~ '^[0-9a-f]{64}$'),
  state text not null check (state in ('PREPARED', 'COMMITTED', 'FAILED', 'EXPIRED')),
  authority_result_id text not null,
  authority_result_revision integer not null check (authority_result_revision >= 1),
  authority_decision_state text not null check (authority_decision_state in ('AUTO_EXPORT', 'REVIEW_REQUIRED')),
  authority_geometry_hash text not null check (authority_geometry_hash ~ '^sha256:[0-9a-f]{64}$'),
  sealed_result jsonb not null,
  sealed_result_sha256 text not null check (sealed_result_sha256 ~ '^[0-9a-f]{64}$'),
  encryption_version text not null check (encryption_version = 'AES_256_GCM_V1'),
  provider_cost_state text not null check (provider_cost_state in ('NOT_INCURRED', 'POSSIBLY_INCURRED', 'USAGE_REPORTED')),
  consume_type text check (consume_type in ('free', 'paid')),
  failure_reason text check (failure_reason is null or failure_reason in ('QUOTA_EXHAUSTED')),
  prepared_at timestamptz not null default statement_timestamp(),
  expires_at timestamptz not null default (statement_timestamp() + interval '24 hours'),
  committed_at timestamptz,
  updated_at timestamptz not null default statement_timestamp(),
  check (
    jsonb_typeof(sealed_result) = 'object'
    and sealed_result ?& array['version', 'iv', 'ciphertext', 'authTag']
    and sealed_result - array['version', 'iv', 'ciphertext', 'authTag'] = '{}'::jsonb
    and sealed_result ->> 'version' = 'AES_256_GCM_V1'
    and sealed_result ->> 'iv' ~ '^[A-Za-z0-9+/]{16}$'
    and sealed_result ->> 'authTag' ~ '^[A-Za-z0-9+/]{22}==$'
    and sealed_result ->> 'ciphertext' ~ '^[A-Za-z0-9+/]+={0,2}$'
  ),
  check (state <> 'COMMITTED' or (committed_at is not null and consume_type is not null)),
  check (state <> 'FAILED' or failure_reason is not null)
);

alter table private.coordinate_recognition_commits enable row level security;
revoke all on table private.coordinate_recognition_commits from public, anon, authenticated, service_role;

create index coordinate_recognition_commits_prepared_expiry_idx
  on private.coordinate_recognition_commits (expires_at, recognition_request_id)
  where state = 'PREPARED';

alter table public.usage_logs
  add column recognition_request_id uuid,
  add column authority_result_id text,
  add column authority_result_revision integer,
  add column authority_decision_state text,
  add column authority_geometry_hash text;

alter table public.usage_logs
  add constraint usage_logs_authority_revision_positive
    check (authority_result_revision is null or authority_result_revision >= 1),
  add constraint usage_logs_authority_decision_valid
    check (authority_decision_state is null or authority_decision_state in ('AUTO_EXPORT', 'REVIEW_REQUIRED')),
  add constraint usage_logs_authority_geometry_hash_valid
    check (authority_geometry_hash is null or authority_geometry_hash ~ '^sha256:[0-9a-f]{64}$'),
  add constraint usage_logs_atomic_identity_all_or_none
    check (
      (recognition_request_id is null
        and authority_result_id is null
        and authority_result_revision is null
        and authority_decision_state is null
        and authority_geometry_hash is null)
      or
      (recognition_request_id is not null
        and authority_result_id is not null
        and authority_result_revision is not null
        and authority_decision_state is not null
        and authority_geometry_hash is not null)
    );

create unique index usage_logs_recognition_request_id_uidx
  on public.usage_logs (recognition_request_id)
  where recognition_request_id is not null;

create or replace function public.prepare_coordinate_recognition_result(
  p_recognition_request_id uuid,
  p_user_id text,
  p_session_binding_sha256 text,
  p_authority_result_id text,
  p_authority_result_revision integer,
  p_authority_decision_state text,
  p_authority_geometry_hash text,
  p_sealed_result jsonb,
  p_sealed_result_sha256 text,
  p_encryption_version text,
  p_provider_cost_state text
)
returns table (
  result text,
  state text,
  consume_type text,
  quota jsonb,
  sealed_result jsonb,
  sealed_result_sha256 text
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_row private.coordinate_recognition_commits%rowtype;
begin
  if p_recognition_request_id is null
    or coalesce(length(p_user_id), 0) = 0
    or p_session_binding_sha256 !~ '^[0-9a-f]{64}$'
    or coalesce(length(p_authority_result_id), 0) = 0
    or p_authority_result_revision < 1
    or p_authority_decision_state not in ('AUTO_EXPORT', 'REVIEW_REQUIRED')
    or p_authority_geometry_hash !~ '^sha256:[0-9a-f]{64}$'
    or jsonb_typeof(p_sealed_result) <> 'object'
    or p_sealed_result_sha256 !~ '^[0-9a-f]{64}$'
    or p_encryption_version <> 'AES_256_GCM_V1'
    or p_provider_cost_state not in ('NOT_INCURRED', 'POSSIBLY_INCURRED', 'USAGE_REPORTED') then
    raise exception using errcode = '22023', message = 'COORDINATE_USAGE_PREPARE_INVALID';
  end if;

  insert into private.coordinate_recognition_commits (
    recognition_request_id,
    user_id,
    session_binding_sha256,
    state,
    authority_result_id,
    authority_result_revision,
    authority_decision_state,
    authority_geometry_hash,
    sealed_result,
    sealed_result_sha256,
    encryption_version,
    provider_cost_state
  ) values (
    p_recognition_request_id,
    p_user_id,
    p_session_binding_sha256,
    'PREPARED',
    p_authority_result_id,
    p_authority_result_revision,
    p_authority_decision_state,
    p_authority_geometry_hash,
    p_sealed_result,
    p_sealed_result_sha256,
    p_encryption_version,
    p_provider_cost_state
  )
  on conflict (recognition_request_id) do nothing;

  select * into v_row
  from private.coordinate_recognition_commits
  where coordinate_recognition_commits.recognition_request_id = p_recognition_request_id
  for update;

  if not found then
    raise exception using errcode = 'P0001', message = 'COORDINATE_USAGE_PREPARE_NOT_FOUND';
  end if;

  if v_row.user_id <> p_user_id
    or v_row.session_binding_sha256 <> p_session_binding_sha256
    or v_row.authority_result_id <> p_authority_result_id
    or v_row.authority_result_revision <> p_authority_result_revision
    or v_row.authority_decision_state <> p_authority_decision_state
    or v_row.authority_geometry_hash <> p_authority_geometry_hash
    or v_row.sealed_result_sha256 <> p_sealed_result_sha256
    or v_row.encryption_version <> p_encryption_version then
    raise exception using errcode = 'P0001', message = 'COORDINATE_USAGE_IDEMPOTENCY_CONFLICT';
  end if;

  if v_row.state = 'COMMITTED' then
    return query select 'ALREADY_COMMITTED'::text, v_row.state, v_row.consume_type,
      null::jsonb, null::jsonb, null::text;
  elsif v_row.state = 'PREPARED' and v_row.expires_at > statement_timestamp() then
    return query select
      case when v_row.prepared_at = v_row.updated_at then 'PREPARED'::text else 'ALREADY_PREPARED'::text end,
      v_row.state, v_row.consume_type, null::jsonb, null::jsonb, null::text;
  elsif v_row.state = 'PREPARED' then
    update private.coordinate_recognition_commits
      set state = 'EXPIRED', updated_at = statement_timestamp()
      where recognition_request_id = p_recognition_request_id;
    return query select 'EXPIRED'::text, 'EXPIRED'::text, null::text, null::jsonb, null::jsonb, null::text;
  end if;

  return query select v_row.state, v_row.state, v_row.consume_type, null::jsonb, null::jsonb, null::text;
end;
$$;

create or replace function public.commit_coordinate_recognition_usage(
  p_recognition_request_id uuid,
  p_user_id text,
  p_session_binding_sha256 text
)
returns table (
  result text,
  state text,
  consume_type text,
  quota jsonb,
  sealed_result jsonb,
  sealed_result_sha256 text
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_commit private.coordinate_recognition_commits%rowtype;
  v_user public.users%rowtype;
  v_consume_type text;
  v_before_balance jsonb;
  v_after_balance jsonb;
begin
  select * into v_commit
  from private.coordinate_recognition_commits
  where coordinate_recognition_commits.recognition_request_id = p_recognition_request_id
  for update;

  if not found or v_commit.user_id <> p_user_id or v_commit.session_binding_sha256 <> p_session_binding_sha256 then
    return query select 'NOT_FOUND'::text, null::text, null::text, null::jsonb, null::jsonb, null::text;
    return;
  end if;
  if v_commit.state = 'COMMITTED' then
    select * into v_user from public.users where users.user_id = p_user_id;
    return query select 'ALREADY_COMMITTED'::text, v_commit.state, v_commit.consume_type,
      jsonb_build_object(
        'freeConvertCount', greatest(coalesce(v_user.free_convert_count, 0), 0),
        'paidConvertCount', greatest(coalesce(v_user.paid_convert_count, 0), 0)
      ), null::jsonb, null::text;
    return;
  end if;
  if v_commit.state <> 'PREPARED' then
    return query select v_commit.state, v_commit.state, v_commit.consume_type, null::jsonb, null::jsonb, null::text;
    return;
  end if;
  if v_commit.expires_at <= statement_timestamp() then
    update private.coordinate_recognition_commits
      set state = 'EXPIRED', updated_at = statement_timestamp()
      where recognition_request_id = p_recognition_request_id;
    return query select 'EXPIRED'::text, 'EXPIRED'::text, null::text, null::jsonb, null::jsonb, null::text;
    return;
  end if;

  select * into v_user
  from public.users
  where users.user_id = p_user_id
  for update;

  if not found then
    raise exception using errcode = '23503', message = 'COORDINATE_USAGE_USER_NOT_FOUND';
  end if;
  v_before_balance := jsonb_build_object(
    'free_convert_count', greatest(coalesce(v_user.free_convert_count, 0), 0),
    'paid_convert_count', greatest(coalesce(v_user.paid_convert_count, 0), 0)
  );

  if coalesce(v_user.free_convert_count, 0) > 0 then
    v_consume_type := 'free';
    update public.users
      set free_convert_count = free_convert_count - 1, updated_at = statement_timestamp()
      where user_id = p_user_id
      returning * into v_user;
  elsif coalesce(v_user.paid_convert_count, 0) > 0 then
    v_consume_type := 'paid';
    update public.users
      set paid_convert_count = paid_convert_count - 1, updated_at = statement_timestamp()
      where user_id = p_user_id
      returning * into v_user;
  else
    update private.coordinate_recognition_commits
      set state = 'FAILED', failure_reason = 'QUOTA_EXHAUSTED', updated_at = statement_timestamp()
      where recognition_request_id = p_recognition_request_id;
    insert into public.usage_logs (
      user_id, feature_type, consume_type, before_balance, after_balance, success, error_reason,
      recognition_request_id, authority_result_id, authority_result_revision,
      authority_decision_state, authority_geometry_hash
    ) values (
      p_user_id, 'convert', 'none', v_before_balance, v_before_balance, false, 'quota_exhausted',
      p_recognition_request_id, v_commit.authority_result_id, v_commit.authority_result_revision,
      v_commit.authority_decision_state, v_commit.authority_geometry_hash
    );
    return query select 'QUOTA_EXHAUSTED'::text, 'FAILED'::text, 'none'::text,
      jsonb_build_object(
        'freeConvertCount', 0,
        'paidConvertCount', 0
      ), null::jsonb, null::text;
    return;
  end if;

  v_after_balance := jsonb_build_object(
    'free_convert_count', greatest(coalesce(v_user.free_convert_count, 0), 0),
    'paid_convert_count', greatest(coalesce(v_user.paid_convert_count, 0), 0)
  );
  insert into public.usage_logs (
    user_id, feature_type, consume_type, before_balance, after_balance, success, note,
    recognition_request_id, authority_result_id, authority_result_revision,
    authority_decision_state, authority_geometry_hash
  ) values (
    p_user_id, 'convert', v_consume_type, v_before_balance, v_after_balance, true,
    'Coordinate recognition committed atomically',
    p_recognition_request_id, v_commit.authority_result_id, v_commit.authority_result_revision,
    v_commit.authority_decision_state, v_commit.authority_geometry_hash
  );
  update private.coordinate_recognition_commits
    set state = 'COMMITTED', consume_type = v_consume_type, committed_at = statement_timestamp(),
      updated_at = statement_timestamp(), failure_reason = null
    where recognition_request_id = p_recognition_request_id;

  return query select 'COMMITTED'::text, 'COMMITTED'::text, v_consume_type,
    jsonb_build_object(
      'freeConvertCount', greatest(coalesce(v_user.free_convert_count, 0), 0),
      'paidConvertCount', greatest(coalesce(v_user.paid_convert_count, 0), 0)
    ), null::jsonb, null::text;
end;
$$;

create or replace function public.get_coordinate_recognition_commit_state(
  p_recognition_request_id uuid,
  p_user_id text,
  p_session_binding_sha256 text
)
returns table (
  result text,
  state text,
  consume_type text,
  quota jsonb,
  sealed_result jsonb,
  sealed_result_sha256 text
)
language plpgsql
security definer
set search_path = ''
stable
as $$
declare
  v_commit private.coordinate_recognition_commits%rowtype;
  v_user public.users%rowtype;
begin
  select * into v_commit
  from private.coordinate_recognition_commits
  where coordinate_recognition_commits.recognition_request_id = p_recognition_request_id;
  if not found or v_commit.user_id <> p_user_id or v_commit.session_binding_sha256 <> p_session_binding_sha256 then
    return query select 'NOT_FOUND'::text, null::text, null::text, null::jsonb, null::jsonb, null::text;
    return;
  end if;
  if v_commit.state = 'PREPARED' and v_commit.expires_at <= statement_timestamp() then
    return query select 'EXPIRED'::text, 'EXPIRED'::text, null::text, null::jsonb, null::jsonb, null::text;
    return;
  end if;
  select * into v_user from public.users where users.user_id = p_user_id;
  return query select
    case when v_commit.state = 'COMMITTED' then 'ALREADY_COMMITTED'::text else v_commit.state end,
    v_commit.state,
    v_commit.consume_type,
    jsonb_build_object(
      'freeConvertCount', greatest(coalesce(v_user.free_convert_count, 0), 0),
      'paidConvertCount', greatest(coalesce(v_user.paid_convert_count, 0), 0)
    ),
    case when v_commit.state = 'COMMITTED' then v_commit.sealed_result else null::jsonb end,
    case when v_commit.state = 'COMMITTED' then v_commit.sealed_result_sha256 else null::text end;
end;
$$;

revoke all on function public.prepare_coordinate_recognition_result(uuid, text, text, text, integer, text, text, jsonb, text, text, text) from public, anon, authenticated;
revoke all on function public.commit_coordinate_recognition_usage(uuid, text, text) from public, anon, authenticated;
revoke all on function public.get_coordinate_recognition_commit_state(uuid, text, text) from public, anon, authenticated;
grant execute on function public.prepare_coordinate_recognition_result(uuid, text, text, text, integer, text, text, jsonb, text, text, text) to service_role;
grant execute on function public.commit_coordinate_recognition_usage(uuid, text, text) to service_role;
grant execute on function public.get_coordinate_recognition_commit_state(uuid, text, text) to service_role;

commit;
