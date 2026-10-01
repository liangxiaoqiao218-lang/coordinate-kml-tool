create table if not exists public.coordinate_cases (
  case_id uuid primary key default gen_random_uuid(),
  case_number bigint generated always as identity unique,
  recognition_request_id uuid not null,
  job_id text,
  result_id text,
  result_revision integer,
  geometry_hash text,
  result_identity_status text not null default 'REQUEST_ONLY' check (result_identity_status in ('REQUEST_ONLY', 'CURRENT', 'UNKNOWN')),
  issue_type text not null check (issue_type in ('recognition_failed', 'coordinate_correction', 'kml_failed')),
  issue_summary text not null check (length(issue_summary) between 1 and 1200),
  resolution_summary text,
  owner text,
  progress_status text not null default 'NEW' check (progress_status in ('NEW', 'TRIAGED', 'IN_PROGRESS', 'BLOCKED', 'VERIFIED', 'CLOSED')),
  blocker text,
  delivery_status text not null default 'UNKNOWN' check (delivery_status in ('UNKNOWN', 'PASS', 'FAIL', 'BLOCKED', 'NOT_APPLICABLE')),
  problem_resolution_status text not null default 'UNKNOWN' check (problem_resolution_status in ('UNKNOWN', 'PASS', 'FAIL', 'BLOCKED', 'NOT_APPLICABLE')),
  peer_validation_status text not null default 'UNKNOWN' check (peer_validation_status in ('UNKNOWN', 'PASS', 'FAIL', 'BLOCKED', 'NOT_APPLICABLE')),
  production_status text not null default 'UNKNOWN' check (production_status in ('UNKNOWN', 'PASS', 'FAIL', 'BLOCKED', 'NOT_APPLICABLE')),
  evidence_scope text,
  sample_count integer not null default 0 check (sample_count >= 0),
  crs_evidence_status text not null default 'UNKNOWN' check (crs_evidence_status in ('UNKNOWN', 'WORKING_ASSUMPTION', 'CONFIRMED')),
  geometry_representation text not null default 'UNKNOWN' check (geometry_representation in ('UNKNOWN', 'POLYGON', 'MULTIPOINT', 'LINESTRING')),
  original_artifact_status text not null default 'NOT_SAVED' check (original_artifact_status in ('NOT_SAVED', 'AUTHORIZED_REFERENCE', 'UNKNOWN')),
  golden_ref text,
  commit_sha text,
  receipt_ref text,
  created_at timestamptz not null default statement_timestamp(),
  updated_at timestamptz not null default statement_timestamp(),
  constraint coordinate_cases_result_identity_all_or_none check (
    (result_id is null and result_revision is null and geometry_hash is null)
    or (length(result_id) > 0 and result_revision > 0 and geometry_hash ~ '^sha256:[0-9a-f]{64}$')
  ),
  constraint coordinate_cases_commit_sha_format check (commit_sha is null or commit_sha ~ '^[0-9a-f]{40}$')
);

create table if not exists public.coordinate_case_evidence (
  evidence_id uuid primary key default gen_random_uuid(),
  case_id uuid not null references public.coordinate_cases(case_id) on delete cascade,
  evidence_type text not null,
  status text not null default 'UNKNOWN' check (status in ('UNKNOWN', 'PASS', 'FAIL', 'BLOCKED', 'NOT_APPLICABLE')),
  evidence_scope text,
  sample_count integer not null default 0 check (sample_count >= 0),
  reference_type text not null check (reference_type in ('GOLDEN', 'COMMIT', 'RECEIPT', 'REQUEST', 'RESULT')),
  reference_value text not null check (length(reference_value) between 1 and 500),
  created_at timestamptz not null default statement_timestamp()
);

create index if not exists coordinate_cases_request_idx on public.coordinate_cases(recognition_request_id);
create index if not exists coordinate_cases_result_idx on public.coordinate_cases(result_id, result_revision, geometry_hash);
create index if not exists coordinate_cases_progress_idx on public.coordinate_cases(progress_status, created_at desc);
create index if not exists coordinate_case_evidence_case_idx on public.coordinate_case_evidence(case_id, created_at desc);

alter table public.coordinate_cases enable row level security;
alter table public.coordinate_case_evidence enable row level security;
revoke all on table public.coordinate_cases from anon, authenticated;
revoke all on table public.coordinate_case_evidence from anon, authenticated;
revoke all on sequence public.coordinate_cases_case_number_seq from anon, authenticated;
grant select, insert, update, delete on table public.coordinate_cases to service_role;
grant select, insert, update, delete on table public.coordinate_case_evidence to service_role;
grant usage, select on sequence public.coordinate_cases_case_number_seq to service_role;

create or replace function private.admin_validate_coordinate_case_identity(
  p_recognition_request_id uuid,
  p_result_id text,
  p_result_revision integer,
  p_geometry_hash text
) returns text
language sql
security definer
set search_path = ''
stable
as $$
  select case
    when not exists (
      select 1 from private.coordinate_recognition_commits c
      where c.recognition_request_id = p_recognition_request_id
    ) then 'NOT_FOUND'
    when exists (
      select 1 from private.coordinate_recognition_commits c
      where c.recognition_request_id = p_recognition_request_id
        and c.authority_result_id = p_result_id
        and c.authority_result_revision = p_result_revision
        and c.authority_geometry_hash = p_geometry_hash
    ) then 'CURRENT'
    else 'CONFLICT'
  end;
$$;

revoke all on function private.admin_validate_coordinate_case_identity(uuid, text, integer, text) from public, anon, authenticated;
grant usage on schema private to service_role;
grant execute on function private.admin_validate_coordinate_case_identity(uuid, text, integer, text) to service_role;

create or replace function public.admin_validate_coordinate_case_identity(
  p_recognition_request_id uuid,
  p_result_id text,
  p_result_revision integer,
  p_geometry_hash text
) returns text
language sql
security invoker
set search_path = ''
stable
as $$
  select private.admin_validate_coordinate_case_identity(
    p_recognition_request_id,
    p_result_id,
    p_result_revision,
    p_geometry_hash
  );
$$;

revoke all on function public.admin_validate_coordinate_case_identity(uuid, text, integer, text) from public, anon, authenticated;
grant execute on function public.admin_validate_coordinate_case_identity(uuid, text, integer, text) to service_role;
