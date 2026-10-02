alter table public.coordinate_cases
  add column if not exists source text not null default 'ADMIN',
  add column if not exists failure_stage text not null default 'UNKNOWN',
  add column if not exists error_code text,
  add column if not exists coordinate_type text not null default 'UNKNOWN',
  add column if not exists runtime_commit text,
  add column if not exists occurred_at timestamptz;

alter table public.coordinate_cases
  drop constraint if exists coordinate_cases_source_check,
  add constraint coordinate_cases_source_check check (source in ('UNKNOWN', 'COORDINATE_IMAGE_UPLOAD', 'MANUAL_ASSISTANCE', 'ADMIN')),
  drop constraint if exists coordinate_cases_failure_stage_check,
  add constraint coordinate_cases_failure_stage_check check (failure_stage in ('UNKNOWN', 'ACQUISITION', 'VALIDATION', 'FINALIZATION', 'MANUAL_ASSISTANCE')),
  drop constraint if exists coordinate_cases_coordinate_type_check,
  add constraint coordinate_cases_coordinate_type_check check (coordinate_type in ('UNKNOWN', 'DECIMAL_DEGREES', 'DMS', 'UTM', 'PROJECTED', 'MIXED')),
  drop constraint if exists coordinate_cases_runtime_commit_check,
  add constraint coordinate_cases_runtime_commit_check check (runtime_commit is null or runtime_commit ~ '^[0-9a-f]{40}$'),
  drop constraint if exists coordinate_cases_progress_status_check,
  add constraint coordinate_cases_progress_status_check check (progress_status in ('NEW', 'TRIAGED', 'IN_PROGRESS', 'BLOCKED', 'VERIFIED', 'RESOLVED', 'CLOSED')),
  drop constraint if exists coordinate_cases_resolved_evidence_check,
  add constraint coordinate_cases_resolved_evidence_check check (
    progress_status <> 'RESOLVED'
    or (
      commit_sha is not null
      and receipt_ref is not null
      and golden_ref is not null
      and sample_count > 0
      and evidence_scope is not null
      and problem_resolution_status = 'PASS'
      and peer_validation_status = 'PASS'
    )
  );

create unique index if not exists coordinate_cases_request_unique_idx
  on public.coordinate_cases(recognition_request_id);

comment on column public.coordinate_cases.source is 'coordinate_recognition_issue_v1 non-sensitive issue source';
comment on column public.coordinate_cases.failure_stage is 'Safe pipeline stage only; UNKNOWN when evidence is insufficient';
comment on column public.coordinate_cases.error_code is 'Redacted machine error code; no provider response or user content';
comment on column public.coordinate_cases.coordinate_type is 'Coarse coordinate type or UNKNOWN; never raw coordinates';
comment on column public.coordinate_cases.runtime_commit is 'Runtime commit observed when the issue record was built';
comment on column public.coordinate_cases.occurred_at is 'Issue event time supplied by the server';
