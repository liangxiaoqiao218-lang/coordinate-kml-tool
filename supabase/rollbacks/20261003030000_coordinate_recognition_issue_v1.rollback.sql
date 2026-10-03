begin;

-- This rollback is deliberately non-destructive: issue rows and additive
-- metadata columns are retained so no case evidence is silently discarded.
do $$
begin
  if exists (
    select 1
    from public.coordinate_cases
    where progress_status = 'RESOLVED'
  ) then
    raise exception 'coordinate_recognition_issue_v1 rollback requires an explicit decision for RESOLVED rows';
  end if;
end
$$;

drop index if exists public.coordinate_cases_automatic_request_unique_idx;

alter table public.coordinate_cases
  drop constraint if exists coordinate_cases_source_check,
  drop constraint if exists coordinate_cases_failure_stage_check,
  drop constraint if exists coordinate_cases_coordinate_type_check,
  drop constraint if exists coordinate_cases_runtime_commit_check,
  drop constraint if exists coordinate_cases_error_code_check,
  drop constraint if exists coordinate_cases_resolved_evidence_check,
  drop constraint if exists coordinate_cases_progress_status_check,
  add constraint coordinate_cases_progress_status_check check (
    progress_status in ('NEW', 'TRIAGED', 'IN_PROGRESS', 'BLOCKED', 'VERIFIED', 'CLOSED')
  );

-- Keep source, failure_stage, error_code, coordinate_type, runtime_commit and
-- occurred_at. The pre-change application ignores them, while retaining them
-- avoids deleting evidence collected before a runtime rollback.

commit;
