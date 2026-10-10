begin;

do $$
begin
  if exists (
    select 1 from private.indonesia_structured_b_claims
    where status in ('DISPATCHED', 'COMPLETED', 'FAILED_AFTER_PROVIDER', 'OUTCOME_UNKNOWN')
  ) then
    raise exception 'indonesia structured B RC evidence exists; rollback requires an explicit evidence-retention decision';
  end if;
end
$$;

drop function if exists public.close_indonesia_structured_b_single_run(text, text, text);
drop function if exists public.settle_indonesia_structured_b_single_run(text, text, text, uuid, text, text, integer, integer, integer, integer, numeric);
drop function if exists public.dispatch_indonesia_structured_b_single_run(text, text, text, text, uuid, text);
drop function if exists public.claim_indonesia_structured_b_single_run(text, text, text, text, uuid, text, boolean);

drop table if exists private.indonesia_structured_b_claims;
drop table if exists private.indonesia_structured_b_single_runs;
drop table if exists private.indonesia_structured_b_batches;

commit;
