begin;

create table if not exists public.spatial_result_shares (
  share_id text primary key,
  schema_version text not null check (schema_version = 'shared_spatial_result_v1'),
  source_result_id text not null,
  source_result_revision integer not null check (source_result_revision >= 1),
  source_geometry_hash text not null,
  manager_capability_hash text not null check (manager_capability_hash ~ '^[a-f0-9]{64}$'),
  access_scope text not null check (access_scope in ('RECIPIENT_ONLY', 'ANYONE_WITH_LINK')),
  usage_permission text not null check (usage_permission in ('VIEW_ONLY', 'ALLOW_EDIT')),
  recipient_capability_hash text check (recipient_capability_hash ~ '^[a-f0-9]{64}$'),
  recipient_bound_at timestamptz,
  snapshot_hash text not null check (snapshot_hash ~ '^[a-f0-9]{64}$'),
  snapshot jsonb not null,
  snapshot_bytes integer not null check (snapshot_bytes > 0 and snapshot_bytes <= 524288),
  vertex_count integer not null check (vertex_count > 0 and vertex_count <= 5000),
  created_at timestamptz not null default now(),
  expires_at timestamptz,
  revoked_at timestamptz,
  constraint spatial_result_shares_share_id_shape check (share_id ~ '^[A-Za-z0-9_-]{32}$'),
  constraint spatial_result_shares_expiry_order check (expires_at is null or expires_at > created_at),
  constraint spatial_result_shares_recipient_binding_pair check (
    (recipient_capability_hash is null and recipient_bound_at is null)
    or (recipient_capability_hash is not null and recipient_bound_at is not null)
  ),
  constraint spatial_result_shares_recipient_scope check (
    access_scope = 'RECIPIENT_ONLY' or recipient_capability_hash is null
  ),
  constraint spatial_result_shares_permission_snapshot_binding check (
    snapshot ->> 'accessScope' = access_scope
    and snapshot ->> 'usagePermission' = usage_permission
  ),
  constraint spatial_result_shares_revocation_order check (revoked_at is null or revoked_at >= created_at)
);

create index if not exists spatial_result_shares_active_expiry_idx
  on public.spatial_result_shares (expires_at)
  where revoked_at is null;

create index if not exists spatial_result_shares_manager_capability_hash_idx
  on public.spatial_result_shares (manager_capability_hash);

create index if not exists spatial_result_shares_created_at_idx
  on public.spatial_result_shares (created_at);

alter table public.spatial_result_shares enable row level security;
revoke all on table public.spatial_result_shares from public, anon, authenticated;
grant select, insert, update, delete on table public.spatial_result_shares to service_role;

comment on table public.spatial_result_shares is
  'Immutable server-derived Shared Spatial Result v1 snapshots. Public clients have no direct table access.';
comment on column public.spatial_result_shares.snapshot is
  'Immutable shared_spatial_result_v1 content. Only revoked_at may change after insert.';
comment on column public.spatial_result_shares.snapshot_hash is
  'Server-derived SHA-256 of deterministic immutable shared snapshot content.';
comment on column public.spatial_result_shares.recipient_capability_hash is
  'Hash-only first-active-browser capability for RECIPIENT_ONLY access. Never exposed publicly.';

create or replace function public.prevent_spatial_result_share_content_update()
returns trigger
language plpgsql
security invoker
set search_path = ''
as $$
begin
  if new.share_id is distinct from old.share_id
    or new.schema_version is distinct from old.schema_version
    or new.source_result_id is distinct from old.source_result_id
    or new.source_result_revision is distinct from old.source_result_revision
    or new.source_geometry_hash is distinct from old.source_geometry_hash
    or new.manager_capability_hash is distinct from old.manager_capability_hash
    or new.access_scope is distinct from old.access_scope
    or new.usage_permission is distinct from old.usage_permission
    or new.snapshot_hash is distinct from old.snapshot_hash
    or new.snapshot is distinct from old.snapshot
    or new.snapshot_bytes is distinct from old.snapshot_bytes
    or new.vertex_count is distinct from old.vertex_count
    or new.created_at is distinct from old.created_at
    or new.expires_at is distinct from old.expires_at then
    raise exception 'spatial_result_share_snapshot_immutable';
  end if;
  if old.recipient_capability_hash is not null
    and (new.recipient_capability_hash is distinct from old.recipient_capability_hash
      or new.recipient_bound_at is distinct from old.recipient_bound_at) then
    raise exception 'spatial_result_share_recipient_binding_terminal';
  end if;
  if (new.recipient_capability_hash is null) is distinct from (new.recipient_bound_at is null) then
    raise exception 'spatial_result_share_recipient_binding_invalid';
  end if;
  if old.revoked_at is not null and new.revoked_at is distinct from old.revoked_at then
    raise exception 'spatial_result_share_revocation_terminal';
  end if;
  return new;
end;
$$;

revoke all on function public.prevent_spatial_result_share_content_update() from public, anon, authenticated;
grant execute on function public.prevent_spatial_result_share_content_update() to service_role;

do $migration$
declare
  existing_trigger record;
begin
  select
    trigger_definition.tgrelid,
    trigger_definition.tgfoid,
    trigger_definition.tgtype,
    trigger_definition.tgenabled,
    trigger_definition.tgqual,
    trigger_definition.tgnargs,
    trigger_definition.tgattr
  into existing_trigger
  from pg_catalog.pg_trigger as trigger_definition
  where trigger_definition.tgrelid = 'public.spatial_result_shares'::regclass
    and trigger_definition.tgname = 'spatial_result_shares_immutable_content'
    and not trigger_definition.tgisinternal;

  if found then
    if existing_trigger.tgrelid <> 'public.spatial_result_shares'::regclass
      or existing_trigger.tgfoid <> 'public.prevent_spatial_result_share_content_update()'::regprocedure
      or existing_trigger.tgtype <> 19
      or existing_trigger.tgenabled <> 'O'
      or existing_trigger.tgqual is not null
      or existing_trigger.tgnargs <> 0
      or existing_trigger.tgattr <> ''::int2vector then
      raise exception 'spatial_result_shares_immutable_content_trigger_mismatch';
    end if;
  else
    execute $trigger$
      create trigger spatial_result_shares_immutable_content
      before update on public.spatial_result_shares
      for each row execute function public.prevent_spatial_result_share_content_update()
    $trigger$;
  end if;
end;
$migration$;

create or replace function public.bind_spatial_share_recipient(
  p_share_id text,
  p_recipient_capability_hash text
)
returns boolean
language plpgsql
security definer
set search_path = ''
as $$
declare
  bound_hash text;
  share_scope text;
begin
  if p_recipient_capability_hash !~ '^[a-f0-9]{64}$' then
    return false;
  end if;

  update public.spatial_result_shares
  set recipient_capability_hash = p_recipient_capability_hash,
      recipient_bound_at = now()
  where share_id = p_share_id
    and access_scope = 'RECIPIENT_ONLY'
    and recipient_capability_hash is null
    and revoked_at is null
    and (expires_at is null or expires_at > now());

  select access_scope, recipient_capability_hash
  into share_scope, bound_hash
  from public.spatial_result_shares
  where share_id = p_share_id
    and revoked_at is null
    and (expires_at is null or expires_at > now());

  return share_scope = 'ANYONE_WITH_LINK'
    or (share_scope = 'RECIPIENT_ONLY' and bound_hash = p_recipient_capability_hash);
end;
$$;

revoke all on function public.bind_spatial_share_recipient(text, text) from public, anon, authenticated;
grant execute on function public.bind_spatial_share_recipient(text, text) to service_role;

commit;
