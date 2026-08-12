-- Require existing City of Ottawa email accounts to choose a new password.
-- Run this migration once in each Supabase project used by the Bus and ERO sites.

create table if not exists public.password_reset_requirements (
  user_id uuid primary key references auth.users(id) on delete cascade,
  reason text not null default 'ottawa_email_password_safety',
  required_at timestamptz not null default timezone('utc', now()),
  completed_at timestamptz
);

alter table public.password_reset_requirements enable row level security;

revoke all on table public.password_reset_requirements from anon, authenticated;
grant select on table public.password_reset_requirements to authenticated;

drop policy if exists "Users can view their own password reset requirement"
  on public.password_reset_requirements;
create policy "Users can view their own password reset requirement"
on public.password_reset_requirements
for select
using (auth.uid() = user_id);

create or replace function public.complete_password_reset_requirement()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
begin
  if old.encrypted_password is distinct from new.encrypted_password then
    update public.password_reset_requirements
    set completed_at = timezone('utc', now())
    where user_id = new.id
      and completed_at is null;
  end if;
  return new;
end;
$$;

revoke all on function public.complete_password_reset_requirement() from public;

drop trigger if exists complete_required_password_reset on auth.users;
create trigger complete_required_password_reset
after update of encrypted_password on auth.users
for each row
execute function public.complete_password_reset_requirement();

insert into public.password_reset_requirements (user_id)
select id
from auth.users
where lower(email) like '%@ottawa.ca'
on conflict (user_id) do nothing;

-- Revoke refresh sessions for affected users. Existing access tokens can remain
-- valid until their configured JWT expiry, but cannot be refreshed afterward.
delete from auth.sessions as session
using public.password_reset_requirements as requirement
where session.user_id = requirement.user_id
  and requirement.completed_at is null;
