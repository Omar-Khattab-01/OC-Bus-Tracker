create table if not exists public.user_profiles (
  user_id uuid primary key references auth.users(id) on delete cascade,
  display_name text,
  weekday_blocks text[] not null default '{}',
  saturday_blocks text[] not null default '{}',
  sunday_blocks text[] not null default '{}',
  work_assignments jsonb not null default '{}'::jsonb,
  saved_shuttles text[] not null default '{}',
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = timezone('utc', now());
  return new;
end;
$$;

drop trigger if exists user_profiles_set_updated_at on public.user_profiles;
create trigger user_profiles_set_updated_at
before update on public.user_profiles
for each row
execute function public.set_updated_at();

alter table public.user_profiles enable row level security;

drop policy if exists "Users can view their own profile" on public.user_profiles;
create policy "Users can view their own profile"
on public.user_profiles
for select
using (auth.uid() = user_id);

drop policy if exists "Users can insert their own profile" on public.user_profiles;
create policy "Users can insert their own profile"
on public.user_profiles
for insert
with check (auth.uid() = user_id);

drop policy if exists "Users can update their own profile" on public.user_profiles;
create policy "Users can update their own profile"
on public.user_profiles
for update
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create table if not exists public.live_bus_paddles (
  bus_number text primary key,
  block text not null,
  paddle_id text,
  service_day text,
  route text,
  trip_number text,
  headsign text,
  start_time text,
  end_time text,
  verified_at timestamptz not null default timezone('utc', now())
);

create index if not exists live_bus_paddles_verified_at_idx
  on public.live_bus_paddles (verified_at desc);

alter table public.live_bus_paddles enable row level security;

create extension if not exists pgcrypto;

create table if not exists public.whatsapp_booking_board_uploads (
  id uuid primary key default gen_random_uuid(),
  sender text not null,
  recipient text not null,
  board_key text not null,
  label text not null,
  source_name text not null,
  pdf_base64 text not null,
  status text not null default 'queued',
  error text,
  created_at timestamptz not null default timezone('utc', now()),
  processed_at timestamptz,
  constraint whatsapp_booking_board_uploads_status_check
    check (status in ('queued', 'processing', 'processed', 'error'))
);

create index if not exists whatsapp_booking_board_uploads_status_created_idx
  on public.whatsapp_booking_board_uploads (status, created_at);

create index if not exists whatsapp_booking_board_uploads_sender_status_created_idx
  on public.whatsapp_booking_board_uploads (sender, status, created_at);

alter table public.whatsapp_booking_board_uploads enable row level security;

create table if not exists public.bus_defect_access (
  email text primary key,
  role text not null default 'reporter',
  granted_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  constraint bus_defect_access_email_normalized check (email = lower(trim(email))),
  constraint bus_defect_access_role_check check (role in ('viewer', 'reporter', 'admin'))
);

drop trigger if exists bus_defect_access_set_updated_at on public.bus_defect_access;
create trigger bus_defect_access_set_updated_at
before update on public.bus_defect_access
for each row
execute function public.set_updated_at();

create index if not exists bus_defect_access_email_idx
  on public.bus_defect_access (lower(email));

create table if not exists public.bus_defect_reports (
  id uuid primary key default gen_random_uuid(),
  bus_number text not null check (bus_number ~ '^[468][0-9]{3}$'),
  defect_category text not null check (
    defect_category in (
      'farebox',
      'radio',
      'destination_sign',
      'interior_lights',
      'exterior_lights',
      'doors',
      'hvac',
      'wipers_washers',
      'mirrors',
      'seat_issue',
      'ramp_kneeler',
      'cleanliness',
      'safety',
      'other'
    )
  ),
  report_status text not null default 'open' check (report_status in ('open', 'reported', 'addressed', 'solved')),
  control_reported_at timestamptz not null default timezone('utc', now()),
  defect_text text not null check (length(trim(defect_text)) > 0),
  reported_by uuid references auth.users(id) on delete set null,
  reported_by_email text,
  reported_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

alter table public.bus_defect_reports
  add column if not exists control_reported_at timestamptz;

update public.bus_defect_reports
set control_reported_at = coalesce(control_reported_at, reported_at, timezone('utc', now()))
where control_reported_at is null;

alter table public.bus_defect_reports
  alter column control_reported_at set default timezone('utc', now());

alter table public.bus_defect_reports
  alter column control_reported_at set not null;

alter table public.bus_defect_reports
  drop constraint if exists bus_defect_reports_bus_number_check;

alter table public.bus_defect_reports
  add constraint bus_defect_reports_bus_number_check
  check (bus_number ~ '^[468][0-9]{3}$') not valid;

drop trigger if exists bus_defect_reports_set_updated_at on public.bus_defect_reports;
create trigger bus_defect_reports_set_updated_at
before update on public.bus_defect_reports
for each row
execute function public.set_updated_at();

create index if not exists bus_defect_reports_bus_idx
  on public.bus_defect_reports (bus_number, reported_at desc);

create index if not exists bus_defect_reports_reported_at_idx
  on public.bus_defect_reports (reported_at desc);

create or replace function public.is_bus_defect_admin()
returns boolean
language sql
security definer
set search_path = public
stable
as $$
  select coalesce(lower(auth.email()), '') = 'omar.hosam2000@gmail.com'
    or exists (
      select 1
      from public.bus_defect_access access
      where lower(access.email) = coalesce(lower(auth.email()), '')
        and access.role = 'admin'
    );
$$;

create or replace function public.can_access_bus_defects()
returns boolean
language sql
security definer
set search_path = public
stable
as $$
  select public.is_bus_defect_admin()
    or exists (
      select 1
      from public.bus_defect_access access
      where lower(access.email) = coalesce(lower(auth.email()), '')
        and access.role in ('viewer', 'reporter', 'admin')
    );
$$;

create or replace function public.can_report_bus_defects()
returns boolean
language sql
security definer
set search_path = public
stable
as $$
  select public.is_bus_defect_admin()
    or exists (
      select 1
      from public.bus_defect_access access
      where lower(access.email) = coalesce(lower(auth.email()), '')
        and access.role in ('reporter', 'admin')
    );
$$;

alter table public.bus_defect_access enable row level security;
alter table public.bus_defect_reports enable row level security;

drop policy if exists "Bus defect admins can view access" on public.bus_defect_access;
create policy "Bus defect admins can view access"
on public.bus_defect_access
for select
using (public.is_bus_defect_admin());

drop policy if exists "Users can view their own bus defect access" on public.bus_defect_access;
create policy "Users can view their own bus defect access"
on public.bus_defect_access
for select
using (lower(email) = coalesce(lower(auth.email()), ''));

drop policy if exists "Bus defect admins can grant access" on public.bus_defect_access;
create policy "Bus defect admins can grant access"
on public.bus_defect_access
for insert
with check (public.is_bus_defect_admin());

drop policy if exists "Bus defect admins can update access" on public.bus_defect_access;
create policy "Bus defect admins can update access"
on public.bus_defect_access
for update
using (public.is_bus_defect_admin())
with check (public.is_bus_defect_admin());

drop policy if exists "Bus defect admins can revoke access" on public.bus_defect_access;
create policy "Bus defect admins can revoke access"
on public.bus_defect_access
for delete
using (public.is_bus_defect_admin());

drop policy if exists "Authorized users can view bus defects" on public.bus_defect_reports;
create policy "Authorized users can view bus defects"
on public.bus_defect_reports
for select
using (public.can_access_bus_defects());

drop policy if exists "Authorized users can report bus defects" on public.bus_defect_reports;
create policy "Authorized users can report bus defects"
on public.bus_defect_reports
for insert
with check (
  public.can_report_bus_defects()
  and reported_by = auth.uid()
);

drop policy if exists "Bus defect admins can update reports" on public.bus_defect_reports;
create policy "Bus defect admins can update reports"
on public.bus_defect_reports
for update
using (public.is_bus_defect_admin())
with check (public.is_bus_defect_admin());

drop policy if exists "Bus defect admins can delete reports" on public.bus_defect_reports;
create policy "Bus defect admins can delete reports"
on public.bus_defect_reports
for delete
using (public.is_bus_defect_admin());
