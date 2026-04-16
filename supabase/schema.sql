create table if not exists public.user_profiles (
  user_id uuid primary key references auth.users(id) on delete cascade,
  display_name text,
  weekday_blocks text[] not null default '{}',
  saturday_blocks text[] not null default '{}',
  sunday_blocks text[] not null default '{}',
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
