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
