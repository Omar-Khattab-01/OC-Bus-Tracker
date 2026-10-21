-- All supporter data is server-only. Browser clients receive a status, never billing records.
create table if not exists public.support_devices (
  device_hash text primary key,
  verified_email text,
  updated_at timestamptz not null default now()
);

create table if not exists public.support_customers (
  id text primary key,
  email text,
  updated_at timestamptz not null default now()
);
create index if not exists support_customers_email_idx on public.support_customers(email);

create table if not exists public.support_device_customers (
  device_hash text not null,
  customer_id text not null references public.support_customers(id),
  primary key (device_hash, customer_id)
);

create table if not exists public.support_payments (
  session_id text primary key,
  payment_intent_id text unique,
  customer_id text references public.support_customers(id),
  device_hash text,
  email text,
  amount bigint not null,
  currency text not null,
  paid_at timestamptz not null,
  refunded boolean not null default false
);
create index if not exists support_payments_device_idx on public.support_payments(device_hash);
create index if not exists support_payments_email_idx on public.support_payments(email);

create table if not exists public.support_subscriptions (
  id text primary key,
  customer_id text not null references public.support_customers(id),
  status text not null,
  current_period_end timestamptz,
  cancel_at_period_end boolean not null default false,
  updated_at timestamptz not null default now()
);
create index if not exists support_subscriptions_customer_idx on public.support_subscriptions(customer_id);

alter table public.support_devices enable row level security;
alter table public.support_customers enable row level security;
alter table public.support_device_customers enable row level security;
alter table public.support_payments enable row level security;
alter table public.support_subscriptions enable row level security;

revoke all on public.support_devices, public.support_customers, public.support_device_customers,
  public.support_payments, public.support_subscriptions from anon, authenticated;
grant all on public.support_devices, public.support_customers, public.support_device_customers,
  public.support_payments, public.support_subscriptions to service_role;
