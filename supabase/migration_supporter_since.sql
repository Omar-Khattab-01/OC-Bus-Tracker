alter table public.support_subscriptions
  add column if not exists first_paid_at timestamptz;
