create extension if not exists pgcrypto;

create table if not exists public.live_lookup_feedback (
  id uuid primary key default gen_random_uuid(),
  issue_type text check (issue_type is null or issue_type in ('incorrect_bus_number')),
  lookup_type text not null check (lookup_type in ('bus', 'block')),
  lookup_value text not null check (length(trim(lookup_value)) > 0),
  block text,
  reported_bus_number text not null check (reported_bus_number ~ '^[0-9]{4}$'),
  correct_bus_number text check (correct_bus_number is null or correct_bus_number ~ '^[0-9]{4}$'),
  comment text check (comment is null or length(trim(comment)) > 0),
  live_source text,
  location_text text,
  lookup_generated_at timestamptz,
  created_at timestamptz not null default timezone('utc', now())
);

create index if not exists live_lookup_feedback_created_at_idx
  on public.live_lookup_feedback (created_at desc);

alter table public.live_lookup_feedback enable row level security;

alter table public.live_lookup_feedback alter column issue_type drop not null;
alter table public.live_lookup_feedback alter column correct_bus_number drop not null;
alter table public.live_lookup_feedback alter column comment drop not null;

alter table public.live_lookup_feedback drop constraint if exists live_lookup_feedback_issue_type_check;
alter table public.live_lookup_feedback add constraint live_lookup_feedback_issue_type_check
  check (issue_type is null or issue_type in ('incorrect_bus_number'));

alter table public.live_lookup_feedback drop constraint if exists live_lookup_feedback_correct_bus_number_check;
alter table public.live_lookup_feedback add constraint live_lookup_feedback_correct_bus_number_check
  check (correct_bus_number is null or correct_bus_number ~ '^[0-9]{4}$');

alter table public.live_lookup_feedback drop constraint if exists live_lookup_feedback_comment_check;
alter table public.live_lookup_feedback add constraint live_lookup_feedback_comment_check
  check (comment is null or length(trim(comment)) > 0);
