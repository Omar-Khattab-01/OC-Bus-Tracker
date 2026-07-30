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
