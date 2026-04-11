alter table public.user_profiles
  add column if not exists weekday_blocks text[] not null default '{}',
  add column if not exists saturday_blocks text[] not null default '{}',
  add column if not exists sunday_blocks text[] not null default '{}',
  add column if not exists saved_shuttles text[] not null default '{}';

update public.user_profiles
set weekday_blocks = case
  when primary_block is not null and array_length(weekday_blocks, 1) is null then array[upper(primary_block)]
  else weekday_blocks
end;

alter table public.user_profiles
  drop column if exists primary_block,
  drop column if exists primary_bus,
  drop column if exists preferred_shuttle_day;
