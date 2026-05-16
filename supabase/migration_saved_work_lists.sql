alter table public.user_profiles
  add column if not exists weekday_blocks text[] not null default '{}',
  add column if not exists saturday_blocks text[] not null default '{}',
  add column if not exists sunday_blocks text[] not null default '{}',
  add column if not exists work_assignments jsonb not null default '{}'::jsonb,
  add column if not exists work_same_both_weeks boolean not null default true,
  add column if not exists saved_shuttles text[] not null default '{}';

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'user_profiles'
      and column_name = 'primary_block'
  ) then
    update public.user_profiles
    set weekday_blocks = case
      when primary_block is not null and array_length(weekday_blocks, 1) is null then array[upper(primary_block)]
      else weekday_blocks
    end;
  end if;
end $$;

update public.user_profiles
set work_assignments = jsonb_build_object(
  'week1_monday', to_jsonb(weekday_blocks),
  'week1_tuesday', to_jsonb(weekday_blocks),
  'week1_wednesday', to_jsonb(weekday_blocks),
  'week1_thursday', to_jsonb(weekday_blocks),
  'week1_friday', to_jsonb(weekday_blocks),
  'week1_saturday', to_jsonb(saturday_blocks),
  'week1_sunday', to_jsonb(sunday_blocks),
  'week2_monday', to_jsonb(weekday_blocks),
  'week2_tuesday', to_jsonb(weekday_blocks),
  'week2_wednesday', to_jsonb(weekday_blocks),
  'week2_thursday', to_jsonb(weekday_blocks),
  'week2_friday', to_jsonb(weekday_blocks),
  'week2_saturday', to_jsonb(saturday_blocks),
  'week2_sunday', to_jsonb(sunday_blocks)
)
where work_assignments = '{}'::jsonb;

update public.user_profiles
set work_same_both_weeks = false
where coalesce(work_assignments->'week1_monday', '[]'::jsonb) is distinct from coalesce(work_assignments->'week2_monday', '[]'::jsonb)
  or coalesce(work_assignments->'week1_tuesday', '[]'::jsonb) is distinct from coalesce(work_assignments->'week2_tuesday', '[]'::jsonb)
  or coalesce(work_assignments->'week1_wednesday', '[]'::jsonb) is distinct from coalesce(work_assignments->'week2_wednesday', '[]'::jsonb)
  or coalesce(work_assignments->'week1_thursday', '[]'::jsonb) is distinct from coalesce(work_assignments->'week2_thursday', '[]'::jsonb)
  or coalesce(work_assignments->'week1_friday', '[]'::jsonb) is distinct from coalesce(work_assignments->'week2_friday', '[]'::jsonb)
  or coalesce(work_assignments->'week1_saturday', '[]'::jsonb) is distinct from coalesce(work_assignments->'week2_saturday', '[]'::jsonb)
  or coalesce(work_assignments->'week1_sunday', '[]'::jsonb) is distinct from coalesce(work_assignments->'week2_sunday', '[]'::jsonb);

alter table public.user_profiles
  drop column if exists primary_block,
  drop column if exists primary_bus,
  drop column if exists preferred_shuttle_day;
