-- Reject new Supabase Auth users whose email address is in the ottawa.ca domain.
-- Existing users are not changed. Because Bus and ERO share one Supabase project,
-- run this migration only once, then enable the function as the Before User Created hook.

create or replace function public.hook_block_ottawa_email_signup(event jsonb)
returns jsonb
language plpgsql
as $$
declare
  signup_email text := lower(trim(coalesce(event -> 'user' ->> 'email', '')));
begin
  if signup_email like '%@ottawa.ca' then
    return jsonb_build_object(
      'error', jsonb_build_object(
        'http_code', 403,
        'message', 'City of Ottawa email addresses cannot be used to create an account. Use a personal email address.'
      )
    );
  end if;

  return '{}'::jsonb;
end;
$$;

grant usage on schema public to supabase_auth_admin;
grant execute on function public.hook_block_ottawa_email_signup(jsonb) to supabase_auth_admin;
revoke execute on function public.hook_block_ottawa_email_signup(jsonb) from anon, authenticated, public;
