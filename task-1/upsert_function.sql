create or replace function upsert_new_date_intervals() returns void
as
$$
begin
    insert into date_intervals (id, rev, date_from, date_to)
    with scm as (
        select *, '31-12-9999 23:59:59.999999'::timestamp as MaxCollatingDate
        from test_data)
    select id,
           rev,
           coalesce(scm.updated_at, scm.created_at)::timestamp as date_from,
           coalesce(lead(updated_at, 1) over (partition by created_at order by updated_at)::timestamp,
                    MaxCollatingDate::timestamp)               as date_to
    from scm
    on conflict (id,rev) do update set (date_from, date_to) = (excluded.date_from, excluded.date_to);

end;
$$
    language plpgsql;