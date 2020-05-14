create table date_intervals
(
    id        integer not null,
    rev       integer not null,
    date_from timestamp,
    date_to   timestamp,
    constraint table_name_pk
        primary key (id, rev)
);

insert into date_intervals (id, rev, date_from, date_to)
with scm as (
    select *, '31-12-9999 23:59:59.999999'::timestamp as MaxCollatingDate
    from test_data_20200113)
select id,
       rev,
       coalesce(scm.updated_at, scm.created_at)::timestamp                                                  as date_from,
       coalesce(lead(updated_at, 1) over (partition by created_at order by updated_at)::timestamp, MaxCollatingDate::timestamp) as date_to
from scm;