select installed_at::date                                as "Day of install",
       date_part('day', cs.created_at - us.installed_at) as "Activity Date",
       coalesce(100 * count(distinct us.id) / (first_value(count(distinct us.id)) over (partition by installed_at)),
                100)                                     as "% of active"
from public.user us
         left join client_session cs
                   on us.id = cs.user_id
group by installed_at, 2;