select
    o.order_id,
    o.user_id
from {{ ref('stg_orders') }} as o
left join {{ ref('stg_users') }} as u
    on o.user_id = u.user_id
where u.user_id is null
