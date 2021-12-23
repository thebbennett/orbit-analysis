with 

metrics_gravity_per_contact_per_week as (
    select * from {{ref('metrics_gravity_per_contact_per_week')}}
),

base as (
    select 

        date_week,

        sum(gravity) as gravity

    from metrics_gravity_per_contact_per_week

    group by 1
)

select * from base
