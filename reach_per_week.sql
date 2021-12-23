with 

metrics_reach_per_contact_per_week as (
    select * from {{ref('metrics_reach_per_contact_per_week')}}
),

reach_per_week as (
    select 

        date_week,

        sum(reach) as reach
    
    from metrics_reach_per_contact_per_week

    group by 1

)

select * from reach_per_week
