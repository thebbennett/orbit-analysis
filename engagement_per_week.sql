with 

metrics_engagement_per_contact_per_week as (
    select * from {{ref('metrics_engagement_per_contact_per_week')}}
),

base as (
    select

        date_week,

        sum(engagement) as engagement

    from metrics_engagement_per_contact_per_week

    group by 1
)

select * from base
