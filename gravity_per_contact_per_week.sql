with 

metrics_engagement_per_contact_per_week as (
    select * from {{ref('metrics_engagement_per_contact_per_week')}}
),

metrics_reach_per_contact_per_week as (
    select * from {{ref('metrics_reach_per_contact_per_week')}}
),


join_orbit_model as (
    select

        coalesce(metrics_reach_per_contact_per_week.date_week, metrics_engagement_per_contact_per_week.date_week) as date_week,

        metrics_reach_per_contact_per_week.vanid,

        metrics_engagement_per_contact_per_week.engagement,

        metrics_reach_per_contact_per_week.reach,

        engagement * reach as gravity

    from metrics_reach_per_contact_per_week 

    full outer join metrics_engagement_per_contact_per_week
        on  metrics_reach_per_contact_per_week.vanid = metrics_engagement_per_contact_per_week.vanid
        and metrics_reach_per_contact_per_week.date_week = metrics_engagement_per_contact_per_week.date_week

)

select * from join_orbit_model 
