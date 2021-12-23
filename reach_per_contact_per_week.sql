with 

full_reach_history as (
    select * from {{ref('full_reach_history')}}
),

all_weeks as (
    select * from {{ref('all_weeks')}}
),

 contact_first_week as (
     select 
        vanid,
        min(date_week) as first_week

     from full_reach_history
     group by 1
 ),

 contacts_all_weeks as (
     select
        vanid, 

        all_weeks.date_week

     from contact_first_week

     left join all_weeks
         on all_weeks.date_week >= contact_first_week.first_week
),

reach_points_all_weeks as (
    select 

        contacts_all_weeks.date_week,

        full_reach_history.vanid,

        coalesce(full_reach_history.reach_per_week, 0) as reach_per_week 
    
    from contacts_all_weeks

    left join full_reach_history
        on contacts_all_weeks.date_week = full_reach_history.date_week
        and contacts_all_weeks.vanid = full_reach_history.vanid
),

reach_points_all_weeks_with_week_count as (
    select 
        *,
        row_number() over (
            partition by vanid
            order by date_week desc
        ) as week_count

    from reach_points_all_weeks
),


calculation as (
    select
        *,
        reach_per_week * power(0.8, week_count - 1) as decayed_points,
        sum(decayed_points) over (
            partition by vanid
            order by date_week
            rows between unbounded preceding and current row
        ) as decayed_points_cumulative,
        decayed_points_cumulative / nullif(power(0.8, week_count - 1), 0) as score_this_week_calc
    from reach_points_all_weeks_with_week_count
),

final as (
    select 

        date_week,

        vanid,

        score_this_week_calc as reach

    from calculation
)

select * from final
