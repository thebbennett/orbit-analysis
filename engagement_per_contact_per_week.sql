with 

full_action_history as (
    select * from {{ref('full_action_history')}}
),

all_weeks as (
  select * from {{ref('all_weeks')}}
),

contacts as (
    select * from {{ref('contacts')}}
),

full_action_history_with_date_week as (
  select 
  	date_trunc('week', action_at) as date_week,
  
  	vanid,
  
  	sum(engagement_points) engagement_points_per_week
  
  
  from full_action_history

  group by 1, 2
  
 ),

 contact_first_week as (
     select 
        vanid,
        min(date_week) as first_week

     from full_action_history_with_date_week
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

engagement_points_all_weeks as (
    select 

        contacts_all_weeks.date_week,

        full_action_history_with_date_week.vanid,

        coalesce(full_action_history_with_date_week.engagement_points_per_week, 0) as engagement_points_per_week 
    
    from contacts_all_weeks

    left join full_action_history_with_date_week
        on contacts_all_weeks.date_week = full_action_history_with_date_week.date_week
        and contacts_all_weeks.vanid = full_action_history_with_date_week.vanid
),

engagement_points_all_weeks_with_week_count as (
    select 
        *,
        row_number() over (
            partition by vanid
            order by date_week desc
        ) as week_count

    from engagement_points_all_weeks
),


calculation as (
    select
        *,
        engagement_points_per_week * power(0.9, week_count - 1) as decayed_points,
        sum(decayed_points) over (
            partition by vanid
            order by date_week
            rows between unbounded preceding and current row
        ) as decayed_points_cumulative,
        decayed_points_cumulative / nullif(power(0.9, week_count - 1), 0) as score_this_week_calc
    from engagement_points_all_weeks_with_week_count
),

final as (
    select 

        date_week,

        vanid,

        score_this_week_calc as engagement

    from calculation
)

select * from final
