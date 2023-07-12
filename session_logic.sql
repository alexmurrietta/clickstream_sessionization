SET TIME ZONE 'UTC';

drop table if exists schema_name.engagement_sessions;
create table if not exists schema_name.engagement_sessions as

with clickstream as (

  select
  expert_id
  ,section_id
  ,timestamp
  ,cs.service_type
  ,cs.application_channel
  ,area
  ,action
  ,object
  ,screen
  ,widget_name
  from schema.clickstream_customersuccess as cs
  where year = '2022'
  and expert_id is not null
  and action = 'engaged'
  
  union all 
  
  select
  expert_id
  ,section_id
  ,timestamp
  ,cs.service_type
  ,cs.application_channel
  ,area
  ,action
  ,object
  ,screen
  ,widget_name
  from schema.clickstream_customersuccess as cs
  where year = '2023'
  and expert_id is not null
  and action = 'engaged'

)

-- Session Definition Creation
,engagement_in_out as (

  select
  expert_id
  ,section_id
  
  -- If the current section_id is different from the previous one, then it must be the start of a session. Also, if it's the first action by an Expert in an Engagement, then it must be the start of a session
  ,case 
    when section_id != lag(section_id, 1) over(partition by expert_id order by timestamp) then timestamp
    when section_id is not null and row_number() over(partition by expert_id, section_id order by timestamp) = 1 then timestamp
    end as session_start_ts
    
   -- If the current section_id is different from the next one, then it must be the end of a session. Also, if it's the last action by an Expert in an engagement, then it must be the end of a session
  ,case 
    when section_id != lead(section_id, 1) over(partition by expert_id order by timestamp) then timestamp
    when section_id is not null and row_number() over(partition by expert_id, section_id order by timestamp desc) = 1 then timestamp
    end as session_end_ts
    
  ,timestamp
  ,service_type
  ,application_channel
  ,area
  ,action
  ,object
  ,screen
  ,widget_name
  from clickstream
)

-- Getting rid of all beacons in between the start and end beacons
,consolidation as (

  select
  expert_id
  ,section_id
  ,date(timestamp) as event_date
  ,session_start_ts as min_open_ts
  
  -- This is a hacky way of getting the right session_end_ts at the end of a window partition
  ,case
    when session_start_ts is not null and session_end_ts is not null then session_end_ts
    else lead(session_end_ts, 1) over(partition by expert_id, section_id order by timestamp) end as max_open_ts
  ,service_type
  ,application_channel
  from in_out_2
  where session_start_ts is not null or session_end_ts is not null
)

,engagement_info as (
  select 
  eng.section_id
  ,eng.engagement_type
  ,eng.engagement_status
  from schema.engagement as eng
  left join schema.employee as emp
    on eng.expert_key = emp.employee_key
)

,sku as (
select section_id, max(completed_SKU) as completed_SKU, max(createdon) as engagement_created_date, max(engagement_complete_ts) as engagement_completed_date from schema.engagements group by 1
union 
select section_id, max(completed_SKU) as completed_SKU, max(createdon) as engagement_created_date, max(engagement_complete_ts) as engagement_completed_date  from schema.engagements group by 1
)

,sync_stats as (

    select
    section_id
    ,field_name
    ,field_name
    from schema.tax
    where section_id is not null

    union

    select
    section_id
    ,field_name
    ,field_name
    from schema.tax
    where section_id is not null
    )

,role_names as (

  select * from
  (
      select
      section_id,
      expert_id,
      role,
      row_number() over (partition by section_id, expert_id order by assignment_start_datetime) dedupe --selects first role that was assigned to a expert_id within an engagement
      from
      schema.expert asgn
      where created_date >= date('2021-12-01')
      ) a 
  where dedupe = 1
  )


,final as (
  select *
  
  -- The timestamp clauses below are a nightmare to look at, but all it is, is subtracting the max_open and min_open to get a session length. The unix_timestamp is the format required for hive and is bulky compared to a simple date_diff
  ,round((unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')))/60,0) as minutes_engagement_opened
  ,unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')) as seconds_engagement_opened
  ,case 
    when round((unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')))/60,0) = 0 then 'a. 0'
    when round((unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')))/60,0) > 0 
          and round((unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')))/60,0) <= 15 then 'b. 1-15'
    when round((unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')))/60,0) > 15 
          and round((unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')))/60,0) <= 30  then 'c. 16-30'
    when round((unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')))/60,0) > 30 
          and round((unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')))/60,0) <= 45 then 'd. 31-45'
    when round((unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')))/60,0) > 45 
          and round((unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')))/60,0) <= 60 then 'e. 46-60'
    when round((unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')))/60,0) > 60 then 'f. 61+'
    end as minutes_engagement_opened_buckets
  ,case 
    when unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')) = 0 then 'a. 0'
    when unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')) > 0 
          and unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')) <= 15 then 'b. 1-15'
    when unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')) > 15 
          and unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')) <= 30  then 'c. 16-30'
    when unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')) > 30 
          and unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')) <= 45 then 'd. 31-45'
    when unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')) > 45 
          and unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')) <= 60 then 'e. 46-60'
    when unix_timestamp(date_format(max_open_ts, 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(date_format(min_open_ts, 'yyyy-MM-dd HH:mm:ss')) > 60 then 'f. 61+'
    end as seconds_engagement_opened_buckets
  from consolidation
  where (min_open_ts is not null and max_open_ts is not null)
  and section_id like '%cto%'
)

select *

-- used to get how many times the engagement was open by an Expert on a given day. You can take the max() of this to get the number of times it was opened.
,row_number() over(partition by expert_id, section_id, event_date order by event_date) as open_num
from (
  select distinct f.*
  ,s.completed_sku
  ,s.engagement_created_date
  ,s.engagement_completed_date 
  ,ei.engagement_type
  ,ei.engagement_status
  ,sy.field_name
  ,sy.field_name
  ,r.role
  from final as f
  left join engagement_info as ei
    on f.section_id = ei.section_id
  left join sku as s
    on ei.section_id = s.section_id
  left join sync_stats as sy
    on f.section_id = sy.section_id
  left join role_names as r
    on f.section_id = r.section_id
    and f.expert_id = r.expert_id
) as a
