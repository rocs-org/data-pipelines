{{config(tags=['nowcast', 'time-series'])}}

select
    v.user_id,
    v.type,
    v.source,
    max(v.date) over (partition by v.user_id, v."type", v."source" order by v.date range between interval '55 day' preceding and current row) as window_end,
    min(v.date) over (partition by v.user_id, v."type", v."source" order by v.date range between interval '55 day' preceding and current row) as window_start,
    avg(v.value_minus_mean) over (partition by v.user_id, v."type", v."source" order by v.date range between interval '55 day' preceding and current row) as mean,
    stddev(v.value_minus_mean) over (partition by v.user_id, v."type", v."source" order by v.date range between interval '55 day' preceding and current row) as std,
    count(v.value_minus_mean) over (partition by v.user_id, v."type", v."source" order by v.date range between interval '55 day' preceding and current row) as data_count
from
    {{ref('vitals_standardized_by_daily_aggregates')}} v
where
        v.type in (65, 9)
order by v.user_id, v.type, window_end