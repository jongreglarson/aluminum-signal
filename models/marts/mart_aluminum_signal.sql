with base as (
    select
        price_date,
        close_price,
        round(ma_30d, 2)                                    as ma_30d,
        round(ma_90d, 2)                                    as ma_90d,
        round(close_price - ma_90d, 2)                      as price_vs_ma_90d,
        round((close_price - ma_90d) / ma_90d * 100, 2)     as price_vs_ma_90d_pct,
        round(ma_30d - ma_90d, 2)                           as ma_spread,
        case
            when days_in_90d_window < 15
                then 'INSUFFICIENT DATA'
            when close_price < ma_30d and close_price < ma_90d
                then 'BUY'
            when close_price > ma_30d and close_price > ma_90d
                then 'HOLD'
            else
                'NEUTRAL'
        end as signal,
        days_in_30d_window,
        days_in_90d_window
    from {{ ref('int_aluminum_moving_averages') }}
),

most_recent_date as (
    select max(price_date) as max_date from base
),

daily as (
    select 'DAILY' as granularity, b.*
    from base b
    cross join most_recent_date m
    where b.price_date >= dateadd(day, -13, m.max_date)
),

weekly as (
    select 'WEEKLY' as granularity, b.*
    from base b
    qualify
        row_number() over (partition by date_trunc('week', price_date) order by price_date desc) = 1
        and dense_rank() over (order by date_trunc('week', price_date) desc) <= 13
),

monthly as (
    select 'MONTHLY' as granularity, b.*
    from base b
    qualify
        row_number() over (partition by date_trunc('month', price_date) order by price_date desc) = 1
        and dense_rank() over (order by date_trunc('month', price_date) desc) <= 13
)

select *, convert_timezone('America/New_York', current_timestamp()) as refreshed_at from daily
union all
select *, convert_timezone('America/New_York', current_timestamp()) as refreshed_at from weekly
union all
select *, convert_timezone('America/New_York', current_timestamp()) as refreshed_at from monthly
order by granularity, price_date desc
