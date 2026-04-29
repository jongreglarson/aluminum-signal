with moving_avgs as (
    select * from {{ ref('int_aluminum_moving_averages') }}
)

select
    price_date,
    close_price,
    round(ma_3m, 2)  as ma_3m,
    round(ma_9m, 2)  as ma_9m,

    -- how far price sits above/below the long-term average
    round(close_price - ma_9m, 2)                   as price_vs_ma_9m,
    round((close_price - ma_9m) / ma_9m * 100, 2)   as price_vs_ma_9m_pct,

    -- gap between the two moving averages
    round(ma_3m - ma_9m, 2) as ma_spread,

    case
        when days_in_9m_window < 189
            then 'INSUFFICIENT DATA'
        when close_price < ma_3m and close_price < ma_9m
            then 'BUY'
        when close_price > ma_3m and close_price > ma_9m
            then 'HOLD'
        else
            'NEUTRAL'
    end as signal,

    days_in_3m_window,
    days_in_9m_window

from moving_avgs
order by price_date desc
