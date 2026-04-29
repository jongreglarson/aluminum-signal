with moving_avgs as (
    select * from {{ ref('int_aluminum_moving_averages') }}
),

with_prev as (
    select
        *,
        lag(ma_3m) over (order by price_date) as prev_ma_3m,
        lag(ma_9m) over (order by price_date) as prev_ma_9m
    from moving_avgs
)

select
    price_date,
    close_price,
    round(ma_3m, 2)  as ma_3m,
    round(ma_9m, 2)  as ma_9m,

    -- how far price sits above/below the long-term average
    round(close_price - ma_9m, 2)                   as price_vs_ma_9m,
    round((close_price - ma_9m) / ma_9m * 100, 2)   as price_vs_ma_9m_pct,

    -- gap between the two moving averages (positive = bullish momentum)
    round(ma_3m - ma_9m, 2) as ma_spread,

    case
        when days_in_9m_window < 189
            then 'INSUFFICIENT DATA'
        when prev_ma_3m < prev_ma_9m and ma_3m >= ma_9m
            then 'GOLDEN CROSS - BUY'
        when prev_ma_3m > prev_ma_9m and ma_3m <= ma_9m
            then 'DEATH CROSS - HOLD'
        when ma_3m > ma_9m
            then 'BUY'
        else
            'HOLD'
    end as signal,

    days_in_3m_window,
    days_in_9m_window

from with_prev
order by price_date desc
