-- Calendar-day windows: averages all trading days that fall within
-- the preceding 30 or 90 calendar days of each price_date
select
    price_date,
    close_price,

    avg(close_price) over (
        order by price_date
        range between interval '30 days' preceding and current row
    ) as ma_30d,

    avg(close_price) over (
        order by price_date
        range between interval '90 days' preceding and current row
    ) as ma_90d,

    count(*) over (
        order by price_date
        range between interval '30 days' preceding and current row
    ) as days_in_30d_window,

    count(*) over (
        order by price_date
        range between interval '90 days' preceding and current row
    ) as days_in_90d_window

from {{ ref('stg_aluminum_prices') }}
