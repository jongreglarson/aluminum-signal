-- 3-month ≈ 63 trading days, 9-month ≈ 189 trading days
-- rows preceding counts exclude the current row, so 62/188 preceding + current = 63/189 total
select
    price_date,
    close_price,

    avg(close_price) over (
        order by price_date
        rows between 62 preceding and current row
    ) as ma_3m,

    avg(close_price) over (
        order by price_date
        rows between 188 preceding and current row
    ) as ma_9m,

    -- track actual window sizes so the mart can flag periods with insufficient history
    count(*) over (
        order by price_date
        rows between 62 preceding and current row
    ) as days_in_3m_window,

    count(*) over (
        order by price_date
        rows between 188 preceding and current row
    ) as days_in_9m_window

from {{ ref('stg_aluminum_prices') }}
