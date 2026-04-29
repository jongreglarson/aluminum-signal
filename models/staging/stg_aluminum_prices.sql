select
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    loaded_at
from {{ source('raw', 'raw_aluminum_prices') }}
where close_price is not null
  and price_date is not null
