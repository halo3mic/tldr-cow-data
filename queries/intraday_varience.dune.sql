WITH
    prices_with_lag AS (
        SELECT
            p.minute,
            p.contract_address AS token,
            p.price,
            LAG(p.price) OVER (
                PARTITION BY p.contract_address
                ORDER BY p.minute
            ) AS prev_price
        FROM prices.usd p
        WHERE p.blockchain = '{{chain}}'
          AND p.minute >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
          AND p.minute < DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
          and contains(split('{{token_whitelist}}', ','), lower(cast(p.contract_address as varchar))) 
    ),
    minute_returns AS (
      SELECT
        DATE_TRUNC('day', minute) AS day,
        DATE_TRUNC('month', minute) as month,
        token,
        ln(price / prev_price) AS log_ret
      FROM prices_with_lag
    ),
    daily_realized_var AS (
      SELECT
        day,
        token,
        SUM(POWER(log_ret, 2)) AS realized_variance, 
        count(1) sample_size
      FROM minute_returns
      WHERE log_ret IS NOT NULL
      GROUP BY day, token
    )

select *
from daily_realized_var
order by token, day