select
    contract_address token,
    decimals,
    "day",
    price,
    price_high,
    price_low
from prices.usd_daily
where 
    blockchain = '{{chain}}'
    and "day" >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
    and "day" <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
    and contains(split('{{token_whitelist}}', ','), lower(cast(contract_address as varchar)))
