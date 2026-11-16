SELECT 
menu_id,try_to_number(r_id) as r_id,
try_to_number(f_id) as f_id,
cuisine,
try_to_decimal(price, 10, 2) as price
FROM {{ source('raw', 'menu') }} WHERE try_to_number(r_id) is not null
