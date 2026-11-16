SELECT 
    f_id,
    item as food_name,
    initcap(veg_or_non_veg) as veg_or_non_veg
FROM {{ source('raw', 'food') }} WHERE f_id is not null