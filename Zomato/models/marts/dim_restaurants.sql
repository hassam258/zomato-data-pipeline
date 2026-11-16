SELECT  
    restaurant_id,
    restaurant_name,
    city,   
    rating,
    rating_count,
    cost_for_two,
    cuisine

FROM {{ref('stg_restaurants')}} r