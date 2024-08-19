{{ 
    config(
        materialized = 'view'
    ) 
}}

select
    *
from mainnet_flashloan

