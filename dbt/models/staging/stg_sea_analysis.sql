{{ config(materialized='view') }}

with econdata as ( select *,
                   row_number() over (partition by year, country) as rn
                   from {{ source ('staging', 'rawdata') }}
                )

select
    country,
    econ_indicator,
    cast(year as integer) as year,
    cast(value as numeric) as value
from econdata