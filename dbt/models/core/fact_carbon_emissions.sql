{{ config(materialized='table') }}

with econdata as (
    select *
    from {{ ref('stg_sea_analysis') }}
)

select country, year, value
from 
    econdata
where econ_indicator='emissions'