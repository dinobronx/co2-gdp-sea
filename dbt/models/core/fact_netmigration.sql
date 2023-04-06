{{ config(materialized='table') }}

with econdata as (
    select *
    from {{ ref('stg_sea_analysis') }}
)

select country, year, value
from 
    econdata
where econ_indicator='net_migration' and year=2020