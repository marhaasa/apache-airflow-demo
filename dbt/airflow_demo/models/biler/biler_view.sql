
{{ config(materialized='view') }}
select *
from source.biler


