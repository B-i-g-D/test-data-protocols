{{ config(materialized='table') }}

select
  cast(pclass as integer) as pclass,
  count(*) as passengers,
  avg(cast(survived as double)) as survival_rate
from {{ ref('titanic_dataset') }}
group by 1
order by 1
