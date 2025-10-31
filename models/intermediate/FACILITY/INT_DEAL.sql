{{ config(materialized="table") }}

select "J_BFCI", "J_CQJC", "J_IDNT", "J_IDOC", "J_KGCC", "J_BGA_"
from dev_staging."ST_ADALM" adalm
join dev_staging."ST_ADWP" adwp on adalm."J_IPNI" = adwp."J_IDII"
where adwp."J_KGCC" = '0' and adalm."J_CQJC" = '900' and length(trim("J_BFCI")) > 2
