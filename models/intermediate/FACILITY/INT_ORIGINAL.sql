{{ config(materialized="table") }}

select
    "CAMS"."J_BACI" as j_baci,
    case
        when "SYXM"."J_SGMC" = 'M'
        then "CAMS"."J_BBQ_" * "SYXM"."J_SGLP"
        else "CAMS"."J_BBQ_" / "SYXM"."J_SGLP"
    end as original
from "dev_staging"."ST_CAMS" "CAMS"
join "dev_staging"."ST_CAMI" "CAMI" on "CAMI"."J_BACI" = "CAMS"."J_BACI"
left outer join
    "dev_staging"."ST_SYXM" "SYXM"
    on (
        "CAMS"."J_BAZC" = "SYXM"."J_SGIC"
        and "SYXM"."J_ICKC" = 'EUR'
        and "SYXM"."J_ICJC" = 'NETHERLAND'
    )
where "CAMI"."J_BCAC" <> 'B'
