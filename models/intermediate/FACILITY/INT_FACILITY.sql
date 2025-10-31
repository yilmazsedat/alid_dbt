{{ config(materialized="table") }}

with
    facility_part1(
        j_baci,
        j_babi,
        j_bagt,
        j_baec,
        j_bayc,
        j_bcgl,
        j_baz,
        original,
        j_crdc,
        j_bcdl
    ) as (
        select
            facility."J_BACI" j_baci,
            coalesce(cams."J_BABI", facility."J_BABI") j_babi,
            facility."J_BAGT",
            misc."J_BAEC",
            misc."J_BAYC",
            misc."J_BCGL",
            'EUR' "J_BAZC",
            case
                when "J_SGMC" = 'M'
                then facility."J_CRI_" * "J_SGLP"
                else facility."J_CRI_" / "J_SGLP"
            end - case
                when "J_SGMC" = 'M'
                then facility."J_BBW_" * "J_SGLP"
                else facility."J_BBW_" / "J_SGLP"
            end
            - coalesce(
                case
                    when "J_SGMC" = 'M'
                    then sum("J_BGA_" * "J_SGLP")
                    else sum("J_BGA_" / "J_SGLP")
                end,
                0.00
            ) original,
            facility."J_CRDC",
            "J_BCDL"
        from {{ ref("ST_ADWB") }} binder
        join {{ ref("ST_ADWC") }} chapter on chapter."J_IZXI" = binder."J_ICWI"
        join {{ ref("ST_ADAMS") }} facility on facility."J_IPPI" = chapter."J_IZYI"
        join {{ ref("ST_ADAMI") }} misc on facility."J_IPPI" = misc."J_IPPI"
        left outer join {{ ref("ST_CAMS") }} cams on (cams."J_BACI" = facility."J_BACI")
        left outer join
            {{ ref("INT_DEAL") }} adalm
            on adalm."J_BFCI" = facility."J_BACI"
            and length(trim(facility."J_BACI")) > 1
        join
            {{ ref("ST_SYXM") }} syxm
            on (
                facility."J_BAZC" = syxm."J_SGIC"
                and syxm."J_ICKC" = 'EUR'
                and syxm."J_ICJC" = 'NETHERLAND'
            )
        where
            binder."J_ICYC" <> '6'
            and binder."J_ICYC" <> '5'
            and chapter."J_IZZC" = 'CA'
            and binder."J_IWGI" <> 'TEMPLATEE'
            and binder."ADACDL" = 'N'  -- ARCHIVE Y/N  
            and chapter."J_KUTC" <> '1'  -- CHAPTER DELETED?  
            and length(trim(facility."J_BACI")) > 3
            and misc."J_BAEC" <> 'GR'
        group by
            facility."J_BACI",
            cams."J_BABI",
            facility."J_BABI",
            facility."J_BAGT",
            misc."J_BAEC",
            misc."J_BAYC",
            misc."J_BCGL",
            facility."J_CRI_",
            facility."J_BBW_",
            facility."J_CRDC",
            syxm."J_SGMC",
            syxm."J_SGLP",
            "J_BCDL"
    ),
    facility_part2(
        j_baci,
        j_babi,
        j_bagt,
        j_baec,
        j_bayc,
        j_bcgl,
        j_baz,
        original,
        j_crdc,
        j_bcdl
    ) as (
        select
            cams."J_BACI" j_baci,
            cams."J_BABI" j_babi,
            cams."J_BAGT",
            cami."J_BAEC",
            cami."J_BAYC",
            cami."J_BCGL",
            'EUR' j_bazc,
            amt.original original,
            "J_CRDC",
            "J_BCDL"
        from {{ ref("ST_CAMS") }} cams
        inner join {{ ref("ST_CAMI") }} cami on cams."J_BACI" = cami."J_BACI"
        inner join {{ ref("INT_ORIGINAL") }} amt on amt.j_baci = cams."J_BACI"
        where cami."J_BCAC" <> 'B' and cami."J_BAEC" <> 'GR' and cami."J_BAEC" <> 'WO'
    )

select *
from facility_part1
union all
select *
from facility_part2
