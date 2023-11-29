with 
    source_stg_contrato as (
        select 
        *
        from {{ source('onb_sellers', 'tb_contato') }}
 )

select * from source_stg_contrato

