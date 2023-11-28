with 
    source_stg_contato_seller as (
        select *
        from {{ source('onb_sellers', 'tb_contato_seller') }}
 )

select * from source_stg_contato_seller
