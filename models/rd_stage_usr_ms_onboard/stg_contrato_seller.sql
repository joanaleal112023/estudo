with 

source as (

    select * from {{ source('onb_sellers', 'tb_contrato_seller') }}

),

renamed as (

    select
        id_contrato_seller,
        dt_criacao,
        id_seller,
        id_contrato,
        op_type,
        dt_process_stage,
        partition_ano_mes
        , cast(21 as bigint) as sk_sistema_origem
        -- , sysdate dh_atualiz_dbt

    from source

)

select * from renamed