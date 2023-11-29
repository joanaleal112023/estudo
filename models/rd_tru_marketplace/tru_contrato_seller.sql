with 

stg_contrato_seller as (

    select * from {{ ref('stg_contrato_seller') }}

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

    from stg_contrato_seller

)

select * from renamed