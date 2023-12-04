with 

source as (

    select * from {{ source('onb_sellers', 'tb_contrato_seller') }}

),

renamed as (

    select
        id_contrato_seller
        , dt_criacao
        , id_seller
        , id_contrato
        , CAST(op_type as string) as op_type
        , dt_process_stage
        , CAST(0 as string) as sk_sistema_origem
        , CAST(0 as string) as DH_ATUALIZ_DBT

    from source

)

select * from renamed