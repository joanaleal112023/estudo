with 

stg_contrato_seller as (

    select * from {{ ref('stg_contrato_seller') }}

),

renamed as (

    select
        CAST(id_contrato_seller as bigint) as sk_contrato_seller
        , CAST(id_contrato_seller as bigint) as cd_contrato_seller
        , dt_criacao
        , CAST(id_seller as bigint) as cd_seller
        , CAST(id_contrato as bigint) as cd_contrato
        , CAST(0 as string) as fl_ultima_versao
        , CAST(0 as string) as dt_data_de
        , CAST(0 as string) as dt_data_ate
        , CAST(op_type as string) as op_type
        , CAST(0 as string) as sk_sistema_origem
        , CAST(0 as string) as DH_INCLUSAO_DW
        , CAST(0 as string) as DH_ATUALIZ_DW

    from stg_contrato_seller

)

select * from renamed
