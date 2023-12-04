with 

stg_historico_status_seller as (

    select * from {{ ref('stg_historico_status_seller') }}

),

renamed as (

    select
        CAST(id_historico_status_seller as bigint) as sk_historico_status_seller
        , id_historico_status_seller
        , id_seller
        , nm_status
        , nm_journey
        , nm_observacao
        , dt_criacao
        , CAST(0 as string) as fl_ultima_versao
        , CAST(0 as string) as dt_data_de
        , CAST(0 as string) as dt_data_ate
        , CAST(op_type as string) as op_type
        , CAST(0 as string) as sk_sistema_origem
        , CAST(0 as string) as DH_INCLUSAO_DW
        , CAST(0 as string) as DH_ATUALIZ_DW

    from stg_historico_status_seller

)

select * from renamed
