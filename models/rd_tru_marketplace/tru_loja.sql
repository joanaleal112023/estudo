with 

stg_loja as (

    select * from {{ ref('stg_loja') }}

),

renamed as (

    select
        CAST(id_loja as bigint) as sk_loja
        , CAST(id_loja as bigint) as cd_loja
        , nm_fantasia
        , nm_nome_loja
        , fl_produtos_especiais
        , nm_inscricao_estadual
        , tx_website
        , quantidade_skus
        , CAST(0 as string) as fl_ultima_versao
        , CAST(0 as string) as dt_data_de
        , CAST(0 as string) as dt_data_ate
        , CAST(op_type as string) as op_type
        , CAST(0 as string) as sk_sistema_origem
        , CAST(0 as string) as DH_INCLUSAO_DW
        , CAST(0 as string) as DH_ATUALIZ_DW

    from stg_loja

)

select * from renamed
