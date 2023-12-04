with 

source as (

    select * from {{ source('onb_sellers', 'tb_loja') }}

),

renamed as (

    select
        id_loja
        , nm_fantasia
        , nm_nome_loja
        , fl_produtos_especiais
        , nm_inscricao_estadual
        , tx_website
        , quantidade_skus
        , CAST(op_type as string) as op_type
        , dt_process_stage
        , CAST(0 as string) as sk_sistema_origem
        , CAST(0 as string) as DH_ATUALIZ_DBT

    from source

)

select * from renamed
