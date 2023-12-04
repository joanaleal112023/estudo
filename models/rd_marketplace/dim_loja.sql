with

    tru_loja as (
        select
            sk_loja
            , cd_loja
            , nm_fantasia
            , nm_nome_loja
            , fl_produtos_especiais
            , nm_inscricao_estadual
            , tx_website
            , quantidade_skus
            , fl_ultima_versao
            , dt_data_de
            , dt_data_ate
            , op_type
            , sk_sistema_origem
            , DH_INCLUSAO_DW
            , DH_ATUALIZ_DW
        from {{ref('tru_loja')}}

    ),

    final as (

        select 
            *
        from tru_loja
      
    )

select * from final