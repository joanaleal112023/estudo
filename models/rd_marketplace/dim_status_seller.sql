with

    tru_historico_status_seller as (
        select
            sk_historico_status_seller
            , cd_historico_status_seller
            , cd_seller
            , nm_status
            , nm_journey
            , nm_observacao
            , dt_criacao
            , fl_ultima_versao
            , dt_data_de
            , dt_data_ate
            , op_type
            , sk_sistema_origem
            , DH_INCLUSAO_DW
            , DH_ATUALIZ_DW
        from {{ref('tru_historico_status_seller')}}

    ),

    final as (

        select 
            *
        from tru_historico_status_seller
      
    )

select * from final