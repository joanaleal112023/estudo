with

    tru_contrato_seller as (
        select
        sk_contrato_seller
        , cd_contrato_seller
        , dt_criacao
        , cd_seller
        , cd_contrato
        , fl_ultima_versao
        , dt_data_de
        , dt_data_ate
        , op_type
        , sk_sistema_origem
        , DH_INCLUSAO_DW
        , DH_ATUALIZ_DW
        from {{ref('tru_contrato_seller')}}

    ),

    tru_contrato as (
        select 
        sk_contrato
        , cd_contrato
        , ds_tipo
        , ds_versao
        , dh_aceite_contrato
        , nm_nome_arquivo
        , op_type
        , dt_process_stage
        , sk_sistema_origem
        , fl_ultima_versao
        , dt_data_de
        , dt_data_ate
        , DH_INCLUSAO_DW
        , DH_ATUALIZ_DW
        from {{ref('tru_contrato')}}
    ),

    final as (

        select 
        a.*
        , ds_tipo
        , b.ds_versao
        , b.dh_aceite_contrato
        , b.nm_nome_arquivo
        from tru_contrato_seller a
        left join tru_contrato b on a.cd_contrato = b.cd_contrato
      
    )

select * from final