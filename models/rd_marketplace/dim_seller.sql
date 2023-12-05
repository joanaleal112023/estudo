with

    tru_seller as (
        select
            sk_seller
            , cd_seller
            , nm_cnpj
            , cd_endereco
            , cd_contato
            , cd_loja
            , ds_status_seller
            , dt_atualizacao
            , dt_criacao
            , fl_email_enviado
            , id_gcertifica
            , sk_sistema_origem
            , op_type
            , dt_process_stage
            , DH_INCLUSAO_DW
            , DH_ATUALIZ_DW
        from {{ref('tru_seller')}}

    ),

    tru_contato_seller as (
        select 
            sk_contato
            , cd_contato
            , email_seller
            , nm_nome_seller
            , telefone_seller
            , ds_tipo
            , fl_ultima_versao
            , dt_data_de
            , dt_data_ate
            , sk_sistema_origem
            , op_type
            , dt_process_stage
            , DH_INCLUSAO_DW
            , DH_ATUALIZ_DW
        from {{ref('tru_contato_seller')}}
    ),

    final as (

        select 
            a.sk_seller
            , a.cd_seller
            , a.nm_cnpj
            , a.cd_endereco
            , a.cd_contato
            , a.cd_loja
            , a.ds_status_seller
            , a.dt_atualizacao
            , a.dt_criacao
            , a.fl_email_enviado
            , a.id_gcertifica
            , a.sk_sistema_origem
            , a.dt_process_stage
            , a.DH_INCLUSAO_DW
            , a.DH_ATUALIZ_DW
            , b.email_seller
            , b.nm_nome_seller
            , b.telefone_seller
        from tru_seller a
        left join tru_contato_seller b on a.cd_contato = b.cd_contato
      
    )

select * from final
