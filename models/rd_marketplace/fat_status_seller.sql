with

    dim_status_seller as (
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
        from {{ref('dim_status_seller')}} 
    ),


    dim_seller as (
        select 
            sk_seller
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
            , a.email_seller
            , a.nm_nome_seller
            , a.telefone_seller
            , max((CASE WHEN b.cd_seller IS NULL THEN 0 ELSE 1 END)) completeQuantity
            , sum((CASE WHEN b.nm_status in ('WAITING_APPROVE') THEN 1 ELSE 0 END)) waitingApproveQuantity
            , sum((CASE WHEN b.nm_status in ('APPROVED') THEN 1 ELSE 0 END)) ApprovedQuantity
            , sum((CASE WHEN b.nm_status in ('INCOMPLETE') THEN 1 ELSE 0 END)) IncompleteQuantity
            , sum((CASE WHEN b.nm_status in ('ONBOARD') THEN 1 ELSE 0 END)) OnboardQuantity
            , sum((CASE WHEN b.nm_status in ('DONE') THEN 1 ELSE 0 END)) DoneQuantity
            , sum((CASE WHEN b.nm_status in ('REJECTED') THEN 1 ELSE 0 END)) RejectedQuantity
            , min((SELECT min(dt_criacao) FROM {{ref('dim_status_seller')}} a WHERE nm_status in ('INCOMPLETE') AND a.cd_SELLER = b.cd_seller)) incomplete_time
            , max((SELECT dt_criacao FROM {{ref('dim_status_seller')}} a WHERE nm_status in ('COMPLETE') AND a.cd_SELLER = b.cd_seller)) complete_time
            , max((SELECT dt_criacao FROM {{ref('dim_status_seller')}} a WHERE nm_status in ('WAITING_APPROVE') AND a.cd_seller = b.cd_seller)) waitingapprove_time
            , max((SELECT dt_criacao FROM {{ref('dim_status_seller')}} a WHERE nm_status in ('APPROVED') AND a.cd_seller = b.cd_seller)) approved_time
            , max((SELECT dt_criacao FROM {{ref('dim_status_seller')}} a WHERE nm_status in ('ONBOARD') AND a.cd_seller = b.cd_seller)) onboard_time
            , max((SELECT dt_criacao FROM {{ref('dim_status_seller')}} a WHERE nm_status in ('DONE') AND a.cd_seller = b.cd_seller)) done_time
            , max((SELECT dt_criacao FROM {{ref('dim_status_seller')}} a WHERE nm_status in ('INTEGRATION') AND a.cd_seller = b.cd_seller)) integration_time
            , max((SELECT dt_criacao FROM {{ref('dim_status_seller')}} a WHERE nm_status in ('REJECTED') AND a.cd_seller = b.cd_seller)) rejected_time
        from {{ref('dim_seller')}} a
        left join {{ref('dim_status_seller')}} b on a.cd_seller = b.cd_seller
        where 1=1
        group by 
            sk_seller
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
            , a.email_seller
            , a.nm_nome_seller
            , a.telefone_seller


    ),

    dim_loja as (
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
        from {{ref('dim_loja')}}

    ),

    dim_contrato_seller as (
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
            , ds_tipo
            , ds_versao
            , dh_aceite_contrato
            , nm_nome_arquivo
        from {{ref('dim_contrato_seller')}}

    ),


    final as (
        select 
            a.sk_seller
            , a.cd_seller
            , b.sk_loja
            , b.cd_loja
            , a.ds_status_seller
            , a.incomplete_time
            , a.complete_time
            , a.waitingapprove_time
            , a.approved_time
            , a.onboard_time
            , a.done_time
            , a.integration_time
            , a.rejected_time
            , (complete_time - incomplete_time) delta_complete
            , (waitingapprove_time - complete_time) delta_waitingapprove
            , (approved_time - waitingapprove_time) delta_approved
            , (onboard_time - approved_time) delta_onboard
            , (done_time - onboard_time) delta_done
            , (integration_time - done_time) delta_integration
            , (rejected_time - waitingapprove_time) delta_reject
        from dim_seller a 
        left join dim_loja b on a.cd_loja = b.cd_loja
        where 1=1
        --group by 
           -- a.sk_seller
           -- , a.cd_seller
           -- , b.sk_historico_status_seller
           -- , b.cd_historico_status_seller
           -- , c.sk_loja
           -- , c.cd_loja
           -- , b.incomplete_time
           -- , b.complete_time
           -- , b.waitingapprove_time
           -- , b.approved_time
           -- , b.onboard_time
           -- , b.done_time
           -- , b.integration_time
           -- , b.rejected_time
    )
    select *
    from final

