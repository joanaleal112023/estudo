with 
    stg_contato_seller as (
        select 
            CAST(id_contato as bigint) as sk_contato_seller
            , CAST(id_contato as bigint) as cd_contato_seller
            , CAST(tx_email as string) as email_seller
            , CAST(nm_nome_contato as string) as nm_nome_seller
            , CAST(tx_telefone as string) as telefone_seller
            , CAST(ds_tipo as string) as ds_tipo
            , CAST(0 as string) as fl_ultima_versao
            , CAST(0 as string) as dt_data_de
            , CAST(0 as string) as dt_data_ate
            , CAST(0 as string) as sk_sistema_origem
            , CAST(op_type as string) as op_type
            , dt_process_stage
            , CAST(0 as string) as DH_INCLUSAO_DW
            , CAST(0 as string) as DH_ATUALIZ_DW

        from {{ ref('stg_contato_seller') }}
 )

select * from stg_contato_seller
