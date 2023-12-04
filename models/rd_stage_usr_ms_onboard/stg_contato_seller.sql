with 
    source_stg_contato_seller as (
        select 
            CAST(id_contato as bigint) as id_contato
            , CAST(tx_email as string) as tx_email
            , CAST(nm_nome_contato as string) as nm_nome_contato
            , CAST(tx_telefone as string) as tx_telefone
            , CAST(ds_tipo as string) as ds_tipo
            , CAST(op_type as string) as op_type
            , dt_process_stage
            , CAST(0 as string) as sk_sistema_origem
            , CAST(0 as string) as DH_ATUALIZ_DBT
        from {{ source('onb_sellers', 'tb_contato_seller') }}
 )

select * from source_stg_contato_seller