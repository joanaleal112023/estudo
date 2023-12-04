with 

stg_seller as (

    select * from {{ ref('stg_seller') }}

),

renamed as (

    select
        CAST(id_seller as bigint) as sk_seller
        , CAST(id_seller as bigint) as cd_seller
        , CAST(nm_cnpj as string) as nm_cnpj
        , CAST(id_endereco as bigint) as cd_endereco
        , CAST(id_contato as bigint) as cd_contato
        , CAST(id_loja as bigint) as cd_loja
        , CAST(ds_status_seller as string) as ds_status_seller
        , dt_atualizacao
        , dt_criacao
        , fl_email_enviado
        , id_gcertifica
        , CAST(0 as string) as sk_sistema_origem
        , CAST(op_type as string) as op_type
        , dt_process_stage
        , CAST(0 as string) as DH_INCLUSAO_DW
        , CAST(0 as string) as DH_ATUALIZ_DW
        
    from stg_seller

)

select * from renamed