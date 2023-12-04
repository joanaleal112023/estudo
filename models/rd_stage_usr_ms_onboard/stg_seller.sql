with 

source as (

    select * from {{ source('onb_sellers', 'tb_seller') }}

),

renamed as (

    select
        id_seller
        , nm_cnpj
        , id_endereco
        , id_contato
        , id_loja
        , ds_status_seller
        , dt_atualizacao
        , dt_criacao
        , fl_email_enviado
        , id_gcertifica
        , CAST(op_type as string) as op_type
        , dt_process_stage
        , CAST(0 as string) as sk_sistema_origem
        , CAST(0 as string) as DH_ATUALIZ_DBT
       
    from source

)

select * from renamed