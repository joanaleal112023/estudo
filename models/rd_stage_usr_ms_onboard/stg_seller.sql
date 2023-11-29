with 

source as (

    select * from {{ source('onb_sellers', 'tb_seller') }}

),

renamed as (

    select
        id_seller,
        nm_cnpj,
        id_endereco,
        id_contato,
        id_loja,
        ds_status_seller,
        dt_atualizacao,
        dt_criacao,
        fl_email_enviado,
        id_gcertifica,
        op_type,
        dt_process_stage,
        partition_ano_mes

    from source

)

select * from renamed
