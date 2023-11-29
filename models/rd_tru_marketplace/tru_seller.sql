with 

stg_seller as (

    select * from {{ ref('stg_seller') }}

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
        , cast(21 as bigint) as sk_sistema_origem
        -- , sysdate dh_atualiz_dbt
        
    from stg_seller

)

select * from renamed
