with 

source as (

    select * from {{ source('onb_sellers', 'tb_loja') }}

),

renamed as (

    select
        id_loja,
        nm_fantasia,
        nm_nome_loja,
        fl_produtos_especiais,
        nm_inscricao_estadual,
        tx_website,
        quantidade_skus,
        op_type,
        dt_process_stage,
        partition_ano_mes

    from source

)

select * from renamed
