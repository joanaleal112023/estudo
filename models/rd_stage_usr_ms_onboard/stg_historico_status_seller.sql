with 

source as (

    select * from {{ source('onb_sellers', 'tb_historico_status_seller') }}

),

renamed as (

    select
        id_historico_status_seller,
        id_seller,
        nm_status,
        nm_journey,
        nm_observacao,
        dt_criacao,
        op_type,
        dt_process_stage,
        partition_ano_mes

    from source

)

select * from renamed
