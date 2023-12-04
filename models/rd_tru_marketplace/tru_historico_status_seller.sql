with 

stg_historico_status_seller as (

    select * from {{ ref('stg_historico_status_seller') }}

),

renamed as (

    select
        CAST(id_historico_status_seller as bigint) as sk_historico_status_seller,
        id_historico_status_seller,
        id_seller,
        nm_status,
        nm_journey,
        nm_observacao,
        dt_criacao,
        op_type,
        dt_process_stage,
        partition_ano_mes
        , cast(21 as bigint) as sk_sistema_origem
        -- , sysdate dh_atualiz_dbt
        
    from stg_historico_status_seller

)

select * from renamed
