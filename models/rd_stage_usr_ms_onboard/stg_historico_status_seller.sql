with 

source as (

    select * from {{ source('onb_sellers', 'tb_historico_status_seller') }}

),

renamed as (

    select
        id_historico_status_seller
        , id_seller
        , nm_status
        , nm_journey
        , nm_observacao
        , dt_criacao
        , CAST(op_type as string) as op_type
        , dt_process_stage
        , CAST(0 as string) as sk_sistema_origem
        , CAST(0 as string) as DH_ATUALIZ_DBT
        
    from source

)

select * from renamed
