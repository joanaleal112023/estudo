with 

stg_contrato as (

    select * from {{ ref('stg_contrato') }}

),

renamed as (

    select
        CAST(id_contrato as bigint) as sk_contrato
        , CAST(id_contrato as bigint) as cd_contrato
        , CAST(ds_tipo as string) as ds_tipo
        , CAST(ds_versao as string) as ds_versao
        , CAST(dt_criacao as string) as dh_aceite_contrato
        , CAST(nm_nome_arquivo as string) as nm_nome_arquivo 
        , op_type
        , dt_process_stage
        , sk_sistema_origem        
        , CAST(0 as string) as fl_ultima_versao
        , CAST(0 as string) as dt_data_de
        , CAST(0 as string) as dt_data_ate
        , CAST(0 as string) as DH_INCLUSAO_DW
        , CAST(0 as string) as DH_ATUALIZ_DW
    
    from stg_contrato

)

select * from renamed

