with 

stg_contrato as (

    select * from {{ ref('stg_contrato') }}

),

renamed as (

    select
         CAST(id_contrato as bigint) as id_contrato
        , CAST(ds_tipo as string) as ds_tipo
        , CAST(ds_versao as string) as ds_versao
        , CAST(dt_criacao as string) as dt_criacao
        , CAST(nm_nome_arquivo as string) as nm_nome_arquivo 
        , CAST(op_type as string) as op_type
        , dt_process_stage
        , cast(21 as bigint) as sk_sistema_origem
        -- , sysdate dh_atualiz_dbt
    from stg_contrato

)

select * from renamed