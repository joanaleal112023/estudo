with 

source as (

    select * from {{ source('onb_sellers', 'tb_contrato') }}

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
        , CAST(0 as string) as sk_sistema_origem
        , CAST(0 as string) as DH_ATUALIZ_DW
    from source

)

select * from renamed