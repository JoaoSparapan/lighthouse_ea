with
    stg_colaboradores as (
        select
            id_colaborador
            , nome_completo_colaborador
        from {{ ref('stg_erp__colaboradores') }}
    )

select * from stg_colaboradores