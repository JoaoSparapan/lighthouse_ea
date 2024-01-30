with
    stg_colaboradores as (
        select
            id_colaborador
            , nome_completo_colaborador
        from {{ ref('stg_erp__colaboradores') }}
    )

    , stg_colaborador_agencia as (
        select
            *
        from {{ ref('stg_erp__colaborador_agencia') }}
    )

    , joined_table as (
        select
            stg_colaboradores.id_colaborador
            , stg_colaborador_agencia.id_agencia
            , stg_colaboradores.nome_completo_colaborador
        from stg_colaboradores
        left join stg_colaborador_agencia on stg_colaboradores.id_colaborador = stg_colaborador_agencia.id_colaborador
    )

select * from joined_table