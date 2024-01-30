with
    fonte_colaboradores as (
        select
            cast(cod_colaborador as int) as id_colaborador
            , cast(primeiro_nome as string) as nome_colaborador
            , cast(ultimo_nome as string) as sobrenome_colaborador
        from {{ source('erp', 'colaboradores') }}
    )

    , agg_name as (
        select
            *
            , TRIM(CONCAT(COALESCE(nome_colaborador,' '),' ',COALESCE(sobrenome_colaborador,' '))) AS nome_completo_colaborador
        from fonte_colaboradores
    )

select * from agg_name