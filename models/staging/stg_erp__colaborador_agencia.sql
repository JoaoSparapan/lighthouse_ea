with
    fonte_colaborador_agencia as (
        select
            cast(cod_colaborador as int) as id_colaborador
            , cast(cod_agencia as int) as id_agencia
        from {{ source('erp', 'colaborador_agencia') }}
    )

select * from fonte_colaborador_agencia