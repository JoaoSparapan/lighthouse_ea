with
    fonte_contas as (
        select
            cast(num_conta as int) as id_conta
            , cast(cod_cliente as int) as id_cliente_conta
            , cast(cod_agencia as int) as id_agencia_conta
            , cast(cod_colaborador as int) as id_colaborador_conta
            , cast(tipo_conta as string) as tipo_conta
            , cast(saldo_total as numeric) as saldo_total_conta
            , cast(saldo_disponivel as numeric) as saldo_disponivel_conta
            , cast(data_abertura as timestamp) as data_abertura_conta
            , cast(data_ultimo_lancamento as timestamp) as data_ultimo_lancamento_conta
        from {{ source('erp', 'contas') }}
    )

select * from fonte_contas