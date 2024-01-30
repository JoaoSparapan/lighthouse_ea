with
    fonte_clientes as (
        select
            cast(cod_cliente as int) as id_cliente
            , cast(primeiro_nome as string) as nome_cliente
            , cast(ultimo_nome as string) as sobrenome_cliente
            , cast(tipo_cliente as string) as tipo_cliente
            , cast(data_inclusao as timestamp) as data_inclusao_cliente
            , cast(data_nascimento as date) as data_nascimento_cliente
            , cast(endereco as string) as endereco_cliente
            , cast(cep as string) as cep_cliente
        from {{ source('erp', 'clientes') }}
    )

    , agg_name as (
        select
            *
            , TRIM(CONCAT(COALESCE(nome_cliente,' '),' ',COALESCE(sobrenome_cliente,' '))) AS nome_completo_cliente
        from fonte_clientes
    )

select * from agg_name