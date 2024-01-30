with
    fonte_proposta_credito as (
        select
            cast(cod_proposta as int) as id_proposta
            , cast(cod_cliente as int) as id_cliente
            , cast(cod_colaborador as int) as id_colaborador
            , cast(data_entrada_proposta as timestamp) as data_entrada_proposta
            , cast(taxa_juros_mensal as numeric) as taxa_juros_mes_proposta
            , cast(valor_proposta as numeric) as valor_total_proposta
            , cast(valor_financiamento as numeric) as valor_financiamento_proposta
            , cast(valor_entrada as numeric) as valor_entrada_proposta
            , cast(valor_prestacao as numeric) as valor_prestacao_proposta
            , cast(quantidade_parcelas as int) as quantidade_parcelas_proposta
            , cast(carencia as int) as prazo_carencia_proposta
            , cast(status_proposta as string) as status_proposta
        from {{ source('erp', 'propostas_credito') }}
    )

select * from fonte_proposta_credito