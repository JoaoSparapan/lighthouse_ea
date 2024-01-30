with
    agencias as (
        select
            *
        from {{ ref('dim_agencias') }}
    )

    , colaboradores as (
        select
            *
        from {{ ref('dim_colaboradores') }}
    )

    , proposta_credito as (
        select
            *
        from {{ ref('dim_proposta_credito') }}
    )

    , clientes_dim as (
        select
            *
        from {{ ref('dim_clientes') }}
    )

    , clientes_propostas_joined as (
        select
            clientes_dim.id_cliente
            , clientes_dim.nome_completo_cliente
            , clientes_dim.tipo_cliente
            , clientes_dim.data_inclusao_cliente
            , clientes_dim.data_nascimento_cliente
            , clientes_dim.endereco_cliente
            , clientes_dim.cep_cliente
            , STRING_AGG(CAST(proposta_credito.id_proposta AS string), ', ') AS id_proposta
        from clientes_dim
        left join proposta_credito on clientes_dim.id_cliente = proposta_credito.id_cliente
        GROUP BY clientes_dim.id_cliente
            , clientes_dim.nome_completo_cliente
            , clientes_dim.tipo_cliente
            , clientes_dim.data_inclusao_cliente
            , clientes_dim.data_nascimento_cliente
            , clientes_dim.endereco_cliente
            , clientes_dim.cep_cliente
    )

    , contas as (
        select
            *
        from {{ ref('dim_contas') }}
    )

    , transacoes as (
        select
            *
        from {{ ref('dim_transacoes') }}
    )

    , colaborador_agencia_joined as (
        select
            colaboradores.id_colaborador
            , agencias.id_agencia
            , colaboradores.nome_completo_colaborador as nome_colaborador
            , agencias.nome_agencia
            , agencias.endereco_agencia
            , agencias.cidade_agencia
            , agencias.uf_agencia
            , agencias.data_abertura_agencia
            , agencias.tipo_agencia
        from colaboradores
        left join agencias on colaboradores.id_agencia = agencias.id_agencia
    )

    , cliente_conta_joined as (
        select
            contas.id_conta
            , clientes.id_cliente
            , ca.id_agencia
            , ca.id_colaborador
            , clientes.id_proposta
            , clientes.nome_completo_cliente as nome_cliente
            , ca.nome_colaborador
            , ca.nome_agencia
            , ca.cidade_agencia
            , ca.uf_agencia
            , contas.tipo_conta
            , ca.tipo_agencia
            , contas.saldo_total_conta
            , contas.saldo_disponivel_conta
        from contas
        left join clientes_propostas_joined clientes on contas.id_cliente_conta = clientes.id_cliente
        left join colaborador_agencia_joined ca on (contas.id_agencia_conta = ca.id_agencia) and (contas.id_colaborador_conta = ca.id_colaborador)
    )

    , transacoes_joined as (
        select
            {{ dbt_utils.generate_surrogate_key(['transacoes.id_transacao', 'transacoes.id_conta', 'cc.id_agencia']) }} as sk_balanco
            , transacoes.id_transacao
            , transacoes.id_conta
            , cc.id_cliente
            , cc.id_agencia
            , cc.id_colaborador
            , cc.id_proposta
            , cc.nome_cliente
            , cc.nome_colaborador
            , cc.nome_agencia
            , cc.cidade_agencia
            , cc.uf_agencia
            , cc.tipo_conta
            , cc.tipo_agencia
            , transacoes.tipo_transacao
            , transacoes.valor_transacao
            , cc.saldo_total_conta
            , cc.saldo_disponivel_conta
            , transacoes.data_transacao
        from transacoes
        left join cliente_conta_joined cc on transacoes.id_conta = cc.id_conta
        where cc.id_cliente is not null
    )

    , transformacao as (
        select
            *
            , saldo_total_conta/count(*) over(partition by id_conta) as saldo_total_conta_ponderado
            , saldo_disponivel_conta/count(*) over(partition by id_conta) as saldo_disponivel_conta_ponderado
        from transacoes_joined
    )

select * from transformacao