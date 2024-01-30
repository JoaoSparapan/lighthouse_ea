with
    stg_transacoes as (
        select
            *
        from {{ ref('stg_erp__transacoes') }}
    )

select * from stg_transacoes