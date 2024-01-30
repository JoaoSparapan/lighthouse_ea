with
    stg_proposta_credito as (
        select
            *
        from {{ ref('stg_erp__proposta_credito') }}
    )

select * from stg_proposta_credito