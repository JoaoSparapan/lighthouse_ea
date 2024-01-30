with
    stg_contas as (
        select
            *
        from {{ ref('stg_erp__contas') }}
    )

select * from stg_contas