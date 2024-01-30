with
    stg_agencias as (
        select
            *
        from {{ ref('stg_erp__agencias') }}
    )

select * from stg_agencias