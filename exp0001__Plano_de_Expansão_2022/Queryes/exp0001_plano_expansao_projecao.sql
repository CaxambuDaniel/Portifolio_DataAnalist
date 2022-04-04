with parametros_base as (
        select
        2022 as ano)
--------------------------------------------------------------------------------- 
,parametros_query as (
    select 
        b.*,
        b.ano -1  as ano_inicio
    from parametros_base b) 
--------------------------------------------------------------------------------- 
,meses as (
    select 
        parse_date('%F',concat(q.ano,'-',mes,'-1')) as mes_data,

    from unnest (generate_array(1, 12)) as mes
    cross join parametros_query q)
--------------------------------------------------------------------------------- 
,base_calculo as (
    select distinct
        m.mes_data,
        d.site as marca
    from projetoomni.ecommerce_postgres.distributors d
        cross join meses m
    where d.site in ('artex', 'mmartan'))   
--------------------------------------------------------------------------------- 
,movimento_mes as (
    select distinct
        lower(p.marca) as marca,
        cast(p.ano as integer) as ano,
        cast(p.mes as integer) as mes,
        parse_date('%F',concat(p.ano,'-',p.mes,'-1')) as mes_data,
        sum(case
            when p.movimento like 'ENTRADA%' then 1
        else 0 end) as entradas,
        sum(case
            when p.movimento like 'SAÍDA%' then -1
        else 0 end) as saidas
    from `projetoomni.datastudio_spreadsheet.exp0001_plano_expansao_base_sheets`  as p
        cross join parametros_query q
    where p.movimento is not null --nulo sao as lojas que já estão abertas
        and p.tipo = 'ORÇAMENTO'
        and p.movimento <> 'NENHUM'
        and cast(p.ano as integer) = q.ano
    group by marca, ano, mes, mes_data
    order by mes_data)
---------------------------------------------------------------------------------   
,lojas_inicial as (
    select 
        date_trunc(o.invoice_date, month) as mes_data_real,
        date_trunc(date_add(o.invoice_date, interval 1 month), month) as mes_data,
        o.site as marca,
        count(distinct o.distributorId) as lojas_inicial
    from projetoomni.reporting_bi.omni_order_items o
        cross join parametros_query q
    where o.type not like '%CD%'
        and o.distributorId <'3000'
        and extract(month from o.invoice_date) = 12
        and extract(year from o.invoice_date) = ano_inicio
    group by mes_data_real, mes_data, marca
    order by mes_data desc)
---------------------------------------------------------------------------------
,calculo_acc as (
    select
        b.mes_data,
        b.marca,
        coalesce(i.lojas_inicial,0) as lojas_inicial,
        coalesce(m.entradas,0) as entradas_mes,
        coalesce(m.saidas,0) as saidas_mes,
        sum(coalesce(m.entradas,0)) over (partition by b.marca order by b.mes_data) as entradas_acc,
        sum(coalesce(m.saidas,0)) over (partition by b.marca order by b.mes_data) as saidas_acc
    from base_calculo b
        left join lojas_inicial i
            on i.marca = b.marca
        left join movimento_mes m
            on m.mes_data = b.mes_data
            and m.marca = b.marca
    order by b.marca desc, b.mes_data)
---------------------------------------------------------------------------------
select
    c.mes_data,
    c.marca,
    c.lojas_inicial,
    c.entradas_mes,
    c.saidas_mes,
    c.entradas_acc,
    c.saidas_acc,
    c.lojas_inicial 
        + c.entradas_acc 
        + c.saidas_acc as lojas_final
from calculo_acc c

