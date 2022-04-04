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
    with subquery as(
 
    select distinct
        t.name,
        t.idList as id,
        lower(t.marca) as marca,
        cast(extract(year from cast(t.data_projecao as datetime)) as integer) as ano,
        cast(extract(month from cast(t.data_projecao as datetime)) as integer) as mes,
        parse_date('%F', concat(extract(year from cast(t.data_projecao as datetime)),'-',extract(month from cast(t.data_projecao as datetime)),'-1')) as mes_data,
        cast(t.data_projecao as datetime) as data_inauguracao ,
        date_trunc(min(min(oi.creation_date)) over (partition by d.id), month) as primeiro_faturamento,
        date_add(current_date,interval - 10 day)as dez_dias,
        date_trunc(max(max(oi.creation_date)) over (partition by d.id), month) as ultimo_mes_faturamento,
        max(max(oi.creation_date)) over (partition by d.id) as ultimo_faturamento


    from `projetoomni.datastudio_spreadsheet.exp0001_plano_expansao_2022_trello` as t
        left join  `projetoomni.reporting_bi.omni_order_items` as oi
            on t.name = oi.store_name and t.marca = oi.site 
        left join `projetoomni.ecommerce_postgres.distributors` as d
            on d.id = oi.distributorId
    group by marca, ano, mes, mes_data, t.name,t.data_projecao, d.id, id  
 

    union all 

     select distinct
        d.name,
        d.id,               
        lower(d.group) as marca,
        cast(extract(year from oi.creation_date) as integer) as ano,
        cast(extract(month from oi.creation_date) as integer) as mes,
        parse_date('%F',concat(extract(year from oi.creation_date),'-',extract(month from oi.creation_date),'-1')) as mes_data,
        cast(d.inaugurationDate as datetime) as data_inauguracao,
        date_trunc(min(min(oi.creation_date)) over (partition by d.id), month) as primeiro_faturamento,
        date_add(current_date,interval - 20 day)as vinte_dias,
        date_trunc(max(max(oi.creation_date)) over (partition by d.id), month) as ultimo_mes_faturamento,
        max(max(oi.creation_date)) over (partition by d.id) as ultimo_faturamento,
        



    from  `projetoomni.reporting_bi.omni_order_items` as oi
        inner join `projetoomni.ecommerce_postgres.distributors` as d
            on d.id = oi.distributorId            
    group by marca, ano, mes, mes_data, d.name,d.inaugurationDate, d.id

    order by mes_data desc                 

    )
    select 
        *, 
        case
            when name in ('Mossoró','Shopping Recife') then 0 
            when primeiro_faturamento  =  mes_data  or id in ('Lojas em negociação','Lojas Negociadas','Lojas em Prospecção')  then 1
            
        else 0 end as entradas,
        case
            when name in ('Mossoró','Shopping Recife') then 0 
            when ultimo_faturamento < dez_dias and ultimo_mes_faturamento  =  mes_data  then -1            
        else 0 end as saidas,

   
    from subquery 
    where ano > 2021 and id <'3000' or id in ('Lojas em negociação','Lojas Negociadas','Lojas em Prospecção')-- and  lower(name) like '%mossor%'
    order by  ano, mes desc
)
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
    sum(c.entradas_mes) as entradas_mes,
    sum(c.saidas_mes) as saidas_mes,
    c.entradas_acc,
    c.saidas_acc,    
    c.lojas_inicial 
        + c.entradas_acc 
        + c.saidas_acc as lojas_final,    
from calculo_acc c
group by 
    c.mes_data,
    c.marca,
    c.lojas_inicial,   
    c.entradas_acc,
    c.saidas_acc
order by c.mes_data