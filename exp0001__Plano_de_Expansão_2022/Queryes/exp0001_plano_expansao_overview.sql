with subquery as(
 
 select distinct
        d.name,
        d.id,
        d.type,               
        lower(d.group) as marca,
        cast(extract(year from oi.creation_date) as integer) as ano,
        cast(extract(month from oi.creation_date) as integer) as mes,
        parse_date('%F',concat(extract(year from oi.creation_date),'-',extract(month from oi.creation_date),'-1')) as mes_data,
        d.inaugurationDate,
        date_trunc(min(min(oi.creation_date)) over (partition by d.id), month) as primeiro_faturamento,
        date_add(current_date,interval - 20 day)as vinte_dias,
        date_trunc(max(max(oi.creation_date)) over (partition by d.id), month) as ultimo_mes_faturamento,
        max(max(oi.creation_date)) over (partition by d.id) as ultimo_faturamento

    from  `projetoomni.reporting_bi.omni_order_items` as oi
        inner join `projetoomni.ecommerce_postgres.distributors` as d
            on d.id = oi.distributorId            
    group by marca, ano, mes, mes_data, d.name,d.inaugurationDate, d.id,  d.type
    ),define_indicadores as (
    select 
        *, 
        case
            when primeiro_faturamento  =  mes_data  then 1
        else 0 end as entradas,
        case
            when ultimo_faturamento < vinte_dias and ultimo_mes_faturamento  =  mes_data  then -1
        else 0 end as saidas
    from subquery 
    where ano > 2021 and id <'3000'-- and  lower(name) like '%mossor%'
    )


    select 
        *,
        case 
            when entradas = 0 and saidas = 0 then "NENHUM"
            when entradas = 1 then "ENTRADA"
            when saidas = -1 then "SAÍDA"        
            
    end as movimento   
    from define_indicadores 
    
