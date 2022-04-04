with indicadores as (    
    select 
        ano,
        mes,
        p.codigo,
        p.tipo,
        p.movimento,
        p.status,
        if(movimento in ('ENTRADA','ENTRADA - PROJEÇÃO'), 1,0) as entradas_projetadas,
        if(movimento in ('SAÍDA','SAÍDA - PROJEÇÃO'), 1,0) as saidas_projetadas,  
        p.nome_da_loja,
        p.cidade,
        p.tipo_de_loja,
        p.marca,
        p.gmv, 
        d.id,
        d.active,
        p.status_abertura_franquia,
        case 
            when movimento in ('ENTRADA') then min(oi.invoice_date)            
        end as mes_inauguracao,
        case 
            when movimento in ('SAÍDA') then max(oi.invoice_date)
        end as mes_fechamento,
        sum(oi.net_value) as net_value,    
        p.data_inauguracao,
        p.repreSentante_1,        
        count(case when upper(p.movimento) like('%ENTRADA%') and ano = '2022' then p.movimento end) over (partition by concat(p.nome_da_loja,p.marca))   as contador
        

    from  `projetoomni.datastudio_spreadsheet.exp0001_plano_expansao_base_sheets` as p
        left join `projetoomni.reporting_bi.omni_order_items` as oi
            on lower(p.nome_da_loja) = lower(oi.store_name) and lower(p.marca) = lower(oi.site) and cast(p.ano as numeric) = extract(year from oi.invoice_date) and p.mes = cast(extract(month from oi.invoice_date) as string)
        left join  `projetoomni.ecommerce_postgres.distributors` as d
            on oi.distributorId = d.id
    
    where
      p.movimento is not null --nulo sao as lojas que já estão abertas
      and p.tipo = 'ORÇAMENTO'
     --and right(p.data_inauguracao,4) = '2022'
--       and p.nome_da_loja  = 'FOZ DO IGUAÇU'
            
    group by 
        p.codigo,
        p.status,
        p.marca,
        p.movimento,
        p.tipo,
        p.nome_da_loja,
        p.tipo_de_loja,
        p.data_inauguracao,
        p.status_abertura_franquia,
        p.ano, 
        p.mes,  
        p.repreSentante_1,
        p.gmv,
        d.id,
        p.cidade,
        d.active       
        
)

select 
    * 
from 
  indicadores

