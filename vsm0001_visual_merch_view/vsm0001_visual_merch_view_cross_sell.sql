WITH
  mais_vendidos AS (
  SELECT
    oi.title produto_mais_vendido,
    COUNT(DISTINCT oi.displaycode) AS numero_transacoes,
    RANK() OVER(ORDER BY COUNT(DISTINCT oi.displaycode) DESC) AS top_10
  FROM
    `projetoomni.reporting_bi.omni_order_items` oi
  WHERE
    oi.title NOT IN ('Frete Ecommerce')
  GROUP BY
    produto_mais_vendido QUALIFY top_10 <= 10 )
----------------------------------------------------------------------------------    
SELECT
  oi.title produto_mais_vendido,
  o.title produto_vendido_junto,
  oi.invoice_date,
  COUNT(DISTINCT oi.displaycode) AS numero_transacoes
FROM
  mais_vendidos m
LEFT JOIN
  `projetoomni.reporting_bi.omni_order_items` oi
    ON  m.produto_mais_vendido = oi.title
LEFT JOIN
  `projetoomni.reporting_bi.omni_order_items` o
    ON oi.displaycode = o.displaycode
        AND oi.title > o.title
WHERE
  o.title NOT IN ('Frete Ecommerce')
GROUP BY
  produto_mais_vendido,
  produto_vendido_junto,
  m.top_10,
  oi.invoice_date
ORDER BY
  m.top_10,
  numero_transacoes DESC