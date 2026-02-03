-- Etapa 3.4

-- Query 1: 5 operadoras com maior crescimento percentual

SELECT
    f.cnpj,
    f.razao_social,
    i.valor_despesas AS valor_inicial,
    f.valor_despesas AS valor_final,
    ROUND(((f.valor_despesas - i.valor_despesas) / i.valor_despesas) * 100, 2) AS crescimento_percentual
FROM ans.despesas_consolidadas f
JOIN ans.despesas_consolidadas i
  ON i.cnpj = f.cnpj
WHERE (i.ano, i.trimestre) = (
        SELECT ano, trimestre
        FROM ans.despesas_consolidadas
        ORDER BY ano, trimestre
        LIMIT 1
      )
  AND (f.ano, f.trimestre) = (
        SELECT ano, trimestre
        FROM ans.despesas_consolidadas
        ORDER BY ano DESC, trimestre DESC
        LIMIT 1
      )
  AND i.valor_despesas > 0
ORDER BY crescimento_percentual DESC
LIMIT 5;

-- Query 2: Distribuicao de despesas por UF (top 5)

SELECT
    uf,
    SUM(total_despesas) AS total_despesas_uf,
    ROUND(AVG(total_despesas), 2) AS media_por_operadora
FROM ans.despesas_agregadas
WHERE uf IS NOT NULL
GROUP BY uf
ORDER BY total_despesas_uf DESC
LIMIT 5;

-- Query 3: Quantas operadoras tiveram despesas acima da media geral

SELECT COUNT(*) AS operadoras_acima_media_2_de_3
FROM (
    SELECT d.cnpj, COUNT(*) AS qtd_acima
    FROM ans.despesas_consolidadas d
    WHERE (d.ano, d.trimestre) IN (
        SELECT ano, trimestre
        FROM ans.despesas_consolidadas
        GROUP BY ano, trimestre
        ORDER BY ano DESC, trimestre DESC
        LIMIT 3
    )
    AND d.valor_despesas > (
        SELECT AVG(x.valor_despesas)
        FROM ans.despesas_consolidadas x
        WHERE (x.ano, x.trimestre) IN (
            SELECT ano, trimestre
            FROM ans.despesas_consolidadas
            GROUP BY ano, trimestre
            ORDER BY ano DESC, trimestre DESC
            LIMIT 3
        )
    )
    GROUP BY d.cnpj
) t
WHERE qtd_acima >= 2;
