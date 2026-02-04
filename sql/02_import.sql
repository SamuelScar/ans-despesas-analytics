-- Etapa 3.3
-- Execute via psql (necessario para \copy).

-- =========================
-- 1) CADOP
-- =========================
CREATE SCHEMA IF NOT EXISTS ans_stg;

DROP TABLE IF EXISTS ans_stg.cadop;
CREATE TABLE ans_stg.cadop (
    registro_operadora text,
    cnpj text,
    razao_social text,
    nome_fantasia text,
    modalidade text,
    logradouro text,
    numero text,
    complemento text,
    bairro text,
    cidade text,
    uf text,
    cep text,
    ddd text,
    telefone text,
    fax text,
    endereco_eletronico text,
    representante text,
    cargo_representante text,
    regiao_de_comercializacao text,
    data_registro_ans text
);

\copy ans_stg.cadop FROM 'data/output/Relatorio_cadop.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');

WITH cadop_clean AS (
    SELECT
        regexp_replace(cnpj, '[^0-9]', '', 'g') AS cnpj,
        NULLIF(trim(registro_operadora), '') AS registro_operadora,
        NULLIF(trim(razao_social), '') AS razao_social,
        NULLIF(trim(nome_fantasia), '') AS nome_fantasia,
        NULLIF(trim(modalidade), '') AS modalidade,
        NULLIF(trim(logradouro), '') AS logradouro,
        NULLIF(trim(numero), '') AS numero,
        NULLIF(trim(complemento), '') AS complemento,
        NULLIF(trim(bairro), '') AS bairro,
        NULLIF(trim(cidade), '') AS cidade,
        upper(NULLIF(trim(uf), '')) AS uf,
        regexp_replace(cep, '[^0-9]', '', 'g') AS cep,
        regexp_replace(ddd, '[^0-9]', '', 'g') AS ddd,
        NULLIF(trim(telefone), '') AS telefone,
        NULLIF(trim(fax), '') AS fax,
        NULLIF(trim(endereco_eletronico), '') AS endereco_eletronico,
        NULLIF(trim(representante), '') AS representante,
        NULLIF(trim(cargo_representante), '') AS cargo_representante,
        NULLIF(trim(regiao_de_comercializacao), '') AS regiao_de_comercializacao,
        CASE
            WHEN data_registro_ans ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN data_registro_ans::date
            WHEN data_registro_ans ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN to_date(data_registro_ans, 'DD/MM/YYYY')
            ELSE NULL
        END AS data_registro_ans
    FROM ans_stg.cadop
    WHERE regexp_replace(cnpj, '[^0-9]', '', 'g') ~ '^[0-9]{14}$'
      AND NULLIF(trim(razao_social), '') IS NOT NULL
),
cadop_dedup AS (
    SELECT DISTINCT ON (cnpj)
        cnpj,
        registro_operadora,
        razao_social,
        nome_fantasia,
        modalidade,
        logradouro,
        numero,
        complemento,
        bairro,
        cidade,
        uf,
        cep,
        ddd,
        telefone,
        fax,
        endereco_eletronico,
        representante,
        cargo_representante,
        regiao_de_comercializacao,
        data_registro_ans
    FROM cadop_clean
    ORDER BY cnpj, data_registro_ans DESC NULLS LAST
)
INSERT INTO ans.operadoras_cadop (
    cnpj,
    registro_operadora,
    razao_social,
    nome_fantasia,
    modalidade,
    logradouro,
    numero,
    complemento,
    bairro,
    cidade,
    uf,
    cep,
    ddd,
    telefone,
    fax,
    endereco_eletronico,
    representante,
    cargo_representante,
    regiao_de_comercializacao,
    data_registro_ans
)
SELECT
    cnpj,
    registro_operadora,
    razao_social,
    nome_fantasia,
    modalidade,
    logradouro,
    numero,
    complemento,
    bairro,
    cidade,
    uf,
    cep,
    ddd,
    telefone,
    fax,
    endereco_eletronico,
    representante,
    cargo_representante,
    regiao_de_comercializacao,
    data_registro_ans
FROM cadop_dedup
ON CONFLICT (cnpj) DO UPDATE SET
    registro_operadora = EXCLUDED.registro_operadora,
    razao_social = EXCLUDED.razao_social,
    nome_fantasia = EXCLUDED.nome_fantasia,
    modalidade = EXCLUDED.modalidade,
    logradouro = EXCLUDED.logradouro,
    numero = EXCLUDED.numero,
    complemento = EXCLUDED.complemento,
    bairro = EXCLUDED.bairro,
    cidade = EXCLUDED.cidade,
    uf = EXCLUDED.uf,
    cep = EXCLUDED.cep,
    ddd = EXCLUDED.ddd,
    telefone = EXCLUDED.telefone,
    fax = EXCLUDED.fax,
    endereco_eletronico = EXCLUDED.endereco_eletronico,
    representante = EXCLUDED.representante,
    cargo_representante = EXCLUDED.cargo_representante,
    regiao_de_comercializacao = EXCLUDED.regiao_de_comercializacao,
    data_registro_ans = EXCLUDED.data_registro_ans
WHERE EXCLUDED.data_registro_ans IS NOT NULL
  AND (
        ans.operadoras_cadop.data_registro_ans IS NULL
        OR EXCLUDED.data_registro_ans >= ans.operadoras_cadop.data_registro_ans
      );

-- =========================
-- 2) CONSOLIDADO
-- =========================
DROP TABLE IF EXISTS ans_stg.consolidado;
CREATE TABLE ans_stg.consolidado (
    cnpj text,
    razao_social text,
    trimestre text,
    ano text,
    valor_despesas text
);

-- Descompacte data/output/consolidado_despesas.zip antes de importar.
\copy ans_stg.consolidado FROM 'data/output/consolidado_despesas.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

INSERT INTO ans.despesas_consolidadas (
    cnpj,
    razao_social,
    trimestre,
    ano,
    valor_despesas
)
SELECT
    regexp_replace(cnpj, '[^0-9]', '', 'g') AS cnpj,
    NULLIF(trim(razao_social), ''),
    trimestre::smallint,
    ano::smallint,
    CASE
        WHEN valor_despesas ~ ',' THEN regexp_replace(replace(valor_despesas, '.', ''), ',', '.', 'g')::numeric(18,5)
        ELSE valor_despesas::numeric(18,5)
    END
FROM ans_stg.consolidado
WHERE regexp_replace(cnpj, '[^0-9]', '', 'g') ~ '^[0-9]{14}$'
  AND NULLIF(trim(razao_social), '') IS NOT NULL
  AND trimestre ~ '^[1-4]$'
  AND ano ~ '^[0-9]{4}$'
  AND valor_despesas ~ '^[0-9.,-]+$'
  AND (
        CASE
            WHEN valor_despesas ~ ',' THEN regexp_replace(replace(valor_despesas, '.', ''), ',', '.', 'g')::numeric(18,5)
            ELSE valor_despesas::numeric(18,5)
        END
      ) > 0
ON CONFLICT (cnpj, ano, trimestre) DO UPDATE SET
    razao_social = EXCLUDED.razao_social,
    valor_despesas = EXCLUDED.valor_despesas;

-- =========================
-- 3) AGREGADO
-- =========================
DROP TABLE IF EXISTS ans_stg.agregadas;
CREATE TABLE ans_stg.agregadas (
    razao_social text,
    uf text,
    total_despesas text,
    media_despesas text,
    desvio_padrao_despesas text
);

\copy ans_stg.agregadas FROM 'data/output/despesas_agregadas.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

INSERT INTO ans.despesas_agregadas (
    razao_social,
    uf,
    total_despesas,
    media_despesas,
    desvio_padrao_despesas
)
SELECT
    NULLIF(trim(razao_social), ''),
    upper(NULLIF(trim(uf), '')),
    CASE
        WHEN total_despesas ~ ',' THEN regexp_replace(replace(total_despesas, '.', ''), ',', '.', 'g')::numeric(18,5)
        ELSE total_despesas::numeric(18,5)
    END,
    CASE
        WHEN media_despesas ~ ',' THEN regexp_replace(replace(media_despesas, '.', ''), ',', '.', 'g')::numeric(18,5)
        ELSE media_despesas::numeric(18,5)
    END,
    CASE
        WHEN desvio_padrao_despesas ~ ',' THEN regexp_replace(replace(desvio_padrao_despesas, '.', ''), ',', '.', 'g')::numeric(18,5)
        ELSE desvio_padrao_despesas::numeric(18,5)
    END
FROM ans_stg.agregadas
WHERE NULLIF(trim(razao_social), '') IS NOT NULL
  AND length(trim(uf)) = 2
  AND total_despesas ~ '^[0-9.,-]+$'
  AND media_despesas ~ '^[0-9.,-]+$'
  AND desvio_padrao_despesas ~ '^[0-9.,-]+$'
  AND (
        CASE
            WHEN total_despesas ~ ',' THEN regexp_replace(replace(total_despesas, '.', ''), ',', '.', 'g')::numeric(18,5)
            ELSE total_despesas::numeric(18,5)
        END
      ) > 0
  AND (
        CASE
            WHEN media_despesas ~ ',' THEN regexp_replace(replace(media_despesas, '.', ''), ',', '.', 'g')::numeric(18,5)
            ELSE media_despesas::numeric(18,5)
        END
      ) > 0
  AND (
        CASE
            WHEN desvio_padrao_despesas ~ ',' THEN regexp_replace(replace(desvio_padrao_despesas, '.', ''), ',', '.', 'g')::numeric(18,5)
            ELSE desvio_padrao_despesas::numeric(18,5)
        END
      ) >= 0
ON CONFLICT (razao_social, uf) DO UPDATE SET
    total_despesas = EXCLUDED.total_despesas,
    media_despesas = EXCLUDED.media_despesas,
    desvio_padrao_despesas = EXCLUDED.desvio_padrao_despesas;

-- Limpeza: remove o schema temporario de staging apos a importacao
DROP SCHEMA IF EXISTS ans_stg CASCADE;
