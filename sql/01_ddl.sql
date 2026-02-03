-- Etapa 3.2
-- DDL das tabelas + chaves + indices
-- Decisao: modelo normalizado (3 tabelas) para reduzir redundancia e preservar integridade.
-- Tipos:
-- - Valores monetarios: numeric(18,5) para precisao (evita erros do float).
-- - Datas: date para Data_Registro_ANS.

CREATE DATABASE ans_despesas;
\connect ans_despesas

CREATE SCHEMA IF NOT EXISTS ans;


-- Cadastro de operadoras
CREATE TABLE IF NOT EXISTS ans.operadoras_cadop (
    cnpj char(14) PRIMARY KEY CHECK (cnpj ~ '^[0-9]{14}$'),
    registro_operadora varchar(20) CHECK (registro_operadora IS NULL OR registro_operadora ~ '^[0-9]+$'),
    razao_social text NOT NULL CHECK (razao_social <> ''),
    nome_fantasia text,
    modalidade text,
    logradouro text,
    numero text,
    complemento text,
    bairro text,
    cidade text,
    uf char(2) CHECK (uf IS NULL OR uf ~ '^[A-Z]{2}$'),
    cep char(8) CHECK (cep IS NULL OR cep ~ '^[0-9]{8}$'),
    ddd char(2) CHECK (ddd IS NULL OR ddd ~ '^[0-9]{2}$'),
    telefone text,
    fax text,
    endereco_eletronico text,
    representante text,
    cargo_representante text,
    regiao_de_comercializacao text,
    data_registro_ans date
);

CREATE INDEX IF NOT EXISTS idx_operadoras_registro
    ON ans.operadoras_cadop (registro_operadora);

CREATE INDEX IF NOT EXISTS idx_operadoras_uf
    ON ans.operadoras_cadop (uf);


-- Despesas consolidadas
CREATE TABLE IF NOT EXISTS ans.despesas_consolidadas (
    cnpj char(14) NOT NULL CHECK (cnpj ~ '^[0-9]{14}$'),
    razao_social text NOT NULL CHECK (razao_social <> ''),
    trimestre smallint NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    ano smallint NOT NULL CHECK (ano BETWEEN 2000 AND 2100),
    valor_despesas numeric(18,5) NOT NULL CHECK (valor_despesas > 0),
    PRIMARY KEY (cnpj, ano, trimestre),
    FOREIGN KEY (cnpj) REFERENCES ans.operadoras_cadop (cnpj)
);

CREATE INDEX IF NOT EXISTS idx_consolidadas_cnpj
    ON ans.despesas_consolidadas (cnpj);

CREATE INDEX IF NOT EXISTS idx_consolidadas_ano_tri
    ON ans.despesas_consolidadas (ano, trimestre);


-- Despesas agregadas
CREATE TABLE IF NOT EXISTS ans.despesas_agregadas (
    razao_social text NOT NULL CHECK (razao_social <> ''),
    uf char(2) NOT NULL CHECK (uf ~ '^[A-Z]{2}$'),
    total_despesas numeric(18,5) NOT NULL CHECK (total_despesas > 0),
    media_despesas numeric(18,5) NOT NULL CHECK (media_despesas > 0),
    desvio_padrao_despesas numeric(18,5) NOT NULL CHECK (desvio_padrao_despesas >= 0),
    PRIMARY KEY (razao_social, uf)
);

CREATE INDEX IF NOT EXISTS idx_agregadas_uf
    ON ans.despesas_agregadas (uf);
