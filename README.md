# ANS Despesas Analytics

Projeto desenvolvido como **teste tecnico de estagio**, com foco em ingestao, processamento, validacao, analise e persistencia de dados publicos da **ANS (Agencia Nacional de Saude Suplementar)**.

---

## Indice
- [Visao geral](#visao-geral)
- [Como executar](#como-executar)
- [Banco de dados (PostgreSQL)](#banco-de-dados-postgresql)
- [Desafios, deducoes e caminho adotado](#desafios-deducoes-e-caminho-adotado)
- [Decisoes e trade-offs](#decisoes-e-trade-offs)

---

## Visao geral
O software foi dividido em camadas para manter o desenvolvimento organizado e permitir evolucao. Separar essas partes facilita testes, manutencao e reaproveitamento: o ETL gera dados limpos, o banco persiste e permite analises, e a camada de API/Frontend pode consumir.

- **ETL**: download, extracao, consolidacao, validacao, enriquecimento e agregacao.
- **Banco de dados**: DDL, importacao com staging e queries analiticas.
- **API e Frontend**: planejados, nao implementados ate o momento.


### Tecnologias e justificativas
- **Python 3.12**: escolhido por ser mais facil de lidar (documentacao, comunidade e exemplos). Apesar do Java parecer mais robusto, ele tende a ser mais complexo, com o tempo limitado do desafio o Python permitiu entregar mais sem perder qualidade.
- **pandas**: manipulacao tabular confiavel e produtiva para consolidar, validar e agregar CSVs.
- **requests + BeautifulSoup**: simples e robusto para listar o FTP da ANS e baixar arquivos.
- **openpyxl**: leitura de XLSX em modo streaming, evitando alto consumo de memoria.
- **PostgreSQL 10+**: escolhido por ser mais funcional para validacoes e scripts SQL. A linguagem e as funcoes disponiveis tornam as limpezas e conversoes mais diretas.


---

## Como executar

### Pre-requisitos
- Python 3.12+
- PostgreSQL 10+

### 1) ETL (pipeline completo)
```bash
python etl/run_pipeline.py
```
Saidas principais:
- `data/output/consolidado_despesas.csv`
- `data/output/consolidado_despesas.zip`
- `data/output/consolidado_validado.csv`
- `data/output/inconsistencias_2_1.csv`
- `data/output/consolidado_enriquecido.csv`
- `data/output/inconsistencias_2_2.csv`
- `data/output/despesas_agregadas.csv`
- `data/output/Teste_Samuel_de_Souza.zip`
- `data/output/Relatorio_cadop.csv`
- Log: `logs/pipeline_YYYYMMDD_HHMMSS.log`

### 2) Banco de dados (DDL + importacao)

1) Se `data/output/consolidado_despesas.csv` ainda nao existir, descompacte `data/output/consolidado_despesas.zip` na pasta `data/output`.

2) Execute os scripts SQL:
```bash
psql -d postgres -f sql/01_ddl.sql
psql -d ans_despesas -f sql/02_import.sql
```

> Obs:
> - `sql/01_ddl.sql` cria o banco e usa `\connect`, entao rode via `psql`.
> - `sql/02_import.sql` usa `\copy` e assume encoding UTF-8.

### 3) Queries analiticas
Arquivo: `sql/03_analytics.sql`
- Rode o arquivo no `psql` ou no seu **SGBD** (cliente SQL) conectado ao banco `ans_despesas`.

---

## Desafios, deducoes e caminho adotado
Durante a execucao, surgiram **divergencias reais** entre o PDF e os dados disponiveis na ANS. Isso impedia o cumprimento literal de partes do enunciado e exigiu uma decisao explicita para continuar.

**Por que o problema aconteceu (o que foi observado):**
- O PDF descrevia uma estrutura de pastas `YYYY/QQ/`, mas o endpoint real da ANS expunha arquivos no formato `{trimestre}T{ano}.zip` (ex.: `1T2024.zip`).
- Os demonstrativos contabeis **nao traziam CNPJ nem Razao Social**, embora o PDF pedisse essas colunas no consolidado (1.3) e mencionasse CNPJs duplicados.
- O PDF solicitava o join com CADOP usando CNPJ (2.2), mas essa chave **nao existia** nos demonstrativos.

Esses pontos foram verificados diretamente nos arquivos baixados, em softwares diferentes e em maquinas diferentes, para garantir que nao era um problema local.

**Caminho adotado para continuar:**
- Adaptamos o **indexador** para o formato real do endpoint (`1T2024.zip`).
- **Deducao aplicada:** ao comparar os datasets, notei que os valores de `REG_ANS` nos demonstrativos apareciam em `REGISTRO_OPERADORA` no CADOP. Como o PDF nao fornecia alternativa e a coluna CNPJ nao existia nos demonstrativos, deduzi essa equivalencia e usei essa ligacao para recuperar CNPJ e Razao Social.
- Enriquecemos o consolidado com o CADOP para obter CNPJ e Razao Social, possibilitando as etapas seguintes.
- Persistimos o `Relatorio_cadop.csv` na saida para atender ao requisito do banco.

Essa escolha foi necessaria para manter o processamento consistente e cumprir o objetivo do desafio mesmo com a divergencia entre o documento e os dados reais.

---

## Banco de dados (PostgreSQL)

### O que foi pedido e como foi atendido

**Arquivos utilizados (requisito do PDF)**
- `consolidado_despesas.csv` (extraido de `consolidado_despesas.zip`)
- `despesas_agregadas.csv`
- `Relatorio_cadop.csv` (CADOP)

**Modelagem e DDL**
- Tabelas criadas:
  - `ans.operadoras_cadop`
  - `ans.despesas_consolidadas`
  - `ans.despesas_agregadas`
- Chaves primarias e indices foram definidos para garantir unicidade e acelerar filtros por `cnpj`, `registro_operadora`, `uf` e periodo.
- **Normalizacao escolhida:** tabelas separadas (CADOP, consolidado e agregado), reduzindo redundancia e facilitando atualizacoes cadastrais.
- **Tipos de dados:**
  - Monetario em `numeric(18,5)` para precisao (evita erros de `float`).
  - Datas em `date` para `Data_Registro_ANS` (nao ha necessidade de horario).

**Importacao e tratamento de inconsistencias**
- Importacao em **staging** (`ans_stg`) como texto, seguida de conversoes para os tipos finais.
- **Encoding UTF-8** garantido no `\copy`.
- **Tratamentos aplicados:**
  - NULL em campos obrigatorios: registros descartados.
  - Strings em campos numericos: conversao apenas quando padrao numerico valido.
  - Datas inconsistentes: tentativa de `YYYY-MM-DD` ou `DD/MM/YYYY`, caso contrario `NULL`.
- Ao final, o schema `ans_stg` e removido para nao deixar lixo temporario.

**Queries analiticas (3.4)**
- Query 1: Top 5 crescimento percentual (primeiro vs ultimo trimestre).
- Query 2: Distribuicao por UF + media por operadora.
- Query 3: Operadoras acima da media geral em pelo menos 2 de 3 trimestres.
---

## Decisoes e trade-offs

### 1.1) Acesso a API de Dados Abertos da ANS
- **Desafio:** o PDF indicava pastas `YYYY/QQ/`, mas o endpoint real expunha ZIPs no formato `{trimestre}T{ano}.zip`.
- **Decisao/Trade-off:** adaptar o indexador ao formato real para garantir download hoje. O trade-off e depender do padrao atual do servidor, mas isso fica documentado e facilmente ajustavel.

### 1.2) Processamento de arquivos
- **Desafio:** arquivos em CSV/TXT/XLSX, com colunas e estruturas diferentes.
- **Trade-off tecnico:** processar tudo em memoria vs processar incrementalmente.
- **Decisao:** leitura em chunks para CSV/TXT e streaming para XLSX; deteccao de colunas por normalizacao e filtro de linhas com "despesa" + "evento/sinistro". Prioriza estabilidade em maquinas com menos memoria.

### 1.3) Consolidacao e analise de inconsistencias
- **Analise critica (do PDF):** CNPJs duplicados com razoes diferentes, valores zerados/negativos, trimestres inconsistentes.
- **Decisoes aplicadas:**
  - Deduzir `REG_ANS` (demonstrativos) = `REGISTRO_OPERADORA` (CADOP) para obter CNPJ/Razao Social, ja que os demonstrativos nao traziam essas colunas.
  - Usar `VL_SALDO_FINAL` como base de `ValorDespesas` por consistencia entre arquivos.
  - Extrair ano/trimestre do nome do arquivo (`3T2025`) para evitar discrepancias com a coluna `DATA`.
  - Separar inconsistencias em arquivos especificos nas etapas 2.1 e 2.2.
- **Trade-off:** `VL_SALDO_FINAL` e a deducao `REG_ANS` como chave sao proxies necessarios para cumprir o desafio com dados reais.

### 2.1) Validacao de dados
- **Trade-off tecnico (CNPJ invalido):** poderia corrigir ou marcar.
- **Decisao:** remover registros invalidos e registrar o motivo em `inconsistencias_2_1.csv`.
- **Justificativa:** evita contaminar agregacoes e rankings com identificacoes incorretas.

### 2.2) Enriquecimento com dados cadastrais
- **Analise critica (do PDF):**
  - CNPJs sem match no cadastro.
  - CNPJs duplicados no CADOP com dados diferentes.
- **Decisoes aplicadas:**
  - Manter registros sem match com campos nulos e registrar em `inconsistencias_2_2.csv`.
  - Resolver duplicidade por CNPJ escolhendo o registro mais recente (`DataRegistroANS`).
- **Trade-off tecnico (join):** pandas em memoria apos validacao, por volume reduzido e simplicidade; para volumes grandes seria necessario streaming/DB.

### 2.3) Agregacao e ordenacao
- **Desafio adicional (do PDF):** media e desvio padrao por operadora/UF.
- **Decisao:** calcular media por operadora/UF e desvio padrao populacional (ddof=0) considerando os 3 trimestres analisados como conjunto completo.
- **Trade-off tecnico (ordenacao):** ordenar em memoria apos agregacao por ser volume pequeno; para volumes maiores a ordenacao poderia ir para SQL ou processamento externo.

### 3.2) Modelagem e DDL
- **Trade-off tecnico (normalizacao):** tabelas separadas (CADOP, consolidado, agregado) vs tabela unica desnormalizada.
- **Decisao:** modelo normalizado para reduzir redundancia e facilitar atualizacoes cadastrais.
- **Trade-off tecnico (tipos):**
  - Monetario em `numeric(18,5)` para precisao.
  - Datas em `date` para `Data_Registro_ANS` (sem necessidade de horario).

### 3.3) Importacao e tratamento de inconsistencias
- **Analise critica (do PDF):** NULLs em campos obrigatorios, strings em campos numericos, datas inconsistentes.
- **Decisao:** staging em `ans_stg`, conversoes com regex e datas em `YYYY-MM-DD` ou `DD/MM/YYYY`; registros invalidos sao descartados.

### 3.4) Queries analiticas
- **Query 1:** compara primeiro vs ultimo trimestre global; operadoras sem dados nos extremos ficam fora (garante comparabilidade).
- **Query 2:** usa `AVG(total_despesas)` por UF para media por operadora.
- **Query 3:** subquery com media geral dos 3 ultimos trimestres; prioriza legibilidade e manutenibilidade.
