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
- `data/output/despesas_agregadas.csv`
- `data/output/Teste_Samuel_de_Souza.zip`
- `data/output/consolidado_despesas.zip`
- `data/output/Relatorio_cadop.csv`
- Log: `logs/pipeline_YYYYMMDD_HHMMSS.log`

### 2) Banco de dados (DDL + importacao)

1) Descompacte `data/output/consolidado_despesas.zip` na pasta `data/output` para gerar `consolidado_despesas.csv`.

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

### 1) Estrutura de diretorios dos trimestres
- **Problema:** PDF indicava formato `YYYY/QQ/`, mas o endpoint da ANS usa `1T2024.zip`.
- **Decisao:** adaptar o indexador para o formato real do endpoint.
- **Justificativa:** o objetivo era executar o pipeline com os dados reais publicados. Aderir ao formato descrito no PDF impediria o download automatico, enquanto seguir o formato observado garante funcionamento hoje. O trade-off e depender do padrao atual do servidor, mas documentado e facilmente ajustavel se mudar.

### 2) Chave de ligacao entre demonstrativos e CADOP
- **Problema:** demonstrativos nao tem CNPJ/Razao Social.
- **Decisao (deducao):** usar `REG_ANS` (demonstrativos) = `REGISTRO_OPERADORA` (CADOP) porque observei a correspondencia direta desses valores entre os arquivos.
- **Justificativa:** sem essa deducao nao existia chave para obter CNPJ/Razao Social e cumprir 1.3/2.2. O trade-off e assumir que a correspondencia e valida para todo o conjunto, mas foi a unica ligacao consistente observada e permitiu seguir com o desafio.

### 3) Colunas e valores de despesas
- **Problema:** ausencia de `VL_SALDO_INICIAL` em arquivos antigos.
- **Decisao:** usar **somatorio de `VL_SALDO_FINAL`** por `REG_ANS + Ano + Trimestre`.
- **Justificativa:** essa coluna existe de forma mais consistente nos arquivos. O trade-off e usar saldo final como proxy de despesa, mas garante comparabilidade entre anos e evita quebrar o processamento por coluna ausente.

### 4) Ano e trimestre
- **Problema:** coluna `DATA` pode representar datas contabeis variadas.
- **Decisao:** extrair ano/trimestre do nome do arquivo (`3T2025`).
- **Justificativa:** o nome do arquivo reflete o periodo oficial do demonstrativo. O trade-off e ignorar a coluna `DATA`, mas evita discrepancias quando a data e de lancamento e nao do trimestre.

### 5) Leitura incremental vs memoria
- **Decisao:** leitura em chunks para CSV/TXT e streaming para XLSX.
- **Justificativa:** os arquivos sao grandes e variam de formato. O trade-off e maior tempo de leitura, mas garante estabilidade e execucao em ambientes com menos memoria.

### 6) Validacao e consistencia dos dados
- **Decisao:** excluir CNPJ invalido, Razao Social vazia e valores <= 0.
- **Justificativa:** a analise pedida depende de identificacao correta e valores positivos. Preferi perder registros inconsistentes a contaminar agregacoes e rankings com dados incorretos. O trade-off e reduzir o volume final, mas aumenta confiabilidade.

### 7) Duplicidade de CNPJ no CADOP
- **Decisao:** usar o registro mais recente por `Data_Registro_ANS`.
- **Justificativa:** o CADOP pode ter historico; para analise atual, o registro mais recente e o mais adequado. O trade-off e perder historico, mas simplifica e evita duplicidade no join.

### 8) Persistencia do CADOP na saida
- **Decisao:** manter `Relatorio_cadop.csv` na pasta de saida.
- **Justificativa:** a etapa do banco exige o CSV de cadastro. Persistir o arquivo evita download repetido e deixa o ambiente reproducivel para quem for validar.

### 9) Modelagem do banco (normalizacao)
- **Decisao:** tabelas normalizadas (CADOP, consolidado, agregado).
- **Justificativa:** evita repetir cadastro a cada trimestre, reduz tamanho e permite atualizar CADOP sem recalcular tudo. O trade-off e necessidade de joins, mas o volume e pequeno o suficiente para consultas analiticas.

### 10) Tipos de dados
- **Monetario:** `numeric(18,5)` para precisao (evita erros de `float`).
- **Data:** `date` para `Data_Registro_ANS` (sem necessidade de horario).
  - **Justificativa:** prioridade para precisao contabel e simplicidade. `float` pode gerar arredondamentos; `date` evita armazenar horario que nao e usado.

### 11) Indices criados
- **PKs:** garantem unicidade e ja indexam as chaves principais.
- **Indices adicionais:** `registro_operadora`, `uf`, `cnpj`, `ano/trimestre` para filtros e analises.
  - **Justificativa:** refletem os padroes de consulta do desafio (detalhe por operadora, filtros por UF e periodo). O trade-off e custo de escrita um pouco maior, aceitavel para cargas batch.

### 12) Importacao com staging
- **Decisao:** importar CSVs em `ans_stg` e limpar antes de inserir.
- **Justificativa:** mesmo com ETL, arquivos externos podem ter variacoes. Staging garante que uma linha ruim nao interrompa toda a carga; o trade-off e mais etapas, mas ganha robustez.

### 13) Queries analiticas
- **Query 1:** considera apenas operadoras com dados nos dois extremos (comparabilidade).
- **Query 2:** usa tabela agregada para reduzir custo e incluir media por operadora.
- **Query 3:** considera os 3 ultimos trimestres globais e mede acima da media geral.
  - **Justificativa:** escolhas privilegiam comparabilidade e simplicidade. Em especial, usar extremos globais evita comparar periodos diferentes, e usar os 3 ultimos trimestres reduz ruido e custo.
