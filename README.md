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

### Estrategia de git
- Usei **trunk-based** porque o projeto e pequeno, nao chegou a um MVP e so eu estou trabalhando nele. Nesse contexto, nao fez sentido aplicar GitFlow ou uma estrategia similar.


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
- Log: `data/logs/pipeline_YYYYMMDD_HHMMSS.log`

### 2) Banco de dados (DDL + importacao)

1) Se `data/output/consolidado_despesas.csv` ainda nao existir, descompacte `data/output/consolidado_despesas.zip` na pasta `data/output`.

2) Execute os scripts SQL:
```bash
psql -d postgres -f sql/ddl.sql
psql -d ans_despesas -f sql/import.sql
```

> Obs:
> - `sql/ddl.sql` cria o banco e usa `\connect`, entao rode via `psql`.
> - `sql/import.sql` usa `\copy` e assume encoding UTF-8.

### 3) Queries analiticas
Arquivo: `sql/analytics.sql`
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
- **Contexto:** o PDF indicava pastas `YYYY/QQ/`, mas o endpoint real expunha ZIPs no formato `{trimestre}T{ano}.zip`.
- **Pros:** garante download imediato com os dados reais publicados; reduz friccao para executar.
- **Contras:** dependencia do padrao atual do servidor; exige ajuste se a ANS mudar o formato.
- **Decisao:** adaptar o indexador ao formato real observado.

### 1.2) Processamento de arquivos
- **Contexto:** arquivos em CSV/TXT/XLSX, com colunas e estruturas diferentes.
- **Pros:** leitura em chunks/streaming reduz consumo de memoria e torna a pipeline mais resiliente.
- **Contras:** processamento mais lento e codigo mais complexo.
- **Decisao:** leitura incremental (chunks para CSV/TXT e streaming para XLSX), com deteccao de colunas por normalizacao e filtro de linhas "despesa" + "evento/sinistro".

### 1.3) Consolidacao e analise de inconsistencias
- **Contexto:** demonstrativos sem CNPJ/Razao Social; colunas variam; o PDF pede tratamento de inconsistencias.
- **Pros:** deducao `REG_ANS` -> `REGISTRO_OPERADORA` permite cumprir 1.3/2.2; `VL_SALDO_FINAL` e mais consistente; ano/trimestre pelo nome evita divergencia com `DATA`.
- **Contras:** `VL_SALDO_FINAL` representa saldo no fim do periodo, nao necessariamente a despesa ocorrida nele, entao pode distorcer o valor real. A ligacao `REG_ANS` -> `REGISTRO_OPERADORA` depende do padrao atual da ANS e pode falhar se houver mudanca ou inconsistencias. Ao usar o trimestre pelo nome do arquivo, a coluna `DATA` (se tiver granularidade ou ajuste diferente) fica de fora.
- **Decisao:** deduzir `REG_ANS` como chave, usar `VL_SALDO_FINAL` e extrair ano/trimestre do nome do arquivo, registrando inconsistencias.

### 2.1) Validacao de dados
- **Contexto:** CNPJs invalidos, valores nao positivos e Razao Social vazia surgem no consolidado.
- **Pros:** remover registros inconsistentes aumenta a confiabilidade das agregacoes e rankings.
- **Contras:** reduz volume e pode excluir dados que seriam corrigiveis.
- **Decisao:** remover registros invalidos e registrar o motivo em `inconsistencias_2_1.csv`, ja que nao ha regras claras de tratamento; optei por manter a integridade do sistema.

### 2.2) Enriquecimento com dados cadastrais
- **Contexto:** ha CNPJs sem match e duplicidade no CADOP com dados diferentes.
- **Pros:** join em memoria e simples e rapido para o volume atual; escolher o mais recente evita duplicidade no resultado.
- **Contras:** manter sem match com campos nulos exige tratamento posterior; join em memoria nao escala; perde historico ao escolher apenas o mais recente.
- **Decisao:** como o PDF nao define regra para duplicidade no CADOP, optei por usar o registro mais recente (`DataRegistroANS`) para evitar duplicar linhas no join e distorcer agregacoes. Mantive sem match com nulos e registrei em `inconsistencias_2_2.csv`.

### 2.3) Agregacao e ordenacao
- **Contexto:** o PDF pede media e desvio padrao por operadora/UF e ordenacao por total.
- **Pros:** o desvio padrao foi calculado considerando os 3 trimestres analisados como o conjunto completo, o que mede bem a variabilidade entre periodos; ordenacao em memoria e simples e suficiente para o tamanho atual.
- **Contras:** se o objetivo fosse estimar a variabilidade de um periodo maior, o resultado poderia ser diferente; ordenacao em memoria pode nao escalar.
- **Decisao:** calcular media e desvio padrao sobre os 3 trimestres analisados e ordenar em memoria apos agregacao.

### 3.2) Modelagem e DDL
- **Contexto:** o banco precisa suportar importacao, integridade e analises.
- **Pros:** normalizacao reduz redundancia e evita repetir cadastro a cada trimestre; facilita atualizar o CADOP sem reprocessar despesas; melhora integridade com chaves/relacoes. `numeric(18,5)` evita erros de arredondamento em valores monetarios; `date` representa exatamente o que o dado fornece (sem horario).
- **Contras:** normalizacao exige joins nas consultas; `numeric` consome mais espaco e pode ser mais lento que `float`; `date` nao guarda horario caso isso fosse necessario no futuro.
- **Decisao:** modelo normalizado (CADOP, consolidado e agregado) porque o cadastro muda com menos frequencia que as despesas e o volume e baixo, entao os joins sao aceitaveis. Tipos `numeric(18,5)` para precisao contabel e `date` para datas cadastrais sem necessidade de horario.

### 3.3) Importacao e tratamento de inconsistencias
- **Contexto:** CSVs podem ter NULLs, strings em campos numericos e datas inconsistentes.
- **Pros:** staging evita falhas na carga inteira e permite limpeza controlada; conversoes padronizam os dados.
- **Contras:** registros invalidos sao descartados; processo tem mais etapas.
- **Decisao:** importar em `ans_stg`, converter com regex e datas em `YYYY-MM-DD`/`DD/MM/YYYY`, descartando invalidos.

### 3.4) Queries analiticas
- **Contexto:** as perguntas exigem comparacao entre periodos, mas nem todas as operadoras tem dados em todos os trimestres. Precisamos manter comparabilidade e ainda ter consultas simples de manter.
- **Pros:** 
  - **Query 1:** usar o primeiro e o ultimo trimestre globais garante que o crescimento seja comparado no mesmo intervalo para todas as operadoras que possuem os dois pontos.
  - **Query 2:** usar a tabela agregada reduz custo e responde direto “total e media por UF”.
  - **Query 3:** limitar aos 3 ultimos trimestres reduz ruido e melhora desempenho sem perder a ideia de “recente”.
- **Contras:** 
  - **Query 1:** operadoras sem dados nos extremos ficam fora.
  - **Query 2:** agregados perdem detalhe por trimestre.
  - **Query 3:** media geral pode esconder sazonalidade e diferencas por trimestre.
- **Decisao por query:**
  - **Query 1:** usar o primeiro e o ultimo trimestre globais para garantir comparacao no mesmo intervalo; operadoras sem esses pontos ficam fora para nao distorcer o crescimento.
  - **Query 2:** usar a tabela agregada para responder total e media por UF com menor custo.
  - **Query 3:** usar os 3 ultimos trimestres e media geral do mesmo periodo para manter consistencia temporal e simplificar a leitura.
  - **Observacao:** mantive esse desenho porque equilibra comparabilidade, custo e clareza para avaliacao; em um cenario maior eu consideraria janelas por operadora ou series completas.
