# ANS Despesas Analytics

Projeto desenvolvido como **teste técnico de estágio**, com foco em ingestão, processamento, validação, análise e exposição de dados públicos da **ANS (Agência Nacional de Saúde Suplementar)**.

---

## 🧠 Visão Geral da Solução

A solução é composta por quatro grandes partes:

1. **ETL (Extract, Transform, Load)**  
   Responsável por baixar, processar, normalizar e consolidar dados públicos da ANS.

2. **API Backend (FastAPI)**  
   Responsável por expor os dados processados via API REST, com paginação, filtros e estatísticas.

3. **Persistência (PostgreSQL)**  
   Banco de dados utilizado para armazenar dados consolidados e agregados.

4. **Frontend (Vue.js)**
   Uma interface web em **Vue.js** será utilizada para visualização dos dados consumindo a API.

---

## 🏗️ Arquitetura (alto nível)

<!-- TODO: adicionar diagrama de arquitetura aqui -->

---

## 🛠️ Tecnologias Utilizadas

### Backend / ETL
- **Python 3.12**
- **FastAPI**
- **Uvicorn**

### Banco de Dados
- **PostgreSQL**

### Frontend
- **Vue.js** (planejado)

---

## 📦 Gerenciamento de Dependências

As dependências do projeto são gerenciadas via **pip** e isoladas em um **ambiente virtual (venv)**.

O arquivo `requirements.txt` representa um snapshot do ambiente no momento do desenvolvimento, contendo dependências diretas e indiretas.

As dependências são adicionadas **de forma incremental**, conforme a necessidade de cada etapa do projeto.

---

## ▶️ Como Executar o Projeto

### 1️⃣ Pré-requisitos
- Python 3.12+
- Git

---

### 2️⃣ Criar e ativar ambiente virtual

```bash
python -m venv .venv
```

Ativar (Windows):

```bash
.venv\Scripts\activate
```

---

### 3️⃣ Instalar dependências

```bash
pip install -r requirements.txt
```

---

### 4️⃣ Executar a API

```bash
uvicorn main:app --reload
```

A API ficará disponível em:

```
http://127.0.0.1:8000
```

---

### 5️⃣ Verificação rápida

Endpoint de saúde:

```
GET /health
```

Resposta esperada:

```json
{ "status": "ok" }
```

Documentação automática da API (Swagger):

```
http://127.0.0.1:8000/docs
```

---

## 🔁 ETL e Processamento de Dados (em evolução)

O processo de ETL será responsável por:

* Download dos dados públicos da ANS
* Extração automática de arquivos ZIP
* Processamento de arquivos CSV, TXT e XLSX
* Normalização de estruturas de dados heterogêneas
* Consolidação dos dados dos últimos 3 trimestres disponíveis
* Geração de arquivos CSV finais para análise e persistência

As decisões relacionadas ao tratamento de inconsistências, como:

* CNPJs inválidos
* Valores zerados ou negativos
* Datas em formatos inconsistentes
* Registros duplicados

serão documentadas conforme a implementação, conforme solicitado no teste técnico.

---

## ⚖️ Trade-offs Técnicos

Ao longo do desenvolvimento, foram (e serão) considerados diversos trade-offs técnicos, incluindo, mas não se limitando a:

* Processamento de dados em memória vs processamento incremental
* Estratégias de validação e tratamento de dados inválidos
* Normalização vs desnormalização no banco de dados
* Estratégias de paginação na API
* Formas de agregação e ordenação de grandes volumes de dados

Todas as decisões serão justificadas no contexto do problema, destacando prós, contras e impactos técnicos.

---

## 📌 Observações Finais

* O projeto prioriza clareza, simplicidade e organização
* Nem todas as funcionalidades serão implementadas de forma exaustiva
* A documentação faz parte do escopo de avaliação
* O foco está na qualidade das decisões técnicas, não na quantidade de código

