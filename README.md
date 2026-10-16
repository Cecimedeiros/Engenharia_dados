# 🌤️ Pipeline ETL - Previsão Meteorológica (Open-Meteo)

## 🎯 Objetivo

Este repositório foi criado para a construção de uma pipeline de **Engenharia de Dados** desenvolvida ao longo do semestre letivo.

O objetivo principal é aplicar os conhecimentos da disciplina através da implementação de um fluxo completo de ETL (*Extract, Transform, Load*) em Python, utilizando Orientação a Objetos para coletar, tratar e armazenar dados de previsão meteorológica.

> ℹ️ **Nota:** Este repositório é evolutivo e vem sofrendo alterações contínuas mediante a exposição a novos conhecimentos e práticas de Engenharia de Dados.

---

## 🔄 Fluxo da Pipeline ETL

A arquitetura da solução segue obrigatoriamente a seguinte sequência de execução:

```text
                 ┌─────────────────────┐
                 │   API Open-Meteo    │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │     MongoDB Atlas   │
                 │                     │
                 │     JSON bruto      │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   Transformação     │
                 │       Pandas        │
                 └──────────┬──────────┘
                   ┌────────┴────────┐
                   │                 │
                   ▼                 ▼
 ┌─────────────────────┐ ─────── ┌───────────────────────┐
 │    SQLite Local     │         │       Neon DB         │                            
 │  Testes e Validação │         │  PostgreSQL Relacional│            
 └─────────────────────┘         └───────────────────────┘    
```

### 1. Extract — Extração dos dados

**Fonte:** API Open-Meteo Forecast API.

Consumo dos dados brutos de previsão meteorológica por meio de requisições HTTP em Python, utilizando a biblioteca `Requests`.

Os dados são coletados para as cidades configuradas na pipeline, respeitando o horizonte de previsão e o fuso horário definidos no projeto.

### 2. Armazenamento dos dados brutos

**Banco:** MongoDB Atlas.

Os dados JSON retornados pela API são armazenados inicialmente no MongoDB Atlas, que atua como uma camada de armazenamento bruto.

Essa etapa tem como finalidade:

* Garantir a persistência dos dados originais.
* Preservar a estrutura JSON retornada pela API.
* Permitir a recuperação dos dados para as etapas posteriores.
* Evitar a perda da estrutura original durante o processo de transformação.

### 3. Transform — Transformação dos dados

**Tecnologia:** Pandas.

Os dados brutos são recuperados do MongoDB Atlas e transformados em um DataFrame para aplicação das regras de negócio.

As principais transformações realizadas são:

#### Limpeza dos dados

* Remoção de valores nulos utilizando `dropna()`.
* Tratamento dos registros para garantir maior consistência dos dados.

#### Conversão de data e hora

* Conversão dos campos de data/hora utilizando `pd.to_datetime`.
* Formatação dos dados temporais para facilitar o armazenamento e a análise.

#### Identificação da cidade

* Adição da coluna de cidade selecionada em todas as células correspondentes aos registros meteorológicos.

#### Padronização do texto da cidade

A coluna de cidade passa por um tratamento para padronização dos nomes:

1. Remoção de acentos utilizando decomposição NFKD ou `unidecode`.
2. Conversão dos caracteres para letras minúsculas utilizando `lower()`.
3. Substituição de espaços por underline utilizando `replace(" ", "_")`.

Exemplo:

```text
São Paulo → sao_paulo
Rio de Janeiro → rio_de_janeiro
Recife → recife
```

### 4. Load — Carregamento dos dados

Após a transformação, os dados são carregados nos bancos relacionais definidos no projeto.

#### NeonDB — Banco de dados em nuvem

O NeonDB é utilizado como banco de dados PostgreSQL relacional em nuvem.

Sua finalidade é armazenar de forma definitiva os dados finais, limpos e estruturados, permitindo consultas e análises posteriores.

#### SQLite — Banco de dados local

O SQLite é utilizado localmente para:

* Testes da pipeline.
* Validações rápidas.
* Apoio ao desenvolvimento.
* Verificação do carregamento dos dados.

> O SQLite possui finalidade de apoio local, enquanto o NeonDB é utilizado como destino relacional em nuvem.

---

## 🌍 Parâmetros e Cobertura

### API Fonte

**Open-Meteo Forecast API**

A API fornece dados de previsão meteorológica utilizados como fonte de extração da pipeline.

### Horizonte de previsão

**7 dias de forecast**, contando a partir do dia em que o script é executado.

### Fuso horário

```text
America/Sao_Paulo (GMT-3)
```

### Cidades disponíveis para consulta

| Cidade            | Latitude | Longitude |
| ----------------- | -------: | --------: |
| 📍 sao_paulo      | -23.5505 |  -46.6333 |
| 📍 rio_de_janeiro | -22.9068 |  -43.1729 |
| 📍 recife         |  -8.0492 |  -34.9231 |

---

## ⚙️ Configuração do Ambiente

Siga os passos abaixo para criar o ambiente virtual e executar a pipeline na sua máquina.

### 1. Criando o ambiente virtual

Abra o terminal na raiz do projeto e execute:

```bash
python -m venv venv
```

### 2. Ativando o ambiente virtual

#### Windows PowerShell

```powershell
venv\Scripts\Activate.ps1
```

#### Linux / Mac

```bash
source venv/bin/activate
```

### 3. Instalando as dependências

Com o ambiente virtual ativado, execute:

```bash
pip install -r requirements.txt
```

---

## 🔐 Variáveis de Ambiente (.env)

Crie um arquivo `.env` na raiz do projeto para armazenar as credenciais de acesso aos bancos de dados.

### Estrutura do arquivo

```env
MONGODB_URI="sua_string_de_conexao_mongodb_atlas"
NEON_DATABASE_URL="sua_string_de_conexao_postgresql_neon"
```

### Exemplo de estrutura do projeto

```text
📁 projeto/
│
├── 📁 venv/
├── 📁 src/
│
├── 📄 runETL.py
├── 📄 requirements.txt
├── 📄 orchestrate_prefect.py
├── 📄 .env
├── 📄 .gitignore
└── 📄 README.md
```

> ⚠️ **Importante:** Não compartilhe suas credenciais de banco de dados publicamente. O arquivo `.env` deve ser incluído no `.gitignore` para evitar que suas informações de acesso sejam enviadas ao GitHub.

---

## ▶️ Execução da Pipeline

Para executar todo o fluxo de ETL, incluindo:

1. Extração dos dados da API Open-Meteo.
2. Armazenamento dos dados brutos no MongoDB Atlas.
3. Recuperação dos dados do MongoDB.
4. Transformação e limpeza com Pandas.
5. Carregamento dos dados finais no NeonDB.
6. Apoio local com SQLite.

### Opção 1: Execução Padrão (Script Tradicional)

Execute o fluxo de dados em sequência direta através do script Python tradicional:

```bash
python runETL.py
```
### Opção 2: Execução Orquestrada com Prefect

Para iniciar o servidor, configurar a URL da API, e executar a orquestação (para visualizar os gráficos, o status das Tasks e logs em tempo real):

```bash

prefect server start
prefect config set PREFECT_API_URL=[http://127.0.0.1:4200/api](http://127.0.0.1:4200/api)
python orchestrate_prefect.py
```
---

## 🛠️ Tecnologias Utilizadas

| Tecnologia                | Finalidade                                                                        |
| ------------------------- | --------------------------------------------------------------------------------- |
| **Python 3.10+**          | Linguagem principal da pipeline, com foco em Orientação a Objetos.                |
| **Pandas**                | Manipulação, limpeza e transformação dos dados meteorológicos.                    |
| **Prefect**               | Orquestração de workflows, gestão de retentativas (retries), logs e monitoramento.|
| **Requests**              | Realização de requisições HTTP à API Open-Meteo.                                  |
| **MongoDB Atlas**         | Banco NoSQL utilizado como Landing Zone para armazenamento dos dados JSON brutos. |
| **PyMongo**               | Conexão e comunicação com o MongoDB Atlas.                                        |
| **NeonDB / PostgreSQL**   | Banco de dados relacional em nuvem para armazenamento dos dados finais.           |
| **SQLAlchemy / psycopg2** | Conexão e integração com o banco PostgreSQL.                                      |
| **SQLite**                | Banco relacional local utilizado para testes e validações.                        |

---

## 📌 Conceitos Aplicados

Durante o desenvolvimento da pipeline, são aplicados conceitos de:

* Engenharia de Dados.
* Arquitetura ETL.
* Extração de dados via API.
* Armazenamento de dados brutos.
* Bancos de dados NoSQL.
* Bancos de dados relacionais.
* Limpeza e transformação de dados.
* Manipulação de DataFrames com Pandas.
* Orientação a Objetos em Python.
* Variáveis de ambiente.
* Persistência de dados.
* Integração entre diferentes bancos de dados.

---

## 👥 Integrantes

* Alvaro Silva
* Arthur Estevão
* Beatriz Paredes
* Cecília Medeiros
* Gabriel Souza
* Icaro Silva
* Isabella Batista
* Jose Leandro De Morais
* Melissa Filgueiras
* Thays Barbosa

---

## 📚 Sobre o Projeto

Projeto acadêmico desenvolvido ao longo do semestre letivo, com foco na aplicação prática dos conhecimentos de Engenharia de Dados através da construção de uma pipeline ETL utilizando Python, MongoDB Atlas, Pandas, NeonDB e SQLite.

**Status:** Em desenvolvimento contínuo 🚀

## Uso de Inteligência Artificial

Declaro que utilizei a ferramenta de Inteligência Artificial ChatGPT (modelo GPT-4o / GPT-4), Google Gemini (modelo Gemini 1.5 Pro) e Claude (modelo Claude 3.5 Sonnet) ao longo dessa atividade, com a finalidade de gerar um rascunho sobre orquestração de pipelines de ETL, resiliência de workflows e boas práticas em Engenharia de Dados, sobre o qual realizei análise crítica (curadoria) das informações apresentadas. Não tratei dados pessoais no uso desta ferramenta. Revisei criticamente o conteúdo gerado, identificando seus acertos e eventuais imprecisões, e assumo a responsabilidade integral pela versão final apresentada.
