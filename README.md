<img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_workshop_panvel.png">

# Workshop Hands-On Databricks | Grupo Panvel

Workshop prático de Databricks personalizado para o time de **Grupo Panvel**, com foco em Data Engineering, Machine Learning e Analytics.

</br>

## Apresentadores

<table>
  <tr>
    <td align="center" width="50%">
      <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/juliandro_circle.png" width="150"/><br>
      <strong>Juliandro Figueiró</strong><br>
      <em>Solutions Architect</em><br>
      <em>Databricks</em>
    </td>
    <td align="center" width="50%">
      <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/jean_circle.png" width="150"/><br>
      <strong>Jean Ertzogue</strong><br>
      <em>Account Executive</em><br>
      <em>Databricks</em>
    </td>
  </tr>
</table>

</br>

## Ementa do Workshop

| # | Lab | Tópicos | Duração |
| -- | -- | -- | -- |
| 00 | **Setup** | Configuração do catálogo personalizado e geração de dados sintéticos | 15 min |
| 01 | **SDP - Pipeline DLT** | Auto Loader, Delta Live Tables, Medallion Architecture (Bronze/Silver/Gold) | 40 min |
| 02 | **Workflows / Jobs** | Orquestração multi-tarefa, dependências, agendamento | 25 min |
| 03 | **Machine Learning** | Segmentação de clientes (RFM + K-Means), MLflow, Unity Catalog | 35 min |
| 04 | **AI/BI** | Genie (linguagem natural) + AI/BI Dashboard | 30 min |
|    | **Encerramento** | Considerações finais e perguntas | 15 min |

</br>

## Arquitetura

```
  JSON Vendas (Streaming)
         │
         ▼
  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────────┐
  │   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │────▶│   AI/BI + ML    │
  │  Auto Loader│     │  Limpeza    │     │  Agregações │     │  Genie          │
  │  Dados Brutos│     │  Enriquec.  │     │  KPIs       │     │  Dashboard      │
  │             │     │  Bairro     │     │  Rankings   │     │  Segmentação    │
  └─────────────┘     └─────────────┘     └─────────────┘     └─────────────────┘
```

</br>

## Pré-requisitos

| Requisito | Detalhes |
| -- | -- |
| Workspace | Databricks com **Unity Catalog** habilitado |
| Compute | Cluster DBR 14.0+ ou **Serverless** |
| SQL Warehouse | Necessário para Lab 04 (AI/BI) |

### Permissões necessárias por Lab

| Permissão | Recurso | Labs |
| -- | -- | -- |
| `CREATE CATALOG` | `workshop_panvel_<nome>` | 00 |
| `CREATE SCHEMA` | `raw`, `bronze`, `silver`, `gold`, `ml` | 00 |
| `USE CATALOG` / `USE SCHEMA` | Catálogo e schemas do participante | Todos |
| `CREATE TABLE` / `SELECT` / `MODIFY` | Tabelas em todos os schemas | Todos |
| `CREATE VOLUME` / `READ` / `WRITE` | Volume `raw.vendas_json` | 01 |
| `CREATE PIPELINE` / `MANAGE` / `RUN` | Pipeline DLT `pipeline_panvel_<nome>` | 01, 02 |
| `CREATE JOB` / `RUN` / `MODIFY` | Workflow `workflow_panvel_<nome>` | 02 |
| `CREATE EXPERIMENT` / `LOG` | Experimento MLflow `workshop_panvel_<nome>_rfm` | 03 |
| `REGISTER MODEL` | Modelo em Unity Catalog (`ml.modelo_segmentacao_clientes`) | 03 |
| `CREATE GENIE` / `USE GENIE` | Genie Room com tabelas Gold | 04 |
| `CREATE DASHBOARD` | AI/BI Dashboard | 04 |
| `USE CLUSTER` / `ATTACH` | Cluster ou Serverless | Todos |

> **Dica:** O perfil de permissões mais simples é conceder **`USE CATALOG`** + **`ALL PRIVILEGES`** no catálogo pessoal do participante, além de acesso a compute e às funcionalidades de Workflows, Genie e Dashboards.

</br>

## Estrutura do Projeto

```
workshop-panvel/
│
├── 00_Setup/
│   ├── 00_configuracao_catalogo.py       # Criação do catálogo personalizado
│   └── 01_dados_cadastrais.py            # Geração de 1.000 clientes, ~120 lojas, 400 produtos
│
├── 01_Lab_SDP/
│   ├── 01a_gerador_vendas_streaming.py   # Gerador de JSONs (1 por loja a cada 20s)
│   ├── 01b_sdp_pipeline_completo.py      # Pipeline DLT completo (referência)
│   └── 01c_sdp_pipeline_todo.py          # Pipeline DLT com TO-DOs (exercício)
│
├── 02_Lab_Jobs/
│   ├── 02a_workflow_completo.py          # Workflow completo (referência)
│   └── 02b_workflow_todo.py              # Workflow com TO-DOs (exercício)
│
├── 03_Lab_ML/
│   ├── 03a_ml_completo.py               # ML completo (referência)
│   └── 03b_ml_todo.py                   # ML com TO-DOs (exercício)
│
├── 04_Lab_AIBI/
│   ├── 04a_genie_dashboard_completo.py   # AI/BI completo (referência)
│   └── 04b_genie_dashboard_todo.py       # AI/BI com TO-DOs (exercício)
```

</br>

## Como Começar

### Passo 1: Importar os notebooks

1. No Databricks, vá em **Workspace** > **Users** > seu usuário
2. Clique em **Import** > selecione **URL** e cole o link deste repositório
3. Ou clone via Git: `Repos` > `Add Repo` > cole a URL do GitHub

### Passo 2: Configurar seu catálogo personalizado

1. Abra o notebook `00_Setup/00_configuracao_catalogo.py`
2. Preencha o widget **nome_participante** com seu primeiro nome
   > ⚠️ Sem espaços, sem acentos, minúsculo. Ex: `joao`, `maria`, `carlos`
3. Execute todas as células
4. Seu catálogo será criado como: `workshop_panvel_<seu_nome>`

### Passo 3: Gerar dados cadastrais

1. Abra o notebook `00_Setup/01_dados_cadastrais.py`
2. Use o **mesmo nome** do Passo 2
3. Execute todas as células
4. Verifique no **Catalog Explorer**:

| Tabela | Registros | Descrição |
| -- | -- | -- |
| `clientes` | 1.000 | Clientes em 50 cidades do RS |
| `lojas` | ~120 | Lojas "Panvel - Bairro" |
| `produtos` | 400 | Produtos em 20 categorias de farmácia |

</br>

---

## Lab 01 — Serverless Delta Pipeline (SDP)

| Item | Detalhes |
| -- | -- |
| **Objetivo** | Criar pipeline DLT com ingestão streaming e arquitetura Medallion |
| **Notebook (exercício)** | `01_Lab_SDP/01c_sdp_pipeline_todo.py` |
| **Notebook (referência)** | `01_Lab_SDP/01b_sdp_pipeline_completo.py` |
| **Gerador de dados** | `01_Lab_SDP/01a_gerador_vendas_streaming.py` |

### Instruções

1. **Inicie o gerador de dados** — execute `01a_gerador_vendas_streaming.py` e **deixe rodando**
2. **Complete os TO-DOs** no notebook `01c_sdp_pipeline_todo.py`:

| TO-DO | Descrição | Dica |
| -- | -- | -- |
| 1 | Criar tabela `bronze_produtos` | Siga o padrão de `bronze_clientes` |
| 2 | Extrair ano, mês e dia | Funções `year()`, `month()`, `dayofmonth()` |
| 3 | Extrair bairro do nome da loja | `regexp_replace()` para remover "Panvel - " |
| 4 | Explodir itens do JSON | Função `explode("itens")` |
| 5 | Agregações por categoria | `groupBy` + `agg` com `sum`, `count`, `avg` |
| 6 | Criar `gold_vendas_por_cidade` | Siga o padrão de `gold_vendas_por_loja` |

3. **Configure o pipeline**: Workflows > Delta Live Tables > Create Pipeline

### Conceitos abordados
- Auto Loader / CloudFiles
- Delta Live Tables (DLT)
- Streaming Tables vs Materialized Views
- Data Quality Expectations
- Medallion Architecture

</br>

---

## Lab 02 — Workflows / Jobs

| Item | Detalhes |
| -- | -- |
| **Objetivo** | Criar workflow multi-tarefa com dependências e agendamento |
| **Notebook (exercício)** | `02_Lab_Jobs/02b_workflow_todo.py` |
| **Notebook (referência)** | `02_Lab_Jobs/02a_workflow_completo.py` |

### Instruções

1. **Complete os TO-DOs** no notebook `02b_workflow_todo.py`
2. **Crie o Workflow** no Databricks UI:

| Tarefa | Tipo | Dependência |
| -- | -- | -- |
| Validação | Notebook | Nenhuma |
| Pipeline DLT | DLT Pipeline | Validação |
| Qualidade | Notebook | Pipeline DLT |
| Resumo | Notebook | Qualidade |

3. **Configure o agendamento** (a cada 30 minutos)

### Conceitos abordados
- Databricks Workflows
- Task Dependencies (DAG)
- Parametrização de notebooks
- Scheduling e monitoramento

</br>

---

## Lab 03 — Machine Learning

| Item | Detalhes |
| -- | -- |
| **Objetivo** | Segmentação de clientes usando RFM + K-Means |
| **Notebook (exercício)** | `03_Lab_ML/03b_ml_todo.py` |
| **Notebook (referência)** | `03_Lab_ML/03a_ml_completo.py` |

### O que é RFM?

| Métrica | Pergunta |
| -- | -- |
| **R**ecency | Há quantos dias o cliente fez a última compra? |
| **F**requency | Quantas compras o cliente fez? |
| **M**onetary | Quanto o cliente gastou no total? |

### Instruções

1. **Complete os TO-DOs** no notebook `03b_ml_todo.py`:

| TO-DO | Descrição |
| -- | -- |
| 1 | Calcular métricas RFM por cliente |
| 2 | Montar VectorAssembler |
| 3 | Treinar K-Means (K=3,4,5,6) com MLflow |
| 4 | Analisar perfil dos segmentos |
| 5 | Salvar tabela de segmentação |

2. **Acompanhe no MLflow**: Experiments > `workshop_panvel_<nome>_rfm`
3. **Verifique o modelo** registrado no Unity Catalog

### Conceitos abordados
- Feature Engineering com PySpark
- K-Means Clustering + Silhouette Score
- MLflow Tracking e Model Registry
- Unity Catalog para modelos

</br>

---

## Lab 04 — AI/BI: Genie + Dashboard

| Item | Detalhes |
| -- | -- |
| **Objetivo** | Criar Genie e AI/BI Dashboard para análise de vendas |
| **Notebook (exercício)** | `04_Lab_AIBI/04b_genie_dashboard_todo.py` |
| **Notebook (referência)** | `04_Lab_AIBI/04a_genie_dashboard_completo.py` |

### Instruções

1. **Complete os TO-DOs** no notebook `04b_genie_dashboard_todo.py`
2. **Crie o Genie**: AI/BI > Genie > New Genie
3. **Crie o Dashboard**: AI/BI > Dashboards > Create Dashboard
4. **Teste o Genie** com perguntas como:
   - *"Qual loja tem o maior faturamento?"*
   - *"Quais são os 5 produtos mais vendidos?"*
   - *"Compare Porto Alegre com Caxias do Sul"*

### Conceitos abordados
- AI/BI Genie (linguagem natural)
- AI/BI Dashboard
- Visualizações: Counter, Bar, Pie, Table
- Instruções customizadas para contexto

</br>

---

## Dicas Importantes

> **Use sempre o mesmo `nome_participante`** em todos os notebooks para garantir que seu catálogo seja consistente.

> **Se travar em algum TO-DO**, consulte a versão completa do notebook (sufixo `_completo.py`).

> **O gerador de vendas** precisa ficar rodando durante o Lab 1. Para parar, interrompa o notebook.

</br>

## Limpeza (Pós-Workshop)

```sql
-- Substitua <seu_nome> pelo nome usado no workshop
DROP CATALOG IF EXISTS workshop_panvel_<seu_nome> CASCADE;
```

</br>

## Referências

* [Documentação Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html)
* [Documentação Databricks Workflows](https://docs.databricks.com/workflows/index.html)
* [MLflow no Databricks](https://docs.databricks.com/mlflow/index.html)
* [AI/BI Genie](https://docs.databricks.com/genie/index.html)
* [AI/BI Dashboards](https://docs.databricks.com/dashboards/index.html)
* [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)

</br>

---

<p align="center">
  <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/panvel_logo.png" height="50" alt="Panvel">
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
  <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/databricks_logo.png" height="45" alt="Databricks">
</p>

<p align="center">
  <strong>Workshop Hands-On Databricks — Grupo Panvel</strong><br>
  <em>Data & AI na prática</em>
</p>
