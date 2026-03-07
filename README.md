# Workshop Hands-On Databricks - Panvel

Workshop prático de Databricks personalizado para o time de Farmácias Panvel.

---

## Arquitetura do Workshop

```
┌─────────────────────────────────────────────────────────────────────┐
│                        WORKSHOP PANVEL                              │
│                                                                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐      │
│  │  Lab 1   │    │  Lab 2   │    │  Lab 3   │    │  Lab 4   │      │
│  │   SDP    │───▶│   Jobs   │───▶│    ML    │───▶│  AI/BI   │      │
│  │(Pipeline)│    │(Workflow)│    │(MLflow)  │    │(Genie +  │      │
│  │          │    │          │    │          │    │Dashboard)│      │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘      │
│                                                                     │
│  Dados:  JSON Streaming ──▶ Bronze ──▶ Silver ──▶ Gold             │
└─────────────────────────────────────────────────────────────────────┘
```

## Pré-requisitos

- Acesso a um workspace Databricks com Unity Catalog habilitado
- Permissão para criar catálogos e schemas
- Cluster com Databricks Runtime 14.0+ (ou Serverless)
- Acesso a um Warehouse SQL (para Lab 4)

## Estrutura do Projeto

```
workshop-panvel/
├── README.md
├── 00_Setup/
│   ├── 00_configuracao_catalogo.py      # Criação do catálogo personalizado
│   └── 01_dados_cadastrais.py           # Geração dos dados sintéticos estáticos
├── 01_Lab_SDP/
│   ├── 01a_gerador_vendas_streaming.py  # Gerador de JSONs de vendas (streaming)
│   ├── 01b_sdp_pipeline_completo.py     # Pipeline DLT completo (referência)
│   └── 01c_sdp_pipeline_todo.py         # Pipeline DLT com TO-DOs (exercício)
├── 02_Lab_Jobs/
│   ├── 02a_workflow_completo.py         # Workflow completo (referência)
│   └── 02b_workflow_todo.py             # Workflow com TO-DOs (exercício)
├── 03_Lab_ML/
│   ├── 03a_ml_completo.py              # ML completo (referência)
│   └── 03b_ml_todo.py                  # ML com TO-DOs (exercício)
├── 04_Lab_AIBI/
│   ├── 04a_genie_dashboard_completo.py  # AI/BI completo (referência)
│   └── 04b_genie_dashboard_todo.py      # AI/BI com TO-DOs (exercício)
```

## Como Começar

### Passo 1: Importar os notebooks

1. No Databricks, vá em **Workspace** > **Users** > seu usuário
2. Clique em **Import** e selecione a pasta do workshop ou importe via URL do repositório Git

### Passo 2: Configurar seu catálogo personalizado

1. Abra o notebook `00_Setup/00_configuracao_catalogo.py`
2. Preencha o widget **nome_participante** com seu primeiro nome (sem espaços, sem acentos)
   - Exemplo: `joao`, `maria`, `carlos`
3. Execute todas as células
4. Seu catálogo será criado como: `workshop_panvel_<seu_nome>`

### Passo 3: Gerar dados cadastrais

1. Abra o notebook `00_Setup/01_dados_cadastrais.py`
2. Preencha o widget **nome_participante** com o mesmo nome usado no Passo 2
3. Execute todas as células
4. Verifique no Catalog Explorer se as tabelas foram criadas:
   - `clientes` (1.000 registros)
   - `lojas` (~120 lojas)
   - `produtos` (400 produtos)

---

## Lab 1: Serverless Delta Pipeline (SDP)

**Objetivo:** Criar um pipeline DLT que ingere dados de vendas em streaming e processa nas camadas Bronze, Silver e Gold.

### Instruções:

1. **Inicie o gerador de dados:**
   - Abra `01_Lab_SDP/01a_gerador_vendas_streaming.py`
   - Configure o widget `nome_participante`
   - Execute o notebook - ele gerará JSONs de vendas a cada 20 segundos
   - **Deixe rodando** enquanto trabalha no pipeline

2. **Crie o pipeline DLT:**
   - Abra `01_Lab_SDP/01c_sdp_pipeline_todo.py` (versão com exercícios)
   - Complete os TO-DOs seguindo as dicas nos comentários
   - Referência: `01b_sdp_pipeline_completo.py`

3. **Configure o pipeline no Databricks:**
   - Vá em **Workflows** > **Delta Live Tables** > **Create Pipeline**
   - Nome: `pipeline_panvel_<seu_nome>`
   - Source: selecione o notebook `01c_sdp_pipeline_todo.py`
   - Target catalog: `workshop_panvel_<seu_nome>`
   - Target schema: `silver` e `gold` (configurado no notebook)
   - Pipeline mode: **Triggered** (para teste) ou **Continuous**

### O que você vai aprender:
- Auto Loader para ingestão de dados JSON
- Transformações com DLT (Delta Live Tables)
- Camadas Bronze, Silver e Gold (Medallion Architecture)
- Extração de dados de strings (bairro a partir do nome da loja)

---

## Lab 2: Workflows / Jobs

**Objetivo:** Criar um workflow orquestrando múltiplas tarefas com dependências.

### Instruções:

1. Abra `02_Lab_Jobs/02b_workflow_todo.py` (versão com exercícios)
2. Complete os TO-DOs
3. Crie o Workflow no Databricks:
   - Vá em **Workflows** > **Create Job**
   - Adicione as tarefas conforme instruções no notebook
4. Referência: `02a_workflow_completo.py`

### O que você vai aprender:
- Criação de workflows multi-tarefa
- Dependências entre tarefas
- Agendamento e monitoramento
- Notificações e alertas

---

## Lab 3: Machine Learning

**Objetivo:** Criar um modelo de segmentação de clientes usando dados de vendas (RFM + Clustering).

### Instruções:

1. Abra `03_Lab_ML/03b_ml_todo.py` (versão com exercícios)
2. Complete os TO-DOs para:
   - Criar features RFM (Recency, Frequency, Monetary)
   - Treinar modelo de clustering (K-Means)
   - Registrar modelo no MLflow / Unity Catalog
3. Referência: `03a_ml_completo.py`

### O que você vai aprender:
- Feature Engineering com Spark SQL
- Treinamento de modelos com scikit-learn
- Tracking de experimentos com MLflow
- Registro de modelos no Unity Catalog

---

## Lab 4: AI/BI - Genie + Dashboard

**Objetivo:** Criar um Genie Room e um Dashboard Lakeview para análise de vendas.

### Instruções:

1. Abra `04_Lab_AIBI/04b_genie_dashboard_todo.py` (versão com exercícios)
2. Complete os TO-DOs
3. Crie o Genie Room e o Dashboard conforme instruções
4. Referência: `04a_genie_dashboard_completo.py`

### O que você vai aprender:
- Criação de Genie Rooms com instruções customizadas
- Construção de dashboards Lakeview
- Visualizações interativas
- Perguntas em linguagem natural sobre dados

---

## Dicas Gerais

- Sempre use o **mesmo nome_participante** em todos os notebooks
- Se algo der errado, execute novamente o notebook de setup (`00_configuracao_catalogo.py`)
- Use a versão **completa** como referência quando travar em algum TO-DO
- Os dados de streaming continuam sendo gerados enquanto o notebook gerador estiver rodando
- Para parar a geração de dados, interrompa o notebook `01a_gerador_vendas_streaming.py`

## Limpeza (Pós-Workshop)

Para limpar todos os recursos criados, execute:

```sql
-- Substitua <seu_nome> pelo nome usado no workshop
DROP CATALOG IF EXISTS workshop_panvel_<seu_nome> CASCADE;
```

E remova o volume de dados:
```python
dbutils.fs.rm(f"/Volumes/workshop_panvel_<seu_nome>/raw/vendas_json/", True)
```

---

**Bom workshop! Qualquer dúvida, chame o facilitador.**
