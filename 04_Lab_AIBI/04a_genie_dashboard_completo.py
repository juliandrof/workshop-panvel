# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Panvel - Lab 4: AI/BI - Genie + Dashboard (Versão Completa)
# MAGIC
# MAGIC Neste lab, vamos:
# MAGIC 1. Preparar dados para o AI/BI Genie
# MAGIC 2. Criar um Genie Room com instruções customizadas
# MAGIC 3. Construir um Dashboard Lakeview

# COMMAND ----------

dbutils.widgets.text("nome_participante", "", "Seu Nome (sem espaços/acentos)")

# COMMAND ----------

nome = dbutils.widgets.get("nome_participante").strip().lower().replace(" ", "_")
assert nome != "", "Por favor, preencha seu nome no widget acima!"
catalog_name = f"workshop_panvel_{nome}"
spark.sql(f"USE CATALOG {catalog_name}")
print(f"Usando catálogo: {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparar Views otimizadas para AI/BI

# COMMAND ----------

# View de resumo de vendas por loja e período
spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog_name}.gold.vw_vendas_resumo AS
    SELECT
        v.id_venda,
        v.data_venda,
        v.ano,
        v.mes,
        v.dia,
        v.nome_cliente,
        v.cidade_cliente,
        v.nome_loja,
        v.cidade_loja,
        v.valor_total,
        -- Extrair bairro do nome da loja
        regexp_replace(regexp_replace(v.nome_loja, '^Panvel - ', ''), '\\\\s+\\\\d+$', '') as bairro_loja
    FROM {catalog_name}.silver.silver_vendas v
""")
print("View vw_vendas_resumo criada!")

# COMMAND ----------

# View de vendas detalhadas com produtos
spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog_name}.gold.vw_vendas_produtos AS
    SELECT
        iv.id_venda,
        iv.data_venda,
        iv.id_loja,
        iv.id_cliente,
        iv.id_produto,
        iv.nome_produto,
        iv.categoria,
        iv.quantidade,
        iv.valor_unitario,
        iv.valor_total,
        iv.desconto,
        l.nome_loja,
        l.cidade as cidade_loja,
        c.nome as nome_cliente,
        c.cidade as cidade_cliente
    FROM {catalog_name}.silver.silver_itens_venda iv
    LEFT JOIN {catalog_name}.raw.lojas l ON iv.id_loja = l.id_loja
    LEFT JOIN {catalog_name}.raw.clientes c ON iv.id_cliente = c.id_cliente
""")
print("View vw_vendas_produtos criada!")

# COMMAND ----------

# View de segmentação de clientes (do Lab 3)
try:
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog_name}.gold.vw_segmentacao_clientes AS
        SELECT
            s.id_cliente,
            s.nome_cliente,
            s.cidade_cliente,
            s.recency as dias_desde_ultima_compra,
            s.frequency as total_compras,
            s.monetary as valor_total_gasto,
            s.ticket_medio,
            s.segmento,
            CASE s.segmento
                WHEN 0 THEN 'Cliente Premium'
                WHEN 1 THEN 'Cliente Regular'
                WHEN 2 THEN 'Cliente Eventual'
                WHEN 3 THEN 'Cliente em Risco'
                WHEN 4 THEN 'Cliente Novo'
                ELSE 'Não Classificado'
            END as nome_segmento
        FROM {catalog_name}.ml.segmentacao_clientes s
    """)
    print("View vw_segmentacao_clientes criada!")
except:
    print("AVISO: Tabela de segmentação não encontrada. Execute o Lab 3 primeiro.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Adicionar comentários às tabelas e colunas
# MAGIC
# MAGIC Os comentários ajudam o Genie a entender os dados.

# COMMAND ----------

# Comentários nas tabelas Gold
spark.sql(f"""
    COMMENT ON TABLE {catalog_name}.gold.gold_vendas_por_loja IS
    'Tabela com métricas agregadas de vendas por loja Panvel, incluindo faturamento total, ticket médio e número de clientes únicos'
""")

spark.sql(f"""
    COMMENT ON TABLE {catalog_name}.gold.gold_vendas_por_categoria IS
    'Tabela com métricas de vendas por categoria de produto de farmácia, incluindo quantidade vendida, faturamento e desconto total'
""")

spark.sql(f"""
    COMMENT ON TABLE {catalog_name}.gold.gold_vendas_por_cidade IS
    'Tabela com métricas de vendas por cidade do Rio Grande do Sul, incluindo número de lojas, faturamento e clientes'
""")

spark.sql(f"""
    COMMENT ON TABLE {catalog_name}.gold.gold_top_produtos IS
    'Ranking dos produtos mais vendidos no Grupo Panvel com quantidade, faturamento e desconto médio'
""")

print("Comentários adicionados às tabelas!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criar Genie Room
# MAGIC
# MAGIC ### Passo a passo no Databricks UI:
# MAGIC
# MAGIC 1. Vá em **AI/BI** > **Genie Rooms** > **New Genie Room**
# MAGIC 2. Nome: `Análise Panvel - <seu_nome>`
# MAGIC 3. **Adicione as tabelas:**
# MAGIC    - `workshop_panvel_<nome>.gold.gold_vendas_por_loja`
# MAGIC    - `workshop_panvel_<nome>.gold.gold_vendas_por_categoria`
# MAGIC    - `workshop_panvel_<nome>.gold.gold_vendas_por_cidade`
# MAGIC    - `workshop_panvel_<nome>.gold.gold_top_produtos`
# MAGIC    - `workshop_panvel_<nome>.gold.vw_vendas_resumo`
# MAGIC    - `workshop_panvel_<nome>.gold.vw_vendas_produtos`
# MAGIC    - `workshop_panvel_<nome>.gold.vw_segmentacao_clientes` (se disponível)

# COMMAND ----------

# Instruções customizadas para o Genie
instrucoes_genie = f"""
## Contexto
Você é um assistente de análise de dados para o Grupo Panvel, uma das maiores redes de farmácias do Rio Grande do Sul.
Os dados são de vendas das lojas Panvel em diversas cidades do RS.

## Regras
- Sempre formate valores monetários em Reais (R$) com separador de milhar
- Para perguntas sobre "faturamento", use a coluna "faturamento_total" ou "valor_total"
- Para perguntas sobre "ticket médio", use a coluna "ticket_medio"
- O nome das lojas segue o padrão "Panvel - Bairro" (ex: "Panvel - Centro")
- As categorias de produtos incluem: Dermocosméticos, Higiene Pessoal, Bebê e Infantil, etc.
- Quando perguntarem sobre "melhor loja", considere faturamento_total como métrica padrão
- Para perguntas sobre segmentação de clientes, use a tabela vw_segmentacao_clientes

## Exemplos de perguntas:
- "Qual loja tem o maior faturamento?"
- "Quais são os 10 produtos mais vendidos?"
- "Qual a cidade com mais vendas?"
- "Qual categoria gera mais receita?"
- "Quantos clientes Premium temos?"
- "Qual o ticket médio por cidade?"
- "Como está a distribuição de vendas por bairro em Porto Alegre?"
"""

print("=" * 60)
print("INSTRUÇÕES PARA O GENIE ROOM")
print("=" * 60)
print(instrucoes_genie)
print("=" * 60)
print("\nCopie as instruções acima e cole no campo 'Instructions' do Genie Room")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criar Dashboard Lakeview
# MAGIC
# MAGIC ### Passo a passo no Databricks UI:
# MAGIC
# MAGIC 1. Vá em **AI/BI** > **Dashboards** > **Create Dashboard**
# MAGIC 2. Nome: `Dashboard Panvel - <seu_nome>`
# MAGIC
# MAGIC ### Datasets sugeridos:
# MAGIC
# MAGIC Adicione os seguintes datasets (SQL queries):

# COMMAND ----------

# Queries prontas para o Dashboard
print("=" * 60)
print("QUERIES PARA O DASHBOARD")
print("=" * 60)

queries = {
    "KPI - Resumo Geral": f"""
SELECT
    COUNT(DISTINCT id_venda) as total_vendas,
    ROUND(SUM(valor_total), 2) as faturamento_total,
    ROUND(AVG(valor_total), 2) as ticket_medio,
    COUNT(DISTINCT id_cliente) as clientes_unicos,
    COUNT(DISTINCT id_loja) as lojas_ativas
FROM {catalog_name}.gold.vw_vendas_resumo
""",

    "Faturamento por Cidade (Top 10)": f"""
SELECT
    cidade,
    total_vendas,
    ROUND(faturamento_total, 2) as faturamento,
    ROUND(ticket_medio, 2) as ticket_medio,
    num_lojas,
    clientes_unicos
FROM {catalog_name}.gold.gold_vendas_por_cidade
ORDER BY faturamento_total DESC
LIMIT 10
""",

    "Vendas por Categoria": f"""
SELECT
    categoria,
    total_itens_vendidos,
    ROUND(faturamento_total, 2) as faturamento,
    ROUND(preco_medio, 2) as preco_medio,
    ROUND(desconto_total, 2) as desconto_total
FROM {catalog_name}.gold.gold_vendas_por_categoria
ORDER BY faturamento_total DESC
""",

    "Top 15 Produtos": f"""
SELECT
    nome_produto,
    categoria,
    quantidade_total,
    ROUND(faturamento_total, 2) as faturamento,
    num_vendas
FROM {catalog_name}.gold.gold_top_produtos
ORDER BY faturamento_total DESC
LIMIT 15
""",

    "Vendas por Loja (Top 20)": f"""
SELECT
    nome_loja,
    cidade_loja as cidade,
    total_vendas,
    ROUND(faturamento_total, 2) as faturamento,
    ROUND(ticket_medio, 2) as ticket_medio,
    clientes_unicos
FROM {catalog_name}.gold.gold_vendas_por_loja
ORDER BY faturamento_total DESC
LIMIT 20
""",

    "Distribuição de Descontos por Categoria": f"""
SELECT
    categoria,
    ROUND(desconto_total, 2) as desconto_total,
    ROUND(faturamento_total, 2) as faturamento_bruto,
    ROUND(desconto_total / faturamento_total * 100, 1) as percentual_desconto
FROM {catalog_name}.gold.gold_vendas_por_categoria
ORDER BY percentual_desconto DESC
""",
}

for nome_query, sql in queries.items():
    print(f"\n--- {nome_query} ---")
    print(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Layout sugerido para o Dashboard
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                   DASHBOARD PANVEL                          │
# MAGIC ├──────────┬──────────┬──────────┬──────────┬────────────────┤
# MAGIC │ Total    │ Fatura-  │ Ticket   │ Clientes │ Lojas          │
# MAGIC │ Vendas   │ mento    │ Médio    │ Únicos   │ Ativas         │
# MAGIC │ [KPI]    │ [KPI]    │ [KPI]    │ [KPI]    │ [KPI]          │
# MAGIC ├──────────┴──────────┴──────────┴──────────┴────────────────┤
# MAGIC │                                                             │
# MAGIC │  ┌──────────────────────┐  ┌──────────────────────────┐    │
# MAGIC │  │  Faturamento por     │  │  Vendas por Categoria    │    │
# MAGIC │  │  Cidade (Bar Chart)  │  │  (Pie Chart)             │    │
# MAGIC │  └──────────────────────┘  └──────────────────────────┘    │
# MAGIC │                                                             │
# MAGIC │  ┌──────────────────────┐  ┌──────────────────────────┐    │
# MAGIC │  │  Top 15 Produtos     │  │  Lojas Top 20            │    │
# MAGIC │  │  (Horizontal Bar)    │  │  (Table)                 │    │
# MAGIC │  └──────────────────────┘  └──────────────────────────┘    │
# MAGIC │                                                             │
# MAGIC │  ┌─────────────────────────────────────────────────────┐   │
# MAGIC │  │  Distribuição de Descontos por Categoria (Bar)      │   │
# MAGIC │  └─────────────────────────────────────────────────────┘   │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Tipos de visualização recomendados:
# MAGIC
# MAGIC | Dataset | Tipo de Visualização |
# MAGIC |---------|---------------------|
# MAGIC | KPI - Resumo Geral | Counter (5 counters) |
# MAGIC | Faturamento por Cidade | Bar Chart (horizontal) |
# MAGIC | Vendas por Categoria | Pie Chart |
# MAGIC | Top 15 Produtos | Bar Chart (horizontal) |
# MAGIC | Vendas por Loja | Table |
# MAGIC | Distribuição Descontos | Bar Chart |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Testar o Genie Room
# MAGIC
# MAGIC Após criar o Genie Room, teste com as seguintes perguntas:
# MAGIC
# MAGIC 1. **"Qual é o faturamento total?"**
# MAGIC 2. **"Quais são os 5 produtos mais vendidos?"**
# MAGIC 3. **"Qual cidade tem o maior ticket médio?"**
# MAGIC 4. **"Compare o faturamento de Porto Alegre vs Caxias do Sul"**
# MAGIC 5. **"Qual categoria de produto gera mais receita?"**
# MAGIC 6. **"Quantos clientes compraram em mais de uma loja?"**
# MAGIC 7. **"Quais bairros de Porto Alegre têm as lojas mais vendedoras?"**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parabéns! Workshop Concluído!
# MAGIC
# MAGIC Você completou todos os 4 labs do Workshop Panvel:
# MAGIC
# MAGIC - **Lab 1** - SDP: Pipeline DLT com Auto Loader (Bronze/Silver/Gold)
# MAGIC - **Lab 2** - Jobs: Workflow orquestrando múltiplas tarefas
# MAGIC - **Lab 3** - ML: Segmentação de clientes com RFM + K-Means
# MAGIC - **Lab 4** - AI/BI: Genie Room + Dashboard Lakeview
