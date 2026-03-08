# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab4.png" width="100%"/>
# MAGIC
# MAGIC **Versão Exercício** — Complete os TO-DOs para criar seu Genie e Dashboard!

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
# MAGIC ## 1. Preparar Views para AI/BI

# COMMAND ----------

# View de resumo de vendas (já pronta)
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
        regexp_replace(regexp_replace(v.nome_loja, '^Panvel - ', ''), '\\\\s+\\\\d+$', '') as bairro_loja
    FROM {catalog_name}.silver.silver_vendas v
""")
print("View vw_vendas_resumo criada!")

# COMMAND ----------

# TODO 1: Crie uma view de vendas detalhadas com produtos
# ─────────────────────────────────────────────────────────
# Dica: Crie a view {catalog_name}.gold.vw_vendas_produtos com:
#   - Campos de silver_itens_venda: id_venda, data_venda, id_loja, id_cliente,
#     id_produto, nome_produto, categoria, quantidade, valor_unitario, valor_total, desconto
#   - JOIN com raw.lojas para trazer: nome_loja, cidade (as cidade_loja)
#   - JOIN com raw.clientes para trazer: nome (as nome_cliente), cidade (as cidade_cliente)
#
# Exemplo de estrutura:
#   CREATE OR REPLACE VIEW {catalog_name}.gold.vw_vendas_produtos AS
#   SELECT
#       iv.id_venda, iv.data_venda, ...
#       l.nome_loja, l.cidade as cidade_loja,
#       c.nome as nome_cliente, c.cidade as cidade_cliente
#   FROM {catalog_name}.silver.silver_itens_venda iv
#   LEFT JOIN {catalog_name}.raw.lojas l ON iv.id_loja = l.id_loja
#   LEFT JOIN {catalog_name}.raw.clientes c ON iv.id_cliente = c.id_cliente

# ▼▼▼ Seu código aqui ▼▼▼

# ▲▲▲ Fim do TODO 1 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Adicionar comentários às tabelas

# COMMAND ----------

# TODO 2: Adicione comentários descritivos às tabelas Gold
# ─────────────────────────────────────────────────────────
# Dica: Use COMMENT ON TABLE para adicionar descrições.
#       Isso ajuda o Genie a entender melhor os dados!
#
# Exemplo:
#   spark.sql(f"COMMENT ON TABLE {catalog_name}.gold.gold_vendas_por_loja IS 'Descrição aqui'")
#
# Adicione comentários para:
#   - gold_vendas_por_loja: métricas de vendas por loja
#   - gold_vendas_por_categoria: métricas por categoria de produto
#   - gold_vendas_por_cidade: métricas por cidade do RS
#   - gold_top_produtos: ranking de produtos mais vendidos

# ▼▼▼ Seu código aqui ▼▼▼

# ▲▲▲ Fim do TODO 2 ▲▲▲

print("Comentários adicionados!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criar Genie (no UI)
# MAGIC
# MAGIC ### TODO 3: Crie o Genie no Databricks
# MAGIC
# MAGIC 1. Vá em **AI/BI** > **Genie** > **New Genie**
# MAGIC 2. Nome: `Análise Panvel - <seu_nome>`
# MAGIC 3. **Adicione as seguintes tabelas:**

# COMMAND ----------

# Listar tabelas disponíveis para o Genie
print(f"Tabelas para adicionar ao Genie:")
print(f"{'='*60}")

tabelas_genie = [
    f"{catalog_name}.gold.gold_vendas_por_loja",
    f"{catalog_name}.gold.gold_vendas_por_categoria",
    f"{catalog_name}.gold.gold_vendas_por_cidade",
    f"{catalog_name}.gold.gold_top_produtos",
    f"{catalog_name}.gold.vw_vendas_resumo",
    f"{catalog_name}.gold.vw_vendas_produtos",
]

for t in tabelas_genie:
    try:
        count = spark.table(t).count()
        print(f"  ✓ {t} ({count} registros)")
    except:
        print(f"  ✗ {t} (não encontrada)")

# COMMAND ----------

# TODO 4: Escreva instruções customizadas para o Genie
# ──────────────────────────────────────────────────────────
# Dica: As instruções ajudam o Genie a entender o contexto dos dados.
# Inclua:
#   - Contexto: "Dados de vendas das Grupo Panvel no RS"
#   - Regras: "Valores em R$", "Nome da loja = Panvel - Bairro"
#   - Exemplos de perguntas que os usuários podem fazer
#
# Cole as instruções no campo "Instructions" do Genie

# ▼▼▼ Escreva suas instruções aqui ▼▼▼
instrucoes = """
## Contexto
(Descreva o que são os dados)

## Regras
(Adicione regras para o Genie)

## Exemplos de perguntas
(Liste exemplos de perguntas)
"""
# ▲▲▲ Fim do TODO 4 ▲▲▲

print(instrucoes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criar AI/BI Dashboard (no UI)
# MAGIC
# MAGIC ### TODO 5: Crie o Dashboard
# MAGIC
# MAGIC 1. Vá em **AI/BI** > **Dashboards** > **Create Dashboard**
# MAGIC 2. Nome: `Dashboard Panvel - <seu_nome>`
# MAGIC 3. Adicione os datasets abaixo como queries SQL

# COMMAND ----------

# Queries prontas para o Dashboard - copie e cole!
print("=" * 60)
print("QUERIES PARA O DASHBOARD - COPIE E COLE")
print("=" * 60)

# Query 1: KPIs
print(f"""
--- DATASET 1: KPI Resumo ---
SELECT
    COUNT(DISTINCT id_venda) as total_vendas,
    ROUND(SUM(valor_total), 2) as faturamento_total,
    ROUND(AVG(valor_total), 2) as ticket_medio,
    COUNT(DISTINCT id_cliente) as clientes_unicos
FROM {catalog_name}.gold.vw_vendas_resumo
""")

# Query 2: Faturamento por Cidade
print(f"""
--- DATASET 2: Faturamento por Cidade ---
SELECT cidade, faturamento_total, ticket_medio, num_lojas
FROM {catalog_name}.gold.gold_vendas_por_cidade
ORDER BY faturamento_total DESC
LIMIT 10
""")

# Query 3: Vendas por Categoria
print(f"""
--- DATASET 3: Vendas por Categoria ---
SELECT categoria, faturamento_total, total_itens_vendidos
FROM {catalog_name}.gold.gold_vendas_por_categoria
ORDER BY faturamento_total DESC
""")

# Query 4: Top Produtos
print(f"""
--- DATASET 4: Top 15 Produtos ---
SELECT nome_produto, categoria, faturamento_total, quantidade_total
FROM {catalog_name}.gold.gold_top_produtos
ORDER BY faturamento_total DESC
LIMIT 15
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Testar o Genie
# MAGIC
# MAGIC ### TODO 6: Teste o Genie com estas perguntas
# MAGIC
# MAGIC Abra seu Genie e faça as seguintes perguntas:
# MAGIC
# MAGIC 1. "Qual é o faturamento total?"
# MAGIC 2. "Quais são os 5 produtos mais vendidos?"
# MAGIC 3. "Qual cidade tem o maior ticket médio?"
# MAGIC 4. "Qual categoria gera mais receita?"
# MAGIC 5. "Compare Porto Alegre com Caxias do Sul"
# MAGIC
# MAGIC **Bônus:** Invente suas próprias perguntas!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parabéns! Workshop Concluído!
# MAGIC
# MAGIC Você completou todos os 4 labs do Workshop Panvel:
# MAGIC
# MAGIC | Lab | Tema | Status |
# MAGIC |-----|------|--------|
# MAGIC | 1 | SDP - Pipeline DLT | ✓ |
# MAGIC | 2 | Jobs - Workflow | ✓ |
# MAGIC | 3 | ML - Segmentação | ✓ |
# MAGIC | 4 | AI/BI - Genie + Dashboard | ✓ |
