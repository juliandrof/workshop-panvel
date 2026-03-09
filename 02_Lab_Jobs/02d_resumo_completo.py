# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab2.png" width="100%"/>
# MAGIC
# MAGIC ## Tarefa 4: Resumo do Processamento (Completo)
# MAGIC
# MAGIC Este notebook gera um resumo do processamento com métricas e top produtos.
# MAGIC
# MAGIC **Arquitetura do Workflow:**
# MAGIC ```
# MAGIC ┌────────────────┐     ┌──────────────────┐     ┌───────────────────┐     ┌───────────────────┐
# MAGIC │  Tarefa 1      │────▶│  Tarefa 2        │────▶│  Tarefa 3         │────▶│  Tarefa 4         │
# MAGIC │  Validar Dados │     │  Pipeline SDP    │     │  Qualidade Dados  │     │  Resumo           │
# MAGIC │                │     │                  │     │                   │     │  ★ ESTE ★         │
# MAGIC └────────────────┘     └──────────────────┘     └───────────────────┘     └───────────────────┘
# MAGIC ```

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
# MAGIC ### Resumo do Processamento

# COMMAND ----------

def gerar_resumo():
    """Gera um resumo do processamento para notificação."""
    print(f"\n{'='*60}")
    print(f"RESUMO DO PROCESSAMENTO - WORKSHOP PANVEL")
    print(f"{'='*60}")
    print(f"Catálogo: {catalog_name}")

    try:
        # Dados das tabelas gold
        vendas_loja = spark.table(f"{catalog_name}.gold.gold_vendas_por_loja")
        vendas_cat = spark.table(f"{catalog_name}.gold.gold_vendas_por_categoria")
        vendas_cidade = spark.table(f"{catalog_name}.gold.gold_vendas_por_cidade")
        top_produtos = spark.table(f"{catalog_name}.gold.gold_top_produtos")

        # TO-DO 3: Calcule o total de vendas e o faturamento total
        # ────────────────────────────────────────────────────────
        # Dica: Use .agg() para calcular as métricas agregadas
        #   total_vendas = vendas_loja.agg({"total_vendas": "sum"}).collect()[0][0]
        #   total_faturamento = vendas_loja.agg({"faturamento_total": "sum"}).collect()[0][0]
        #   Depois imprima os resultados com print()
        total_vendas = vendas_loja.agg({"total_vendas": "sum"}).collect()[0][0]
        total_faturamento = vendas_loja.agg({"faturamento_total": "sum"}).collect()[0][0]
        num_lojas = vendas_loja.count()
        num_cidades = vendas_cidade.count()
        num_categorias = vendas_cat.count()

        print(f"\nMétricas:")
        print(f"  Total de vendas processadas: {total_vendas:,.0f}")
        print(f"  Faturamento total: R$ {total_faturamento:,.2f}")
        print(f"  Lojas ativas: {num_lojas}")
        print(f"  Cidades atendidas: {num_cidades}")
        print(f"  Categorias de produtos: {num_categorias}")

        print(f"\nTop 5 Produtos:")
        top5 = top_produtos.limit(5).collect()
        for i, row in enumerate(top5, 1):
            print(f"  {i}. {row.nome_produto} ({row.categoria}) - R$ {row.faturamento_total:,.2f}")

    except Exception as e:
        print(f"\nErro ao gerar resumo: {e}")
        print("Execute o pipeline SDP primeiro!")

    print(f"\n{'='*60}")

gerar_resumo()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como criar o Workflow no Databricks
# MAGIC
# MAGIC 1. Vá em **Jobs & Pipelines** > **Create Job**
# MAGIC 2. Nome do Job: `workflow_panvel_<seu_nome>`
# MAGIC
# MAGIC **Configure 4 tarefas:**
# MAGIC
# MAGIC | Tarefa | Tipo | Notebook | Dependência |
# MAGIC |--------|------|----------|-------------|
# MAGIC | validacao | Notebook | `02_Lab_Jobs/02a_validacao_completo` | Nenhuma |
# MAGIC | trigger_pipeline | Notebook | `02_Lab_Jobs/02b_trigger_pipeline_completo` | validacao |
# MAGIC | qualidade | Notebook | `02_Lab_Jobs/02c_qualidade_completo` | trigger_pipeline |
# MAGIC | resumo | Notebook | `02_Lab_Jobs/02d_resumo_completo` | qualidade |
# MAGIC
# MAGIC **Parâmetros:** Em cada tarefa, adicione `nome_participante` = `<seu_nome>`
# MAGIC
# MAGIC **Agendamento:** A cada 30 minutos (para demo) ou Manual
