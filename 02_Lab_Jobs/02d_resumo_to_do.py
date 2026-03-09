# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab2.png" width="100%"/>
# MAGIC
# MAGIC ## Tarefa 4: Resumo do Processamento (Exercício)
# MAGIC
# MAGIC Complete os TO-DOs para gerar o resumo do processamento!
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
    """Gera resumo do processamento."""
    print(f"\n{'='*60}")
    print(f"RESUMO - WORKSHOP PANVEL")
    print(f"{'='*60}")

    try:
        vendas_loja = spark.table(f"{catalog_name}.gold.gold_vendas_por_loja")

        # TO-DO 4: Calcule o total de vendas e o faturamento total
        # ────────────────────────────────────────────────────────
        # Dica: Use .agg() para calcular as métricas agregadas
        #   total_vendas = vendas_loja.agg({"total_vendas": "sum"}).collect()[0][0]
        #   total_faturamento = vendas_loja.agg({"faturamento_total": "sum"}).collect()[0][0]
        #   Depois imprima os resultados com print()
        # ▼▼▼ Seu código aqui ▼▼▼

        # ▲▲▲ Fim do TO-DO 4 ▲▲▲

        # Top 5 Produtos
        print(f"\nTop 5 Produtos por Faturamento:")
        top5 = spark.table(f"{catalog_name}.gold.gold_top_produtos").limit(5).collect()
        for i, row in enumerate(top5, 1):
            print(f"  {i}. {row.nome_produto} - R$ {row.faturamento_total:,.2f}")

    except Exception as e:
        print(f"Erro: {e}")
        print("Execute o pipeline SDP primeiro!")

    print(f"{'='*60}")

gerar_resumo()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como criar o Workflow no Databricks
# MAGIC
# MAGIC #### TO-DO 5: Crie o Workflow no Databricks UI
# MAGIC
# MAGIC 1. Vá em **Jobs & Pipelines** > **Create Job**
# MAGIC 2. Nome: `workflow_panvel_<seu_nome>`
# MAGIC
# MAGIC **Configure 4 tarefas:**
# MAGIC
# MAGIC | Tarefa | Tipo | Notebook | Dependência |
# MAGIC |--------|------|----------|-------------|
# MAGIC | validacao | Notebook | `02_Lab_Jobs/02a_validacao_to_do` | Nenhuma |
# MAGIC | trigger_pipeline | Notebook | `02_Lab_Jobs/02b_trigger_pipeline_to_do` | validacao |
# MAGIC | qualidade | Notebook | `02_Lab_Jobs/02c_qualidade_to_do` | trigger_pipeline |
# MAGIC | resumo | Notebook | `02_Lab_Jobs/02d_resumo_to_do` | qualidade |
# MAGIC
# MAGIC **Parâmetros:** Em cada tarefa, adicione `nome_participante` = `<seu_nome>`
# MAGIC
# MAGIC #### TO-DO 6: Configure o agendamento
# MAGIC - Clique em **Schedule** no canto superior direito
# MAGIC - Configure para rodar a cada **30 minutos**
# MAGIC - Timezone: **America/Sao_Paulo**
