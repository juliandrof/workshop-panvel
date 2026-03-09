# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab2.png" width="100%"/>
# MAGIC
# MAGIC ## Tarefa 3: Verificação de Qualidade dos Dados (Exercício)
# MAGIC
# MAGIC Complete os TO-DOs para verificar a qualidade dos dados processados!
# MAGIC
# MAGIC **Arquitetura do Workflow:**
# MAGIC ```
# MAGIC ┌────────────────┐     ┌──────────────────┐     ┌───────────────────┐     ┌───────────────────┐
# MAGIC │  Tarefa 1      │────▶│  Tarefa 2        │────▶│  Tarefa 3         │────▶│  Tarefa 4         │
# MAGIC │  Validar Dados │     │  Pipeline SDP    │     │  Qualidade Dados  │     │  Resumo           │
# MAGIC │                │     │                  │     │  ★ ESTE ★         │     │                   │
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
# MAGIC ### Verificação de Qualidade dos Dados

# COMMAND ----------

def verificar_qualidade_dados():
    """Verifica a qualidade dos dados processados."""
    print("Verificando qualidade dos dados...")
    verificacoes = []

    # Verificar silver_vendas
    try:
        df_vendas = spark.table(f"{catalog_name}.silver.silver_vendas")
        count_vendas = df_vendas.count()
        verificacoes.append(("silver_vendas", count_vendas > 0, count_vendas))
    except Exception as e:
        verificacoes.append(("silver_vendas", False, str(e)))

    # TO-DO 3: Verifique se a tabela silver_lojas possui a coluna "bairro"
    # ────────────────────────────────────────────────────────────────────
    # Dica:
    #   1. Leia a tabela: df_lojas = spark.table(f"{catalog_name}.silver.silver_lojas")
    #   2. Verifique se "bairro" está em df_lojas.columns
    #   3. Conte quantos bairros nulos existem
    #   4. Adicione o resultado na lista verificacoes
    #      verificacoes.append(("silver_lojas - bairro", True/False, detalhe))
    # ▼▼▼ Seu código aqui ▼▼▼
    try:
        pass  # Substitua este 'pass' pelo seu código
    except Exception as e:
        verificacoes.append(("silver_lojas - bairro", False, str(e)))
    # ▲▲▲ Fim do TO-DO 3 ▲▲▲

    # Verificar gold tables
    for gold_table in ["gold_vendas_por_loja", "gold_vendas_por_categoria", "gold_vendas_por_cidade"]:
        try:
            df = spark.table(f"{catalog_name}.gold.{gold_table}")
            count = df.count()
            verificacoes.append((gold_table, count > 0, count))
        except Exception as e:
            verificacoes.append((gold_table, False, str(e)))

    # Relatório
    print(f"\n{'='*60}")
    for nome_check, status, detalhe in verificacoes:
        emoji = "✓" if status else "✗"
        print(f"  {emoji} {nome_check}: {detalhe}")
    print(f"{'='*60}")

    return verificacoes

verificar_qualidade_dados()
