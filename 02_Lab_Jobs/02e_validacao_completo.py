# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab2.png" width="100%"/>
# MAGIC
# MAGIC ## Tarefa 1: Validacao dos Dados Cadastrais (Completo)
# MAGIC
# MAGIC Este notebook verifica se as tabelas de cadastro existem e possuem dados validos.
# MAGIC
# MAGIC **Arquitetura do Workflow:**
# MAGIC ```
# MAGIC ┌────────────────┐     ┌──────────────────┐     ┌───────────────────┐     ┌───────────────────┐
# MAGIC │  Tarefa 1      │────▶│  Tarefa 2        │────▶│  Tarefa 3         │────▶│  Tarefa 4         │
# MAGIC │  Validar Dados │     │  Pipeline SDP    │     │  Qualidade Dados  │     │  Resumo           │
# MAGIC │  ★ ESTE ★      │     │                  │     │                   │     │                   │
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
# MAGIC ### Validação dos Dados Cadastrais

# COMMAND ----------

def validar_dados_cadastrais():
    """Verifica integridade dos dados cadastrais."""
    resultados = {}

    tabelas = {
        "clientes": {"min_registros": 100, "colunas_obrigatorias": ["id_cliente", "nome", "cidade"]},
        "lojas": {"min_registros": 50, "colunas_obrigatorias": ["id_loja", "nome_loja", "cidade"]},
        "produtos": {"min_registros": 300, "colunas_obrigatorias": ["id_produto", "nome", "categoria"]},
    }

    for tabela, regras in tabelas.items():
        df = spark.table(f"{catalog_name}.raw.{tabela}")
        count = df.count()
        colunas = df.columns

        # TO-DO 1: Verifique se a tabela possui a quantidade mínima de registros
        # ─────────────────────────────────────────────────────────────────────
        # Dica: Compare a variável 'count' com regras["min_registros"]
        #       Use assert para lançar erro se não atender o mínimo
        #       Exemplo: assert count >= regras["min_registros"], "mensagem de erro"
        # Verificar quantidade mínima
        assert count >= regras["min_registros"], \
            f"Tabela {tabela}: esperado mínimo {regras['min_registros']}, encontrado {count}"

        # Verificar colunas obrigatórias
        for col_name in regras["colunas_obrigatorias"]:
            assert col_name in colunas, \
                f"Tabela {tabela}: coluna {col_name} não encontrada"

        # Verificar nulos nas colunas obrigatórias
        for col_name in regras["colunas_obrigatorias"]:
            nulos = df.filter(f"{col_name} IS NULL").count()
            assert nulos == 0, \
                f"Tabela {tabela}: {nulos} nulos na coluna {col_name}"

        resultados[tabela] = {"registros": count, "status": "OK"}
        print(f"  ✓ {tabela}: {count} registros - OK")

    return resultados

print("Validando dados cadastrais...")
resultado_validacao = validar_dados_cadastrais()
print("\nTodos os dados cadastrais estão válidos!")
