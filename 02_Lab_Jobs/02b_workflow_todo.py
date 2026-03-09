# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab2.png" width="100%"/>
# MAGIC
# MAGIC **Versão Exercício** — Complete os TO-DOs para construir seu Workflow Databricks!
# MAGIC
# MAGIC ## Arquitetura do Workflow
# MAGIC ```
# MAGIC ┌────────────────┐     ┌──────────────────┐     ┌───────────────────┐
# MAGIC │  Tarefa 1      │────▶│  Tarefa 2        │────▶│  Tarefa 3         │
# MAGIC │  Validar Dados │     │  Pipeline SDP    │     │  Qualidade Dados  │
# MAGIC │  Cadastrais    │     │                  │     │  (Verificação)    │
# MAGIC └────────────────┘     └──────────────────┘     └───────────────────┘
# MAGIC                                                          │
# MAGIC                                                          ▼
# MAGIC                                                 ┌───────────────────┐
# MAGIC                                                 │  Tarefa 4         │
# MAGIC                                                 │  Notificação      │
# MAGIC                                                 │  (Resumo)         │
# MAGIC                                                 └───────────────────┘
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
# MAGIC ## Tarefa 1: Validação dos Dados Cadastrais

# COMMAND ----------

def validar_dados_cadastrais():
    """Verifica integridade dos dados cadastrais."""

    tabelas = {
        "clientes": {"min_registros": 100, "colunas_obrigatorias": ["id_cliente", "nome", "cidade"]},
        "lojas": {"min_registros": 50, "colunas_obrigatorias": ["id_loja", "nome_loja", "cidade"]},
        "produtos": {"min_registros": 300, "colunas_obrigatorias": ["id_produto", "nome", "categoria"]},
    }

    for tabela, regras in tabelas.items():
        df = spark.table(f"{catalog_name}.raw.{tabela}")
        count = df.count()
        colunas = df.columns

        # TODO 1: Verifique se a tabela possui a quantidade mínima de registros
        # ─────────────────────────────────────────────────────────────────────
        # Dica: Compare a variável 'count' com regras["min_registros"]
        #       Use assert para lançar erro se não atender o mínimo
        #       Exemplo: assert count >= regras["min_registros"], "mensagem de erro"
        # ▼▼▼ Seu código aqui ▼▼▼

        # ▲▲▲ Fim do TODO 1 ▲▲▲

        # Verificar colunas obrigatórias
        for col_name in regras["colunas_obrigatorias"]:
            assert col_name in colunas, \
                f"Tabela {tabela}: coluna {col_name} não encontrada"

        print(f"  ✓ {tabela}: {count} registros - OK")

    return True

print("Validando dados cadastrais...")
validar_dados_cadastrais()
print("\nValidação concluída!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tarefa 2: Referência para configuração do Pipeline SDP
# MAGIC
# MAGIC No Workflow real, esta tarefa será do tipo **Pipeline**.

# COMMAND ----------

print(f"""
Para configurar no Workflow:

1. Vá em Workflows > Create Job
2. Nome do Job: workflow_panvel_{nome}
3. Adicione as seguintes tarefas:

   Tarefa 1 - Validação:
     Tipo: Notebook
     Notebook: 02_Lab_Jobs/02b_workflow_todo
     Parâmetros: nome_participante = {nome}

   Tarefa 2 - Pipeline:
     Tipo: Pipeline
     Pipeline: pipeline_panvel_{nome}
     Depende de: Tarefa 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tarefa 3: Verificação de Qualidade dos Dados

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

    # TODO 2: Verifique se a tabela silver_lojas possui a coluna "bairro"
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
    # ▲▲▲ Fim do TODO 2 ▲▲▲

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tarefa 4: Resumo do Processamento

# COMMAND ----------

def gerar_resumo():
    """Gera resumo do processamento."""
    print(f"\n{'='*60}")
    print(f"RESUMO - WORKSHOP PANVEL")
    print(f"{'='*60}")

    try:
        vendas_loja = spark.table(f"{catalog_name}.gold.gold_vendas_por_loja")

        # TODO 3: Calcule o total de vendas e o faturamento total
        # ────────────────────────────────────────────────────────
        # Dica: Use .agg() para calcular as métricas agregadas
        #   total_vendas = vendas_loja.agg({"total_vendas": "sum"}).collect()[0][0]
        #   total_faturamento = vendas_loja.agg({"faturamento_total": "sum"}).collect()[0][0]
        #   Depois imprima os resultados com print()
        # ▼▼▼ Seu código aqui ▼▼▼

        # ▲▲▲ Fim do TODO 3 ▲▲▲

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
# MAGIC ## Instruções para criar o Workflow
# MAGIC
# MAGIC ### TODO 4: Crie o Workflow no Databricks UI
# MAGIC
# MAGIC 1. Vá em **Workflows** > **Create Job**
# MAGIC 2. Nome: `workflow_panvel_<seu_nome>`
# MAGIC
# MAGIC **Configure 4 tarefas:**
# MAGIC
# MAGIC | Tarefa | Tipo | Dependência | Descrição |
# MAGIC |--------|------|-------------|-----------|
# MAGIC | validacao | Notebook | Nenhuma | Valida dados cadastrais |
# MAGIC | pipeline_sdp | Pipeline | validacao | Executa o pipeline SDP |
# MAGIC | qualidade | Notebook | pipeline_sdp | Verifica qualidade |
# MAGIC | resumo | Notebook | qualidade | Gera relatório |
# MAGIC
# MAGIC ### TODO 5: Configure o agendamento
# MAGIC - Clique em **Schedule** no canto superior direito
# MAGIC - Configure para rodar a cada **30 minutos**
# MAGIC - Timezone: **America/Sao_Paulo**
