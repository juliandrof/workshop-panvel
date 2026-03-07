# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab2.png" width="100%"/>
# MAGIC
# MAGIC **Versão Completa** — Este notebook contém o código para criar e configurar um Workflow Databricks
# MAGIC que orquestra o pipeline de dados da Panvel.
# MAGIC
# MAGIC ## Arquitetura do Workflow
# MAGIC ```
# MAGIC ┌────────────────┐     ┌──────────────────┐     ┌───────────────────┐
# MAGIC │  Tarefa 1      │────▶│  Tarefa 2        │────▶│  Tarefa 3         │
# MAGIC │  Validar Dados │     │  Pipeline DLT    │     │  Qualidade Dados  │
# MAGIC │  Cadastrais    │     │  (SDP)           │     │  (Verificação)    │
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
# MAGIC
# MAGIC Esta tarefa verifica se as tabelas de cadastro existem e possuem dados válidos.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tarefa 2: Trigger do Pipeline DLT
# MAGIC
# MAGIC Em um workflow real, esta tarefa seria configurada como uma tarefa do tipo **DLT Pipeline**.
# MAGIC Aqui mostramos como seria feito via API.

# COMMAND ----------

# Em um workflow real, você configuraria uma tarefa do tipo "Delta Live Tables pipeline"
# apontando para o pipeline criado no Lab 1.
#
# Via código, você pode triggar o pipeline usando a API REST:

print("""
Para configurar no Workflow:
1. Adicione uma nova tarefa do tipo "Delta Live Tables pipeline"
2. Selecione o pipeline: pipeline_panvel_{nome}
3. Configure a dependência: depende da Tarefa 1 (Validação)
""".format(nome=nome))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tarefa 3: Verificação de Qualidade dos Dados

# COMMAND ----------

def verificar_qualidade_dados():
    """Verifica a qualidade dos dados processados pelo pipeline."""
    print("Verificando qualidade dos dados processados...")

    verificacoes = []

    # 1. Verificar se silver_vendas tem dados
    try:
        df_vendas = spark.table(f"{catalog_name}.silver.silver_vendas")
        count_vendas = df_vendas.count()
        verificacoes.append(("silver_vendas - registros", count_vendas > 0, count_vendas))
    except Exception as e:
        verificacoes.append(("silver_vendas - registros", False, str(e)))

    # 2. Verificar se silver_lojas tem bairro
    try:
        df_lojas = spark.table(f"{catalog_name}.silver.silver_lojas")
        tem_bairro = "bairro" in df_lojas.columns
        bairros_nulos = df_lojas.filter("bairro IS NULL").count() if tem_bairro else -1
        verificacoes.append(("silver_lojas - coluna bairro", tem_bairro and bairros_nulos == 0, f"bairros_nulos={bairros_nulos}"))
    except Exception as e:
        verificacoes.append(("silver_lojas - coluna bairro", False, str(e)))

    # 3. Verificar gold tables
    for gold_table in ["gold_vendas_por_loja", "gold_vendas_por_categoria", "gold_vendas_por_cidade", "gold_top_produtos"]:
        try:
            df = spark.table(f"{catalog_name}.gold.{gold_table}")
            count = df.count()
            verificacoes.append((gold_table, count > 0, count))
        except Exception as e:
            verificacoes.append((gold_table, False, str(e)))

    # Relatório
    print(f"\n{'='*60}")
    print("RELATÓRIO DE QUALIDADE DOS DADOS")
    print(f"{'='*60}")
    todas_ok = True
    for nome_check, status, detalhe in verificacoes:
        emoji = "✓" if status else "✗"
        print(f"  {emoji} {nome_check}: {detalhe}")
        if not status:
            todas_ok = False

    if todas_ok:
        print(f"\n{'='*60}")
        print("TODAS AS VERIFICAÇÕES PASSARAM!")
        print(f"{'='*60}")
    else:
        print(f"\n{'='*60}")
        print("ATENÇÃO: Algumas verificações falharam!")
        print(f"{'='*60}")

    return verificacoes

resultado_qualidade = verificar_qualidade_dados()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tarefa 4: Notificação / Resumo

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
        print("Execute o pipeline DLT primeiro!")

    print(f"\n{'='*60}")

gerar_resumo()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Como criar o Workflow no Databricks
# MAGIC
# MAGIC ### Passo a passo:
# MAGIC
# MAGIC 1. Vá em **Workflows** > **Create Job**
# MAGIC 2. Nome do Job: `workflow_panvel_<seu_nome>`
# MAGIC
# MAGIC ### Tarefa 1 - Validação:
# MAGIC - Tipo: **Notebook**
# MAGIC - Notebook: `02_Lab_Jobs/02a_workflow_completo` (ou 02b_workflow_todo)
# MAGIC - Parâmetros: `nome_participante` = `<seu_nome>`
# MAGIC - Cluster: Serverless ou seu cluster
# MAGIC
# MAGIC ### Tarefa 2 - Pipeline DLT:
# MAGIC - Tipo: **Delta Live Tables pipeline**
# MAGIC - Pipeline: `pipeline_panvel_<seu_nome>`
# MAGIC - Depende de: Tarefa 1
# MAGIC
# MAGIC ### Tarefa 3 - Qualidade:
# MAGIC - Tipo: **Notebook**
# MAGIC - Notebook: Este mesmo notebook (seção de qualidade)
# MAGIC - Depende de: Tarefa 2
# MAGIC
# MAGIC ### Tarefa 4 - Resumo:
# MAGIC - Tipo: **Notebook**
# MAGIC - Notebook: Este mesmo notebook (seção de resumo)
# MAGIC - Depende de: Tarefa 3
# MAGIC
# MAGIC ### Agendamento:
# MAGIC - Schedule: A cada 30 minutos (para demo) ou Manual
