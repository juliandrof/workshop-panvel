# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab1.png" width="100%"/>
# MAGIC
# MAGIC **Versão Exercício** — Complete os TO-DOs para construir seu pipeline Spark Declarative Pipelines (SDP)!
# MAGIC
# MAGIC Pipeline com arquitetura Medallion:
# MAGIC - **Bronze**: Ingestão de JSONs de vendas via Auto Loader
# MAGIC - **Silver**: Limpeza, transformação e enriquecimento dos dados
# MAGIC - **Gold**: Tabelas agregadas para análise

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração
# MAGIC
# MAGIC > **Como criar o pipeline SDP:**
# MAGIC > 1. Vá em **Jobs & Pipelines** > **ETL pipeline**
# MAGIC > 2. **Pipeline name**: `pipeline_panvel_<seu_nome>`
# MAGIC > 3. **Source code**: selecione este notebook
# MAGIC > 4. **Target catalog**: `workshop_panvel_<seu_nome>`
# MAGIC > 5. **Target schema**: `default`
# MAGIC > 6. **Configuration** → adicione: `pipeline.nome_participante` = `<seu_nome>`
# MAGIC > 7. **Compute**: Serverless (recomendado)
# MAGIC > 8. Clique em **Create** e depois **Start**

# COMMAND ----------

nome_participante = spark.conf.get("pipeline.nome_participante", "default")
catalog_name = f"workshop_panvel_{nome_participante}"
volume_path = f"/Volumes/{catalog_name}/raw/vendas_json"

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE - Ingestão dos JSONs de Vendas
# MAGIC
# MAGIC Nesta etapa, vamos usar o **Auto Loader** para ingerir os arquivos JSON de vendas.

# COMMAND ----------

schema_vendas = StructType([
    StructField("id_venda", LongType(), False),
    StructField("id_cliente", IntegerType(), False),
    StructField("id_loja", IntegerType(), False),
    StructField("data_venda", StringType(), False),
    StructField("valor_total", DoubleType(), False),
    StructField("itens", ArrayType(StructType([
        StructField("id_venda", LongType(), False),
        StructField("id_produto", IntegerType(), False),
        StructField("quantidade", IntegerType(), False),
        StructField("valor_unitario", DoubleType(), False),
        StructField("valor_total", DoubleType(), False),
        StructField("desconto", DoubleType(), False),
    ])), False),
])

@dlt.table(
    name="bronze.bronze_vendas",
    comment="Dados brutos de vendas ingeridos via Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_vendas():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{volume_path}/_schema")
        .schema(schema_vendas)
        .load(volume_path)
        .withColumn("arquivo_origem", col("_metadata.file_path"))
        .withColumn("data_ingestao", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE - Tabelas de Cadastro (Referência)

# COMMAND ----------

@dlt.table(
    name="bronze.bronze_clientes",
    comment="Cadastro de clientes - tabela de referência",
    table_properties={"quality": "bronze"},
)
def bronze_clientes():
    return spark.table(f"{catalog_name}.raw.clientes")

@dlt.table(
    name="bronze.bronze_lojas",
    comment="Cadastro de lojas - tabela de referência",
    table_properties={"quality": "bronze"},
)
def bronze_lojas():
    return spark.table(f"{catalog_name}.raw.lojas")

# TO-DO 1: Crie a tabela bronze_produtos
# ─────────────────────────────────────
# Dica: Siga o mesmo padrão das tabelas bronze_clientes e bronze_lojas acima.
#       A tabela de origem é: {catalog_name}.raw.produtos
#       Use o decorator @dlt.table com name="bronze.bronze_produtos"
#       O prefixo "bronze." indica o schema de destino no catálogo

# ▼▼▼ Seu código aqui ▼▼▼



# ▲▲▲ Fim do TO-DO 1 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER - Vendas com Limpeza e Transformação
# MAGIC
# MAGIC Aqui vamos limpar e enriquecer os dados de vendas, fazendo joins com as tabelas de referência.

# COMMAND ----------

@dlt.table(
    name="silver.silver_vendas",
    comment="Vendas limpas e enriquecidas",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("id_venda_valido", "id_venda IS NOT NULL AND id_venda > 0")
@dlt.expect_or_drop("valor_positivo", "valor_total > 0")
def silver_vendas():
    vendas = dlt.read_stream("bronze.bronze_vendas")
    clientes = dlt.read("bronze.bronze_clientes")
    lojas = dlt.read("bronze.bronze_lojas")

    return (
        vendas
        .withColumn("data_venda", to_timestamp("data_venda"))

        # TO-DO 2: Extraia o ANO, MÊS e DIA da coluna data_venda
        # ─────────────────────────────────────────────────────
        # Dica: Use as funções year(), month() e dayofmonth() do PySpark
        #       Exemplo: .withColumn("ano", year("data_venda"))
        # ▼▼▼ Seu código aqui ▼▼▼

        # ▲▲▲ Fim do TO-DO 2 ▲▲▲

        .join(clientes, "id_cliente", "left")
        .withColumnRenamed("nome", "nome_cliente")
        .withColumnRenamed("cidade", "cidade_cliente")
        .join(lojas, "id_loja", "left")
        .withColumnRenamed("cidade", "cidade_loja")
        .select(
            "id_venda", "data_venda", "ano", "mes", "dia",
            "id_cliente", "nome_cliente", "cidade_cliente",
            "id_loja", "nome_loja", "cidade_loja",
            "valor_total", "itens", "data_ingestao"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER - Lojas com Extração do Bairro
# MAGIC
# MAGIC O nome da loja segue o padrão **"Panvel - Bairro"** (ex: "Panvel - Centro", "Panvel - Moinhos de Vento").
# MAGIC Precisamos extrair o nome do bairro para uma nova coluna.

# COMMAND ----------

@dlt.table(
    name="silver.silver_lojas",
    comment="Lojas com coluna bairro extraída do nome da loja",
    table_properties={"quality": "silver"},
)
def silver_lojas():
    lojas = dlt.read("bronze.bronze_lojas")

    return (
        lojas

        # TO-DO 3: Extraia o bairro do nome da loja
        # ────────────────────────────────────────
        # O nome da loja tem o formato: "Panvel - Centro" ou "Panvel - Moinhos de Vento 2"
        # Você precisa:
        #   1. Remover "Panvel - " do início
        #   2. Remover números do final (lojas com número sequencial)
        #
        # Dica: Use a função regexp_replace() do PySpark
        #   - Para remover "Panvel - ": regexp_replace(col("nome_loja"), "^Panvel - ", "")
        #   - Para remover números do final: regexp_replace(..., "\\s+\\d+$", "")
        #   - Combine os dois em uma única expressão aninhada
        #   - Crie a coluna com .withColumn("bairro", ...)
        # ▼▼▼ Seu código aqui ▼▼▼

        # ▲▲▲ Fim do TO-DO 3 ▲▲▲
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER - Itens de Venda (Explode)
# MAGIC
# MAGIC Os itens da venda estão aninhados como array no JSON. Precisamos "explodir" para ter um registro por item.

# COMMAND ----------

@dlt.table(
    name="silver.silver_itens_venda",
    comment="Itens de venda explodidos com dados de produto",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("quantidade_valida", "quantidade > 0")
def silver_itens_venda():
    vendas = dlt.read_stream("bronze.bronze_vendas")
    produtos = dlt.read("bronze.bronze_produtos")

    return (
        vendas
        .select("id_venda", "id_loja", "id_cliente",
                to_timestamp("data_venda").alias("data_venda"),

                # TO-DO 4: Use a função explode() para "explodir" a coluna "itens"
                # ────────────────────────────────────────────────────────────────
                # Dica: explode("itens").alias("item")
                #       Isso transforma cada elemento do array em uma linha separada
                # ▼▼▼ Seu código aqui ▼▼▼

                # ▲▲▲ Fim do TO-DO 4 ▲▲▲
        )
        .select(
            "id_venda", "id_loja", "id_cliente", "data_venda",
            col("item.id_produto").alias("id_produto"),
            col("item.quantidade").alias("quantidade"),
            col("item.valor_unitario").alias("valor_unitario"),
            col("item.valor_total").alias("valor_total"),
            col("item.desconto").alias("desconto"),
        )
        .join(produtos, "id_produto", "left")
        .withColumnRenamed("nome", "nome_produto")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD - Vendas por Loja

# COMMAND ----------

@dlt.table(
    name="gold.gold_vendas_por_loja",
    comment="Agregação de vendas por loja",
    table_properties={"quality": "gold"},
)
def gold_vendas_por_loja():
    vendas = dlt.read("silver.silver_vendas")

    return (
        vendas
        .groupBy("id_loja", "nome_loja", "cidade_loja")
        .agg(
            count("id_venda").alias("total_vendas"),
            sum("valor_total").alias("faturamento_total"),
            avg("valor_total").alias("ticket_medio"),
            countDistinct("id_cliente").alias("clientes_unicos")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD - Vendas por Categoria

# COMMAND ----------

@dlt.table(
    name="gold.gold_vendas_por_categoria",
    comment="Agregação de vendas por categoria de produto",
    table_properties={"quality": "gold"},
)
def gold_vendas_por_categoria():
    itens = dlt.read("silver.silver_itens_venda")

    # TO-DO 5: Complete as agregações por categoria
    # ─────────────────────────────────────────────
    # Dica: Use groupBy("categoria") e depois .agg() com:
    #   - count("*").alias("total_itens_vendidos")
    #   - sum("quantidade").alias("quantidade_total")
    #   - sum("valor_total").alias("faturamento_total")
    #   - sum("desconto").alias("desconto_total")
    #   - avg("valor_unitario").alias("preco_medio")
    # ▼▼▼ Seu código aqui ▼▼▼
    return (
        itens
        .groupBy("categoria")
        .agg(
            # Complete as agregações aqui
        )
    )
    # ▲▲▲ Fim do TO-DO 5 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD - Vendas por Cidade

# COMMAND ----------

# TO-DO 6: Crie a tabela gold_vendas_por_cidade
# ──────────────────────────────────────────────
# Dica: Siga o padrão da tabela gold_vendas_por_loja
#       mas agrupando por "cidade_loja" em vez de "id_loja"
#
#       Agregações sugeridas:
#       - count("id_venda").alias("total_vendas")
#       - sum("valor_total").alias("faturamento_total")
#       - avg("valor_total").alias("ticket_medio")
#       - countDistinct("id_loja").alias("num_lojas")
#       - countDistinct("id_cliente").alias("clientes_unicos")
#
#       Renomeie a coluna "cidade_loja" para "cidade" no final

# ▼▼▼ Seu código aqui ▼▼▼



# ▲▲▲ Fim do TO-DO 6 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD - Top Produtos

# COMMAND ----------

@dlt.table(
    name="gold.gold_top_produtos",
    comment="Ranking dos produtos mais vendidos",
    table_properties={"quality": "gold"},
)
def gold_top_produtos():
    itens = dlt.read("silver.silver_itens_venda")

    return (
        itens
        .groupBy("id_produto", "nome_produto", "categoria")
        .agg(
            sum("quantidade").alias("quantidade_total"),
            sum("valor_total").alias("faturamento_total"),
            count("id_venda").alias("num_vendas"),
            avg("desconto").alias("desconto_medio")
        )
        .orderBy(desc("faturamento_total"))
    )
