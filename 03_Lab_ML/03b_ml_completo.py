# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab3.png" width="100%"/>
# MAGIC
# MAGIC ## Segmentação de Clientes com RFM + K-Means (Versão Completa)
# MAGIC
# MAGIC Neste lab, vamos:
# MAGIC 1. Criar features RFM (Recency, Frequency, Monetary) a partir dos dados de vendas
# MAGIC 2. Treinar um modelo de clustering K-Means
# MAGIC 3. Registrar o modelo no MLflow
# MAGIC 4. Analisar os segmentos de clientes

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
# MAGIC ## 1. Carregar dados de vendas

# COMMAND ----------

df_vendas = spark.table(f"{catalog_name}.silver.silver_vendas")
print(f"Total de vendas: {df_vendas.count()}")
df_vendas.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering - Análise RFM

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data de referência (mais recente no dataset)
data_ref = df_vendas.agg(max("data_venda")).collect()[0][0]
print(f"Data de referência: {data_ref}")

# Calcular RFM por cliente
df_rfm = (
    df_vendas
    .groupBy("id_cliente", "nome_cliente", "cidade_cliente")
    .agg(
        # TO-DO 1: Calcule as métricas RFM por cliente
        # ────────────────────────────────────────────
        # Dica: Calcule:
        #   - recency: datediff(lit(data_ref), max("data_venda"))
        #   - frequency: countDistinct("id_venda")
        #   - monetary: sum("valor_total")
        #   - ticket_medio: avg("valor_total")
        # Recency: dias desde a última compra
        datediff(lit(data_ref), max("data_venda")).alias("recency"),
        # Frequency: número de compras
        countDistinct("id_venda").alias("frequency"),
        # Monetary: valor total gasto
        sum("valor_total").alias("monetary"),
        # Métricas adicionais
        avg("valor_total").alias("ticket_medio"),
        count("id_venda").alias("num_transacoes"),
    )
)

print(f"Clientes com compras: {df_rfm.count()}")
df_rfm.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preparar dados para clustering

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StandardScaler

# Selecionar features para clustering
feature_cols = ["recency", "frequency", "monetary"]

# TO-DO 2: Monte o vetor de features usando VectorAssembler
# ────────────────────────────────────────────────────────
# Dica:
#   assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
#   df_features = assembler.transform(df_rfm)
# Criar vetor de features
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
df_features = assembler.transform(df_rfm)

# Normalizar features (StandardScaler)
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
scaler_model = scaler.fit(df_features)
df_scaled = scaler_model.transform(df_features)

print("Features preparadas e normalizadas!")
df_scaled.select("id_cliente", "nome_cliente", "recency", "frequency", "monetary", "features").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Treinar modelo K-Means com MLflow

# COMMAND ----------

import mlflow
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Configurar experimento MLflow
experiment_name = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/workshop_panvel_{nome}_rfm"
mlflow.set_experiment(experiment_name)

# Testar diferentes valores de K
resultados = []

# TO-DO 3: Treine modelos K-Means para K = 3, 4, 5, 6
# ────────────────────────────────────────────────────
# Dica: Para cada valor de K:
#   1. Abra um run MLflow: with mlflow.start_run(run_name=f"kmeans_k{k}"):
#   2. Crie o modelo: kmeans = KMeans(k=k, seed=42, featuresCol="features", predictionCol="segmento")
#   3. Treine: model = kmeans.fit(df_scaled)
#   4. Predições: predictions = model.transform(df_scaled)
#   5. Avalie com silhouette score
#   6. Log no MLflow: mlflow.log_param, mlflow.log_metric, mlflow.spark.log_model
for k in [3, 4, 5, 6]:
    with mlflow.start_run(run_name=f"kmeans_k{k}"):
        # Treinar modelo
        kmeans = KMeans(k=k, seed=42, featuresCol="features", predictionCol="segmento")
        model = kmeans.fit(df_scaled)

        # Predições
        predictions = model.transform(df_scaled)

        # Avaliar - Silhouette Score
        evaluator = ClusteringEvaluator(
            predictionCol="segmento",
            featuresCol="features",
            metricName="silhouette"
        )
        silhouette = evaluator.evaluate(predictions)

        # Log no MLflow
        mlflow.log_param("k", k)
        mlflow.log_param("features", feature_cols)
        mlflow.log_metric("silhouette_score", silhouette)
        mlflow.spark.log_model(model, "model")

        resultados.append({"k": k, "silhouette": silhouette})
        print(f"K={k}: Silhouette Score = {silhouette:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Selecionar melhor modelo e analisar segmentos

# COMMAND ----------

# Selecionar o K com melhor silhouette
melhor = max(resultados, key=lambda x: x["silhouette"])
melhor_k = melhor["k"]
print(f"Melhor K: {melhor_k} (Silhouette: {melhor['silhouette']:.4f})")

# Retreinar com o melhor K
with mlflow.start_run(run_name=f"kmeans_final_k{melhor_k}") as run:
    kmeans_final = KMeans(k=melhor_k, seed=42, featuresCol="features", predictionCol="segmento")
    model_final = kmeans_final.fit(df_scaled)
    predictions_final = model_final.transform(df_scaled)

    evaluator = ClusteringEvaluator(predictionCol="segmento", featuresCol="features", metricName="silhouette")
    silhouette_final = evaluator.evaluate(predictions_final)

    mlflow.log_param("k", melhor_k)
    mlflow.log_param("features", feature_cols)
    mlflow.log_param("modelo", "KMeans_final")
    mlflow.log_metric("silhouette_score", silhouette_final)
    mlflow.spark.log_model(model_final, "model")

    run_id = run.info.run_id
    print(f"Run ID: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Análise dos Segmentos

# COMMAND ----------

# TO-DO 4: Analise o perfil de cada segmento
# ──────────────────────────────────────────
# Dica: Agrupe predictions_final por "segmento" e calcule:
#   - count("*").alias("num_clientes")
#   - round(avg("recency"), 1).alias("recency_medio")
#   - round(avg("frequency"), 1).alias("frequency_media")
#   - round(avg("monetary"), 2).alias("monetary_medio")
#   - round(avg("ticket_medio"), 2).alias("ticket_medio")
#   Ordene por "segmento"
# Analisar perfil de cada segmento
df_segmentos = (
    predictions_final
    .groupBy("segmento")
    .agg(
        count("*").alias("num_clientes"),
        round(avg("recency"), 1).alias("recency_medio"),
        round(avg("frequency"), 1).alias("frequency_media"),
        round(avg("monetary"), 2).alias("monetary_medio"),
        round(avg("ticket_medio"), 2).alias("ticket_medio"),
    )
    .orderBy("segmento")
)

print("Perfil dos Segmentos:")
df_segmentos.show(truncate=False)

# COMMAND ----------

# Nomear segmentos baseado no perfil RFM
# (Os nomes são atribuídos com base nas características observadas)

from pyspark.sql.functions import when

df_com_nome = (
    predictions_final
    .select("id_cliente", "nome_cliente", "cidade_cliente",
            "recency", "frequency", "monetary", "ticket_medio", "segmento")
)

# Adicionar label descritivo baseado no segmento
segmento_labels = {
    0: "Cliente Premium",
    1: "Cliente Regular",
    2: "Cliente Eventual",
    3: "Cliente em Risco",
    4: "Cliente Novo",
    5: "Cliente Inativo"
}

for seg_id, label in segmento_labels.items():
    if seg_id < melhor_k:
        df_com_nome = df_com_nome.withColumn(
            "nome_segmento",
            when(col("segmento") == seg_id, lit(label)).otherwise(
                col("nome_segmento") if "nome_segmento" in df_com_nome.columns else lit(None)
            )
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Salvar resultado na camada Gold

# COMMAND ----------

# TO-DO 5: Salve a tabela de segmentação no catálogo
# ──────────────────────────────────────────────────
# Dica: Use .write.mode("overwrite").saveAsTable(f"{catalog_name}.ml.segmentacao_clientes")
# Salvar segmentação de clientes na camada ML
(
    df_com_nome
    .write
    .mode("overwrite")
    .saveAsTable(f"{catalog_name}.ml.segmentacao_clientes")
)

print(f"Segmentação salva em {catalog_name}.ml.segmentacao_clientes")
print(f"Total de clientes segmentados: {df_com_nome.count()}")

# Resumo
print(f"\n{'='*60}")
print("SEGMENTAÇÃO DE CLIENTES CONCLUÍDA!")
print(f"{'='*60}")
print(f"Modelo: K-Means com K={melhor_k}")
print(f"Silhouette Score: {silhouette_final:.4f}")
print(f"Experimento MLflow: {experiment_name}")
print(f"Run ID: {run_id}")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Registrar modelo no Unity Catalog

# COMMAND ----------

model_name = f"{catalog_name}.ml.modelo_segmentacao_clientes"

# Registrar modelo
model_uri = f"runs:/{run_id}/model"
mlflow.set_registry_uri("databricks-uc")
registered_model = mlflow.register_model(model_uri, model_name)

print(f"Modelo registrado: {model_name}")
print(f"Versão: {registered_model.version}")
