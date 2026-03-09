# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab3.png" width="100%"/>
# MAGIC
# MAGIC ## Segmentação de Clientes com RFM + K-Means (Versão Exercício)
# MAGIC
# MAGIC Complete os TO-DOs para criar seu modelo de segmentação!
# MAGIC
# MAGIC Etapas:
# MAGIC 1. Criar features RFM (Recency, Frequency, Monetary)
# MAGIC 2. Treinar modelo K-Means
# MAGIC 3. Registrar no MLflow
# MAGIC 4. Analisar segmentos

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
# MAGIC
# MAGIC **RFM** é uma técnica de segmentação baseada em:
# MAGIC - **R**ecency (Recência): Há quantos dias o cliente fez a última compra?
# MAGIC - **F**requency (Frequência): Quantas vezes o cliente comprou?
# MAGIC - **M**onetary (Valor): Quanto o cliente gastou no total?

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data de referência (mais recente no dataset)
data_ref = df_vendas.agg(max("data_venda")).collect()[0][0]
print(f"Data de referência: {data_ref}")

# TO-DO 1: Calcule as métricas RFM por cliente
# ────────────────────────────────────────────
# Dica: Agrupe por id_cliente, nome_cliente, cidade_cliente e calcule:
#   - recency: datediff(lit(data_ref), max("data_venda"))
#     → Dias desde a última compra
#   - frequency: countDistinct("id_venda")
#     → Número de compras únicas
#   - monetary: sum("valor_total")
#     → Valor total gasto
#   - ticket_medio: avg("valor_total")
#     → Ticket médio por compra

df_rfm = (
    df_vendas
    .groupBy("id_cliente", "nome_cliente", "cidade_cliente")
    .agg(
        # ▼▼▼ Seu código aqui ▼▼▼

        # ▲▲▲ Fim do TO-DO 1 ▲▲▲
    )
)

print(f"Clientes com compras: {df_rfm.count()}")
df_rfm.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preparar dados para clustering

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StandardScaler

feature_cols = ["recency", "frequency", "monetary"]

# TO-DO 2: Monte o vetor de features usando VectorAssembler
# ────────────────────────────────────────────────────────
# Dica:
#   assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
#   df_features = assembler.transform(df_rfm)
# ▼▼▼ Seu código aqui ▼▼▼

# ▲▲▲ Fim do TO-DO 2 ▲▲▲

# Normalizar features (já pronto)
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
scaler_model = scaler.fit(df_features)
df_scaled = scaler_model.transform(df_features)

print("Features preparadas e normalizadas!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Treinar modelo K-Means com MLflow

# COMMAND ----------

import mlflow
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

experiment_name = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/workshop_panvel_{nome}_rfm"
mlflow.set_experiment(experiment_name)

resultados = []

# TO-DO 3: Treine modelos K-Means para K = 3, 4, 5, 6
# ────────────────────────────────────────────────────
# Dica: Para cada valor de K:
#   1. Abra um run MLflow: with mlflow.start_run(run_name=f"kmeans_k{k}"):
#   2. Crie o modelo: kmeans = KMeans(k=k, seed=42, featuresCol="features", predictionCol="segmento")
#   3. Treine: model = kmeans.fit(df_scaled)
#   4. Predições: predictions = model.transform(df_scaled)
#   5. Avalie com silhouette score:
#      evaluator = ClusteringEvaluator(predictionCol="segmento", featuresCol="features", metricName="silhouette")
#      silhouette = evaluator.evaluate(predictions)
#   6. Log no MLflow:
#      mlflow.log_param("k", k)
#      mlflow.log_metric("silhouette_score", silhouette)
#      mlflow.spark.log_model(model, "model")
#   7. Adicione ao resultados: resultados.append({"k": k, "silhouette": silhouette})

for k in [3, 4, 5, 6]:
    # ▼▼▼ Seu código aqui ▼▼▼
    pass  # Substitua este pass pelo código
    # ▲▲▲ Fim do TO-DO 3 ▲▲▲

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Selecionar melhor modelo

# COMMAND ----------

# Selecionar o K com melhor silhouette
melhor = max(resultados, key=lambda x: x["silhouette"])
melhor_k = melhor["k"]
print(f"Melhor K: {melhor_k} (Silhouette: {melhor['silhouette']:.4f})")

# Retreinar modelo final
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

print(f"Modelo final treinado! Run ID: {run_id}")

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

# ▼▼▼ Seu código aqui ▼▼▼
df_segmentos = (
    predictions_final
    # Complete a agregação
)
# ▲▲▲ Fim do TO-DO 4 ▲▲▲

print("Perfil dos Segmentos:")
df_segmentos.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Salvar resultado

# COMMAND ----------

# Selecionar colunas relevantes para salvar
df_resultado = (
    predictions_final
    .select("id_cliente", "nome_cliente", "cidade_cliente",
            "recency", "frequency", "monetary", "ticket_medio", "segmento")
)

# TO-DO 5: Salve a tabela de segmentação no catálogo
# ──────────────────────────────────────────────────
# Dica: Use .write.mode("overwrite").saveAsTable(f"{catalog_name}.ml.segmentacao_clientes")
# ▼▼▼ Seu código aqui ▼▼▼

# ▲▲▲ Fim do TO-DO 5 ▲▲▲

print(f"Segmentação salva!")
print(f"Total de clientes segmentados: {df_resultado.count()}")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parabéns!
# MAGIC
# MAGIC Você completou o Lab de Machine Learning!
# MAGIC
# MAGIC **O que você aprendeu:**
# MAGIC - Feature Engineering com RFM
# MAGIC - Treinamento de K-Means com PySpark ML
# MAGIC - Tracking de experimentos com MLflow
# MAGIC - Registro de modelos no Unity Catalog
# MAGIC
# MAGIC **Próximo Lab:** AI/BI - Genie + Dashboard
