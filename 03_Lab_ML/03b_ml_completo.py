# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab3.png" width="100%"/>
# MAGIC
# MAGIC ## Segmentação de Clientes com RFM + K-Means (Versão Completa)
# MAGIC
# MAGIC Neste lab, vamos:
# MAGIC 1. Criar features RFM (Recency, Frequency, Monetary) a partir dos dados de vendas
# MAGIC 2. Treinar um modelo de clustering K-Means com scikit-learn
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

from pyspark.sql import functions as F

# Data de referência (mais recente no dataset)
data_ref = df_vendas.agg(F.max("data_venda")).collect()[0][0]
print(f"Data de referência: {data_ref}")

# TO-DO 1: Calcule as métricas RFM por cliente
# ────────────────────────────────────────────
# Dica: Calcule:
#   - recency: F.datediff(F.lit(data_ref), F.max("data_venda"))
#   - frequency: F.countDistinct("id_venda")
#   - monetary: F.sum("valor_total")
#   - ticket_medio: F.avg("valor_total")
# Calcular RFM por cliente
df_rfm = (
    df_vendas
    .groupBy("id_cliente", "nome_cliente", "cidade_cliente")
    .agg(
        # Recency: dias desde a última compra
        F.datediff(F.lit(data_ref), F.max("data_venda")).alias("recency"),
        # Frequency: número de compras
        F.countDistinct("id_venda").alias("frequency"),
        # Monetary: valor total gasto
        F.sum("valor_total").alias("monetary"),
        # Ticket médio
        F.avg("valor_total").alias("ticket_medio"),
    )
)

print(f"Clientes com compras: {df_rfm.count()}")
df_rfm.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preparar dados para clustering
# MAGIC
# MAGIC Convertemos para Pandas e usamos **scikit-learn** para normalização e clustering.
# MAGIC Isso garante compatibilidade com clusters Serverless e Shared Access Mode.

# COMMAND ----------

import pandas as pd
from sklearn.preprocessing import StandardScaler

feature_cols = ["recency", "frequency", "monetary"]

# TO-DO 2: Converta para Pandas e normalize as features com StandardScaler
# ────────────────────────────────────────────────────────────────────────
# Dica:
#   pdf_rfm = df_rfm.toPandas()
#   scaler = StandardScaler()
#   pdf_rfm[["recency_scaled", "frequency_scaled", "monetary_scaled"]] = scaler.fit_transform(pdf_rfm[feature_cols])
# Converter para Pandas
pdf_rfm = df_rfm.toPandas()

# Normalizar features
scaler = StandardScaler()
scaled_cols = ["recency_scaled", "frequency_scaled", "monetary_scaled"]
pdf_rfm[scaled_cols] = scaler.fit_transform(pdf_rfm[feature_cols])

print("Features preparadas e normalizadas!")
print(f"Shape: {pdf_rfm.shape}")
pdf_rfm[["id_cliente", "nome_cliente"] + feature_cols + scaled_cols].head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Treinar modelo K-Means com MLflow

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# Configurar experimento MLflow
experiment_name = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/workshop_panvel_{nome}_rfm"
mlflow.set_experiment(experiment_name)

# Features normalizadas para clustering
X = pdf_rfm[scaled_cols].values

# Testar diferentes valores de K
resultados = []

# TO-DO 3: Treine modelos K-Means para K = 3, 4, 5, 6
# ────────────────────────────────────────────────────
# Dica: Para cada valor de K:
#   1. Abra um run MLflow: with mlflow.start_run(run_name=f"kmeans_k{k}"):
#   2. Crie o modelo: kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
#   3. Treine: kmeans.fit(X)
#   4. Calcule silhouette: silhouette = silhouette_score(X, kmeans.labels_)
#   5. Log no MLflow:
#      mlflow.log_param("k", k)
#      mlflow.log_metric("silhouette_score", silhouette)
#      mlflow.sklearn.log_model(kmeans, "model")
#   6. Adicione ao resultados: resultados.append({"k": k, "silhouette": silhouette})
for k in [3, 4, 5, 6]:
    with mlflow.start_run(run_name=f"kmeans_k{k}"):
        # Treinar modelo
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        kmeans.fit(X)

        # Avaliar - Silhouette Score
        silhouette = silhouette_score(X, kmeans.labels_)

        # Log no MLflow
        mlflow.log_param("k", k)
        mlflow.log_param("features", feature_cols)
        mlflow.log_metric("silhouette_score", silhouette)
        mlflow.sklearn.log_model(kmeans, "model")

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
    kmeans_final = KMeans(n_clusters=melhor_k, random_state=42, n_init=10)
    kmeans_final.fit(X)
    silhouette_final = silhouette_score(X, kmeans_final.labels_)

    mlflow.log_param("k", melhor_k)
    mlflow.log_param("features", feature_cols)
    mlflow.log_param("modelo", "KMeans_final")
    mlflow.log_metric("silhouette_score", silhouette_final)
    mlflow.sklearn.log_model(kmeans_final, "model")

    run_id = run.info.run_id
    print(f"Run ID: {run_id}")

# Adicionar segmentos ao DataFrame
pdf_rfm["segmento"] = kmeans_final.labels_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Análise dos Segmentos

# COMMAND ----------

# TO-DO 4: Analise o perfil de cada segmento
# ──────────────────────────────────────────
# Dica: Use pdf_rfm.groupby("segmento").agg() com:
#   - num_clientes: ("id_cliente", "count")
#   - recency_medio: ("recency", "mean")
#   - frequency_media: ("frequency", "mean")
#   - monetary_medio: ("monetary", "mean")
#   - ticket_medio: ("ticket_medio", "mean")
# Analisar perfil de cada segmento
df_segmentos = (
    pdf_rfm
    .groupby("segmento")
    .agg(
        num_clientes=("id_cliente", "count"),
        recency_medio=("recency", "mean"),
        frequency_media=("frequency", "mean"),
        monetary_medio=("monetary", "mean"),
        ticket_medio=("ticket_medio", "mean"),
    )
    .round(2)
    .sort_index()
)

print("Perfil dos Segmentos:")
print(df_segmentos.to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Salvar resultado na camada ML

# COMMAND ----------

# TO-DO 5: Salve a tabela de segmentação no catálogo
# ──────────────────────────────────────────────────
# Dica:
#   df_resultado = spark.createDataFrame(pdf_rfm[colunas])
#   df_resultado.write.mode("overwrite").saveAsTable(f"{catalog_name}.ml.segmentacao_clientes")
# Selecionar colunas relevantes e converter de volta para Spark
colunas_salvar = ["id_cliente", "nome_cliente", "cidade_cliente",
                  "recency", "frequency", "monetary", "ticket_medio", "segmento"]
df_resultado = spark.createDataFrame(pdf_rfm[colunas_salvar])

# Salvar segmentação
(
    df_resultado
    .write
    .mode("overwrite")
    .saveAsTable(f"{catalog_name}.ml.segmentacao_clientes")
)

print(f"Segmentação salva em {catalog_name}.ml.segmentacao_clientes")
print(f"Total de clientes segmentados: {len(pdf_rfm)}")

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
