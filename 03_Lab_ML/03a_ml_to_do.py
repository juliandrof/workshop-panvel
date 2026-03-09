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
# MAGIC 2. Treinar modelo K-Means com scikit-learn
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

from pyspark.sql import functions as F

# Data de referência (mais recente no dataset)
data_ref = df_vendas.agg(F.max("data_venda")).collect()[0][0]
print(f"Data de referência: {data_ref}")

# TO-DO 1: Calcule as métricas RFM por cliente
# ────────────────────────────────────────────
# Dica: Agrupe por id_cliente, nome_cliente, cidade_cliente e calcule:
#   - recency: F.datediff(F.lit(data_ref), F.max("data_venda"))
#     → Dias desde a última compra
#   - frequency: F.countDistinct("id_venda")
#     → Número de compras únicas
#   - monetary: F.sum("valor_total")
#     → Valor total gasto
#   - ticket_medio: F.avg("valor_total")
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
#   scaled_cols = ["recency_scaled", "frequency_scaled", "monetary_scaled"]
#   pdf_rfm[scaled_cols] = scaler.fit_transform(pdf_rfm[feature_cols])
# ▼▼▼ Seu código aqui ▼▼▼

# ▲▲▲ Fim do TO-DO 2 ▲▲▲

scaled_cols = ["recency_scaled", "frequency_scaled", "monetary_scaled"]
print("Features preparadas e normalizadas!")

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
    kmeans_final = KMeans(n_clusters=melhor_k, random_state=42, n_init=10)
    kmeans_final.fit(X)
    silhouette_final = silhouette_score(X, kmeans_final.labels_)

    mlflow.log_param("k", melhor_k)
    mlflow.log_param("features", feature_cols)
    mlflow.log_param("modelo", "KMeans_final")
    mlflow.log_metric("silhouette_score", silhouette_final)
    mlflow.sklearn.log_model(kmeans_final, "model")

    run_id = run.info.run_id

# Adicionar segmentos ao DataFrame
pdf_rfm["segmento"] = kmeans_final.labels_

print(f"Modelo final treinado! Run ID: {run_id}")

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
#   Depois use .round(2).sort_index()

# ▼▼▼ Seu código aqui ▼▼▼
df_segmentos = (
    pdf_rfm
    .groupby("segmento")
    # Complete a agregação
)
# ▲▲▲ Fim do TO-DO 4 ▲▲▲

print("Perfil dos Segmentos:")
print(df_segmentos.to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Salvar resultado

# COMMAND ----------

# Selecionar colunas relevantes e converter de volta para Spark
colunas_salvar = ["id_cliente", "nome_cliente", "cidade_cliente",
                  "recency", "frequency", "monetary", "ticket_medio", "segmento"]

# TO-DO 5: Salve a tabela de segmentação no catálogo
# ──────────────────────────────────────────────────
# Dica:
#   df_resultado = spark.createDataFrame(pdf_rfm[colunas_salvar])
#   df_resultado.write.mode("overwrite").saveAsTable(f"{catalog_name}.ml.segmentacao_clientes")
# ▼▼▼ Seu código aqui ▼▼▼

# ▲▲▲ Fim do TO-DO 5 ▲▲▲

print(f"Segmentação salva!")
print(f"Total de clientes segmentados: {len(pdf_rfm)}")

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
# MAGIC - Treinamento de K-Means com scikit-learn
# MAGIC - Tracking de experimentos com MLflow
# MAGIC - Registro de modelos no Unity Catalog
# MAGIC
# MAGIC **Próximo Lab:** AI/BI - Genie + Dashboard
