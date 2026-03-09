# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab2.png" width="100%"/>
# MAGIC
# MAGIC ## Tarefa 2: Trigger do Pipeline SDP via API (Exercício)
# MAGIC
# MAGIC Complete os TO-DOs para buscar e executar o pipeline SDP via API REST!
# MAGIC
# MAGIC **Arquitetura do Workflow:**
# MAGIC ```
# MAGIC ┌────────────────┐     ┌──────────────────┐     ┌───────────────────┐     ┌───────────────────┐
# MAGIC │  Tarefa 1      │────▶│  Tarefa 2        │────▶│  Tarefa 3         │────▶│  Tarefa 4         │
# MAGIC │  Validar Dados │     │  Pipeline SDP    │     │  Qualidade Dados  │     │  Resumo           │
# MAGIC │                │     │  ★ ESTE ★        │     │                   │     │                   │
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
# MAGIC ### Trigger do Pipeline SDP

# COMMAND ----------

import requests
import time

# Obter contexto de autenticação do Databricks
db_host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
db_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

headers = {"Authorization": f"Bearer {db_token}", "Content-Type": "application/json"}

# TO-DO 2: Busque o pipeline pelo nome e dispare a execução via API
# ─────────────────────────────────────────────────────────────────
# Dica:
#   1. Buscar pipeline:
#      response = requests.get(f"{db_host}/api/2.0/pipelines", headers=headers,
#                              params={"filter": f"name LIKE 'pipeline_panvel_{nome}'"})
#      pipelines = response.json().get("statuses", [])
#      Encontre o pipeline_id no resultado
#
#   2. Verificar se já está rodando (evitar erro 409 Conflict):
#      response = requests.get(f"{db_host}/api/2.0/pipelines/{pipeline_id}", headers=headers)
#      Se state == "RUNNING", pare com:
#        requests.post(f"{db_host}/api/2.0/pipelines/{pipeline_id}/stop", headers=headers)
#      Aguarde até o estado mudar de "RUNNING"
#
#   3. Triggar execução:
#      response = requests.post(f"{db_host}/api/2.0/pipelines/{pipeline_id}/updates",
#                               headers=headers, json={"full_refresh": False})
#      update_id = response.json().get("update_id")
#
#   4. Monitorar (loop while):
#      response = requests.get(f"{db_host}/api/2.0/pipelines/{pipeline_id}/updates/{update_id}", headers=headers)
#      state = response.json().get("update", {}).get("state")
#      Estados finais: "COMPLETED", "FAILED", "CANCELED"

pipeline_name = f"pipeline_panvel_{nome}"
print(f"Buscando pipeline: {pipeline_name}")

# ▼▼▼ Seu código aqui ▼▼▼

# ▲▲▲ Fim do TO-DO 2 ▲▲▲
