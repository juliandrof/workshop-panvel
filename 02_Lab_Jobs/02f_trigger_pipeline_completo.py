# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab2.png" width="100%"/>
# MAGIC
# MAGIC ## Tarefa 2: Trigger do Pipeline SDP via API (Completo)
# MAGIC
# MAGIC Este notebook busca o pipeline SDP criado no Lab 01 e dispara sua execução usando a API REST do Databricks.
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
#   2. Triggar execução:
#      response = requests.post(f"{db_host}/api/2.0/pipelines/{pipeline_id}/updates",
#                               headers=headers, json={"full_refresh": False})
#      update_id = response.json().get("update_id")
#
#   3. Monitorar (loop while):
#      response = requests.get(f"{db_host}/api/2.0/pipelines/{pipeline_id}/updates/{update_id}", headers=headers)
#      state = response.json().get("update", {}).get("state")
#      Estados finais: "COMPLETED", "FAILED", "CANCELED"

# 1. Buscar o pipeline pelo nome
pipeline_name = f"pipeline_panvel_{nome}"
print(f"Buscando pipeline: {pipeline_name}")

response = requests.get(
    f"{db_host}/api/2.0/pipelines",
    headers=headers,
    params={"filter": f"name LIKE '{pipeline_name}'", "max_results": 10},
)
response.raise_for_status()
pipelines = response.json().get("statuses", [])

pipeline_id = None
for p in pipelines:
    if p["name"] == pipeline_name:
        pipeline_id = p["pipeline_id"]
        break

assert pipeline_id is not None, f"Pipeline '{pipeline_name}' não encontrado! Crie-o primeiro no Lab 01."
print(f"Pipeline encontrado! ID: {pipeline_id}")

# 2. Verificar se o pipeline já está rodando e parar se necessário
print("Verificando estado do pipeline...")
response = requests.get(f"{db_host}/api/2.0/pipelines/{pipeline_id}", headers=headers)
response.raise_for_status()
pipeline_state = response.json().get("state", "UNKNOWN")
print(f"  Estado atual: {pipeline_state}")

if pipeline_state == "RUNNING":
    print("  Pipeline já está rodando. Parando execução atual...")
    response = requests.post(
        f"{db_host}/api/2.0/pipelines/{pipeline_id}/stop",
        headers=headers,
    )
    response.raise_for_status()
    # Aguardar pipeline parar
    while True:
        time.sleep(10)
        response = requests.get(f"{db_host}/api/2.0/pipelines/{pipeline_id}", headers=headers)
        response.raise_for_status()
        state = response.json().get("state", "UNKNOWN")
        if state != "RUNNING":
            print(f"  Pipeline parado. Estado: {state}")
            break
        print(f"  Aguardando pipeline parar... Estado: {state}")

# 3. Triggar a execução do pipeline
print("Iniciando pipeline...")
response = requests.post(
    f"{db_host}/api/2.0/pipelines/{pipeline_id}/updates",
    headers=headers,
    json={"full_refresh": False},
)
response.raise_for_status()
update_id = response.json().get("update_id")
print(f"Update iniciado! ID: {update_id}")

# 4. Monitorar a execução
print("Aguardando conclusão do pipeline...")
while True:
    response = requests.get(
        f"{db_host}/api/2.0/pipelines/{pipeline_id}/updates/{update_id}",
        headers=headers,
    )
    response.raise_for_status()
    update = response.json().get("update", {})
    state = update.get("state", "UNKNOWN")

    if state in ("COMPLETED",):
        print(f"Pipeline concluído com sucesso!")
        break
    elif state in ("FAILED", "CANCELED"):
        raise Exception(f"Pipeline falhou com estado: {state}")
    else:
        print(f"  Estado: {state} — aguardando 30s...")
        time.sleep(30)
