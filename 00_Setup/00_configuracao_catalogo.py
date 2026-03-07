# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Panvel - Configuração do Catálogo
# MAGIC
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/8/84/Panvel_Farm%C3%A1cias_logo.svg/1200px-Panvel_Farm%C3%A1cias_logo.svg.png" width="200"/>
# MAGIC
# MAGIC Este notebook cria o catálogo personalizado para cada participante do workshop.
# MAGIC
# MAGIC **Importante:** Preencha o widget `nome_participante` com seu primeiro nome (sem espaços, sem acentos, minúsculo).

# COMMAND ----------

# Widget para personalização
dbutils.widgets.text("nome_participante", "", "Seu Nome (sem espaços/acentos)")

# COMMAND ----------

nome = dbutils.widgets.get("nome_participante").strip().lower().replace(" ", "_")
assert nome != "", "Por favor, preencha seu nome no widget acima!"
print(f"Participante: {nome}")

catalog_name = f"workshop_panvel_{nome}"
print(f"Catálogo: {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando o Catálogo e Schemas

# COMMAND ----------

# Criar catálogo
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
print(f"Catálogo '{catalog_name}' criado com sucesso!")

# COMMAND ----------

# Criar schemas para cada camada
schemas = ["raw", "bronze", "silver", "gold", "ml"]

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")
    print(f"  Schema '{catalog_name}.{schema}' criado!")

# COMMAND ----------

# Criar volume para dados de streaming (JSON de vendas)
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog_name}.raw.vendas_json
    COMMENT 'Volume para arquivos JSON de vendas em streaming'
""")
print(f"Volume '{catalog_name}.raw.vendas_json' criado!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação

# COMMAND ----------

# Verificar estrutura criada
print(f"\n{'='*60}")
print(f"ESTRUTURA CRIADA COM SUCESSO!")
print(f"{'='*60}")
print(f"\nCatálogo: {catalog_name}")
print(f"\nSchemas:")
for schema in schemas:
    print(f"  - {catalog_name}.{schema}")
print(f"\nVolume:")
print(f"  - {catalog_name}.raw.vendas_json")
print(f"\n{'='*60}")
print(f"Próximo passo: Execute o notebook '01_dados_cadastrais'")
print(f"Use o mesmo nome_participante: {nome}")
print(f"{'='*60}")
