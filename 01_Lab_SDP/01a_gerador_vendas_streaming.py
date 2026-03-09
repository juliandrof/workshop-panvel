# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_lab1.png" width="100%"/>
# MAGIC
# MAGIC Este notebook gera arquivos JSON de vendas continuamente:
# MAGIC - **1 arquivo por loja** a cada 20 segundos
# MAGIC - **50 vendas** por arquivo
# MAGIC - **4 a 10 itens** aleatórios por venda
# MAGIC
# MAGIC **Deixe este notebook rodando** enquanto trabalha no pipeline SDP.

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
# MAGIC ## Carregar dados de referência

# COMMAND ----------

import json
import random
import time
from datetime import datetime, timedelta

# Carregar dados de referência
df_lojas = spark.table(f"{catalog_name}.raw.lojas")
df_clientes = spark.table(f"{catalog_name}.raw.clientes")
df_produtos = spark.table(f"{catalog_name}.raw.produtos")

lojas = df_lojas.collect()
clientes = [row.id_cliente for row in df_clientes.collect()]
produtos = [(row.id_produto, row.preco_referencia) for row in df_produtos.collect()]

print(f"Lojas carregadas: {len(lojas)}")
print(f"Clientes carregados: {len(clientes)}")
print(f"Produtos carregados: {len(produtos)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Caminho do Volume para os JSONs

# COMMAND ----------

volume_path = f"/Volumes/{catalog_name}/raw/vendas_json"
print(f"Arquivos serão salvos em: {volume_path}")

# Limpar arquivos anteriores (opcional)
try:
    dbutils.fs.rm(volume_path + "/", True)
    print("Arquivos anteriores removidos.")
except:
    print("Nenhum arquivo anterior encontrado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Geração de Vendas

# COMMAND ----------

venda_id_global = 1

def gerar_vendas_loja(id_loja, nome_loja, num_vendas=50):
    """Gera um lote de vendas para uma loja específica."""
    global venda_id_global
    vendas = []

    for _ in range(num_vendas):
        # Dados da venda
        num_itens = random.randint(4, 10)
        id_cliente = random.choice(clientes)
        data_venda = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Gerar itens da venda
        itens = []
        valor_total_venda = 0.0
        produtos_selecionados = random.sample(produtos, num_itens)

        for id_produto, preco_ref in produtos_selecionados:
            quantidade = random.randint(1, 5)
            # Variação de preço de -5% a +10%
            valor_unitario = round(preco_ref * random.uniform(0.95, 1.10), 2)
            desconto = round(random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20]) * valor_unitario * quantidade, 2)
            valor_total_item = round(valor_unitario * quantidade - desconto, 2)

            itens.append({
                "id_venda": venda_id_global,
                "id_produto": id_produto,
                "quantidade": quantidade,
                "valor_unitario": valor_unitario,
                "valor_total": valor_total_item,
                "desconto": desconto
            })
            valor_total_venda += valor_total_item

        venda = {
            "id_venda": venda_id_global,
            "id_cliente": id_cliente,
            "id_loja": id_loja,
            "data_venda": data_venda,
            "valor_total": round(valor_total_venda, 2),
            "itens": itens
        }
        vendas.append(venda)
        venda_id_global += 1

    return vendas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loop de Geração Contínua
# MAGIC
# MAGIC Este loop gera **1 arquivo JSON por loja** a cada 20 segundos.
# MAGIC
# MAGIC **Para parar:** Clique em "Cancel" ou "Interrupt" no notebook.

# COMMAND ----------

batch_num = 0

print("Iniciando geração de vendas em streaming...")
print(f"Total de lojas: {len(lojas)}")
print(f"Vendas por arquivo: 50")
print(f"Itens por venda: 4 a 10")
print(f"Intervalo entre lotes: 20 segundos")
print(f"{'='*60}")

while True:
    batch_num += 1
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for loja in lojas:
        # Gerar vendas para esta loja
        vendas = gerar_vendas_loja(loja.id_loja, loja.nome_loja, num_vendas=50)

        # Montar nome do arquivo: loja_ID_timestamp.json
        nome_arquivo = f"vendas_loja_{loja.id_loja:03d}_{timestamp}.json"
        caminho_arquivo = f"{volume_path}/{nome_arquivo}"

        # Salvar como JSON (cada linha um registro JSON - formato JSON Lines)
        conteudo = "\n".join([json.dumps(v, ensure_ascii=False) for v in vendas])

        dbutils.fs.put(caminho_arquivo, conteudo, overwrite=True)

    total_vendas_batch = len(lojas) * 50
    print(f"[Lote {batch_num:04d}] {timestamp} | {len(lojas)} arquivos | {total_vendas_batch} vendas | ID até {venda_id_global - 1}")

    # Aguardar 20 segundos
    time.sleep(20)
