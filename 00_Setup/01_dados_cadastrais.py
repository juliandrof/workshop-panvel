# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/juliandrof/workshop-panvel/main/images/header_setup.png" width="100%"/>
# MAGIC
# MAGIC Este notebook gera os dados sintéticos de cadastro:
# MAGIC - **Clientes**: 1.000 clientes de cidades do RS
# MAGIC - **Lojas**: ~120 lojas Panvel em cidades do RS
# MAGIC - **Produtos**: 400 produtos de farmácia em diversas categorias

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
# MAGIC ## 1. Cidades do Rio Grande do Sul

# COMMAND ----------

cidades_rs = [
    "Porto Alegre", "Canoas", "Pelotas", "Caxias do Sul", "Santa Maria",
    "Gravataí", "Viamão", "Novo Hamburgo", "São Leopoldo", "Rio Grande",
    "Alvorada", "Passo Fundo", "Sapucaia do Sul", "Uruguaiana", "Santa Cruz do Sul",
    "Cachoeirinha", "Bagé", "Bento Gonçalves", "Erechim", "Guaíba",
    "Cachoeira do Sul", "Sapiranga", "Lajeado", "Ijuí", "Alegrete",
    "Esteio", "Santana do Livramento", "Torres", "Santo Ângelo", "Cruz Alta",
    "Venâncio Aires", "Farroupilha", "Camaquã", "São Borja", "Tramandaí",
    "Montenegro", "Parobé", "Estância Velha", "Taquara", "Campo Bom",
    "São Gabriel", "Capão da Canoa", "Santa Rosa", "Carazinho", "Vacaria",
    "Osório", "Garibaldi", "Flores da Cruz", "Canela", "Gramado"
]

print(f"Total de cidades: {len(cidades_rs)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cadastro de Clientes

# COMMAND ----------

import random
random.seed(42)

nomes_masculinos = [
    "João", "Pedro", "Carlos", "José", "Antonio", "Francisco", "Paulo", "Lucas",
    "Marcos", "Rafael", "Gabriel", "Bruno", "Felipe", "Gustavo", "Matheus",
    "Leonardo", "Diego", "Thiago", "Eduardo", "Rodrigo", "André", "Daniel",
    "Fernando", "Ricardo", "Alexandre", "Henrique", "Vinicius", "Marcelo",
    "Roberto", "Sérgio", "Leandro", "Guilherme", "Renato", "Fábio", "Márcio"
]

nomes_femininos = [
    "Maria", "Ana", "Juliana", "Fernanda", "Patrícia", "Camila", "Beatriz",
    "Amanda", "Letícia", "Carolina", "Gabriela", "Larissa", "Mariana", "Aline",
    "Bruna", "Natália", "Vanessa", "Tatiana", "Priscila", "Renata", "Carla",
    "Luciana", "Sandra", "Cláudia", "Daniela", "Simone", "Cristiane", "Michele",
    "Débora", "Raquel", "Elaine", "Mônica", "Paula", "Adriana", "Silvia"
]

sobrenomes = [
    "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Alves",
    "Pereira", "Lima", "Gomes", "Costa", "Ribeiro", "Martins", "Carvalho",
    "Araújo", "Melo", "Barbosa", "Rocha", "Cardoso", "Nascimento",
    "Moreira", "Nunes", "Teixeira", "Vieira", "Monteiro", "Mendes",
    "Correia", "Freitas", "Machado", "Pinto", "Reis", "Lopes",
    "Marques", "Andrade", "Batista", "Duarte", "Dias", "Moraes",
    "Campos", "Castro"
]

clientes = []
for i in range(1, 1001):
    if random.random() < 0.5:
        primeiro_nome = random.choice(nomes_masculinos)
    else:
        primeiro_nome = random.choice(nomes_femininos)
    sobrenome = f"{random.choice(sobrenomes)} {random.choice(sobrenomes)}"
    nome_completo = f"{primeiro_nome} {sobrenome}"
    cidade = random.choice(cidades_rs)
    clientes.append((i, nome_completo, cidade))

df_clientes = spark.createDataFrame(clientes, ["id_cliente", "nome", "cidade"])
df_clientes.write.mode("overwrite").saveAsTable(f"{catalog_name}.raw.clientes")
print(f"Tabela de clientes criada com {df_clientes.count()} registros!")
df_clientes.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cadastro de Lojas

# COMMAND ----------

# Bairros por cidade - lojas no formato "Panvel - Bairro Cidade"
bairros_por_cidade = {
    "Porto Alegre": ["Centro", "Moinhos de Vento", "Bela Vista", "Menino Deus", "Cidade Baixa",
                     "Petrópolis", "Bom Fim", "Higienópolis", "Mont'Serrat", "Auxiliadora",
                     "Passo d'Areia", "Cristo Redentor", "Jardim Botânico", "Três Figueiras",
                     "Cavalhada", "Cristal", "Ipanema", "Tristeza", "Restinga", "Lomba do Pinheiro"],
    "Canoas": ["Centro", "Mathias Velho", "Niterói", "Harmonia", "Estância Velha", "Igara"],
    "Pelotas": ["Centro", "Fragata", "Areal", "Três Vendas", "Laranjal"],
    "Caxias do Sul": ["Centro", "Exposição", "São Pelegrino", "Pio X", "De Lazzer", "Nossa Senhora de Lourdes"],
    "Santa Maria": ["Centro", "Camobi", "Nossa Senhora de Lourdes", "Patronato"],
    "Gravataí": ["Centro", "Morada do Vale", "Salgado Filho"],
    "Viamão": ["Centro", "São Lucas"],
    "Novo Hamburgo": ["Centro", "Hamburgo Velho", "Canudos", "Rio Branco", "Ideal"],
    "São Leopoldo": ["Centro", "Scharlau", "Feitoria", "Cristo Rei"],
    "Rio Grande": ["Centro", "Cassino", "Cidade Nova"],
    "Alvorada": ["Centro", "Stella Maris"],
    "Passo Fundo": ["Centro", "São Cristóvão", "Boqueirão", "Vila Rodrigues"],
    "Sapucaia do Sul": ["Centro", "Capão da Cruz"],
    "Uruguaiana": ["Centro", "São Miguel"],
    "Santa Cruz do Sul": ["Centro", "Universitário", "Avenida"],
    "Cachoeirinha": ["Centro", "Vila Veranópolis"],
    "Bagé": ["Centro", "Getúlio Vargas"],
    "Bento Gonçalves": ["Centro", "São Roque", "Cidade Alta"],
    "Erechim": ["Centro", "Três Vendas"],
    "Guaíba": ["Centro"],
    "Cachoeira do Sul": ["Centro", "Soares"],
    "Sapiranga": ["Centro"],
    "Lajeado": ["Centro", "Montanha"],
    "Ijuí": ["Centro", "Modelo"],
    "Alegrete": ["Centro"],
    "Esteio": ["Centro", "Primavera"],
    "Santana do Livramento": ["Centro", "Armour"],
    "Torres": ["Centro", "Praia Grande"],
    "Santo Ângelo": ["Centro"],
    "Cruz Alta": ["Centro"],
    "Venâncio Aires": ["Centro"],
    "Farroupilha": ["Centro"],
    "Camaquã": ["Centro"],
    "São Borja": ["Centro"],
    "Tramandaí": ["Centro", "Beira Mar"],
    "Montenegro": ["Centro"],
    "Parobé": ["Centro"],
    "Estância Velha": ["Centro"],
    "Taquara": ["Centro"],
    "Campo Bom": ["Centro"],
    "São Gabriel": ["Centro"],
    "Capão da Canoa": ["Centro", "Navegantes"],
    "Santa Rosa": ["Centro"],
    "Carazinho": ["Centro"],
    "Vacaria": ["Centro"],
    "Osório": ["Centro"],
    "Garibaldi": ["Centro"],
    "Flores da Cruz": ["Centro"],
    "Canela": ["Centro"],
    "Gramado": ["Centro", "Bavária"]
}

lojas = []
loja_id = 1
for cidade, bairros in bairros_por_cidade.items():
    bairro_count = {}
    for bairro in bairros:
        bairro_count[bairro] = bairro_count.get(bairro, 0) + 1

    # Verificar se algum bairro aparece mais de uma vez
    for bairro in bairros:
        count = bairros.count(bairro)
        if count > 1:
            # Adicionar número incremental
            for seq in range(1, count + 1):
                nome_loja = f"Panvel - {bairro} {seq}"
                lojas.append((loja_id, nome_loja, cidade))
                loja_id += 1
        else:
            nome_loja = f"Panvel - {bairro}"
            lojas.append((loja_id, nome_loja, cidade))
            loja_id += 1

df_lojas = spark.createDataFrame(lojas, ["id_loja", "nome_loja", "cidade"])
df_lojas.write.mode("overwrite").saveAsTable(f"{catalog_name}.raw.lojas")
print(f"Tabela de lojas criada com {df_lojas.count()} registros!")
df_lojas.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cadastro de Produtos (400 produtos)

# COMMAND ----------

produtos = []
prod_id = 1

categorias_produtos = {
    "Dermocosméticos": [
        ("La Roche-Posay Effaclar Duo+", 89.90), ("Vichy Mineral 89", 129.90),
        ("Bioderma Sensibio H2O 250ml", 79.90), ("Neutrogena Hydro Boost Gel", 69.90),
        ("Eucerin Hyaluron-Filler Creme", 159.90), ("Avène Eau Thermale Spray 150ml", 59.90),
        ("RoC Retinol Correxion Sérum", 189.90), ("CeraVe Loção Hidratante 200ml", 49.90),
        ("La Roche-Posay Cicaplast B5", 59.90), ("Vichy Liftactiv Supreme", 179.90),
        ("Bioderma Atoderm Creme 200ml", 69.90), ("Neutrogena Retinol Boost Sérum", 99.90),
        ("Eucerin Sun Gel-Creme FPS 60", 79.90), ("Avène Cleanance Gel 200ml", 69.90),
        ("CeraVe Creme Hidratante 454g", 79.90), ("La Roche-Posay Hyalu B5 Sérum", 169.90),
        ("Vichy Normaderm Phytosolution", 99.90), ("Bioderma Sébium Gel 200ml", 59.90),
        ("Neutrogena Age Perfect Creme", 89.90), ("RoC Multi Correxion Gel Creme", 149.90),
    ],
    "Higiene Pessoal": [
        ("Dove Sabonete Original 90g", 3.99), ("Rexona Clinical Aerosol 150ml", 29.90),
        ("Nivea Creme Hidratante 97g", 12.90), ("Lux Sabonete Líquido 250ml", 9.90),
        ("Dove Desodorante Roll-On 50ml", 14.90), ("Protex Sabonete Antibacteriano 85g", 4.49),
        ("Rexona Motionsense Roll-On 50ml", 12.90), ("Nivea Desodorante Spray 150ml", 19.90),
        ("Johnson's Sabonete Líquido 200ml", 8.90), ("Palmolive Sabonete Naturals 85g", 2.99),
        ("Dove Creme Hidratante Corporal 200ml", 19.90), ("Nivea Loção Corporal 200ml", 16.90),
        ("Granado Sabonete Glicerina 90g", 5.90), ("Phebo Sabonete 90g", 6.90),
        ("Dove Men+Care Sabonete 90g", 4.49), ("Rexona Creme Desodorante 58g", 8.90),
        ("Dove Sabonete Karité 90g", 4.49), ("Nivea Q10 Creme Corporal 200ml", 24.90),
        ("Soapex Sabonete Antisséptico 80g", 15.90), ("Dove Baby Sabonete 75g", 5.90),
    ],
    "Bebê e Infantil": [
        ("Pampers Premium Care Fralda G 68un", 89.90), ("Huggies Supreme Care M 80un", 79.90),
        ("Johnson's Baby Shampoo 200ml", 14.90), ("Pampers Pants XG 56un", 74.90),
        ("Huggies Turma da Mônica G 72un", 69.90), ("Johnson's Baby Óleo 200ml", 18.90),
        ("Bepantol Baby Creme 30g", 29.90), ("Hipoglós Creme 45g", 14.90),
        ("Johnson's Baby Talco 200g", 12.90), ("Mustela Gel Lavante 500ml", 89.90),
        ("Granado Bebê Sabonete Líquido 250ml", 19.90), ("Pampers Recém-Nascido RN 36un", 29.90),
        ("Huggies Lenços Umedecidos 96un", 16.90), ("Johnson's Baby Creme Assaduras 45g", 15.90),
        ("Mustela Hydra Bebê Creme 40ml", 39.90), ("Weleda Calêndula Creme 75ml", 49.90),
        ("Klorane Petit Junior Shampoo 200ml", 34.90), ("Johnsons Baby Condicionador 200ml", 14.90),
        ("Huggies Natural Care Lenços 48un", 12.90), ("Avent Mamadeira Classic 260ml", 49.90),
    ],
    "Cardiovascular e Hipertensão": [
        ("Losartana Potássica 50mg 30cp", 12.90), ("Atenolol 25mg 30cp", 8.90),
        ("Anlodipino 5mg 30cp", 9.90), ("Enalapril 10mg 30cp", 7.90),
        ("Propranolol 40mg 30cp", 6.90), ("Hidroclorotiazida 25mg 30cp", 5.90),
        ("Valsartana 160mg 30cp", 39.90), ("Metoprolol 50mg 30cp", 19.90),
        ("Captopril 25mg 30cp", 6.90), ("Nifedipino 20mg 30cp", 9.90),
        ("Carvedilol 6.25mg 30cp", 12.90), ("Espironolactona 25mg 30cp", 14.90),
        ("AAS Infantil 100mg 30cp", 6.90), ("Sinvastatina 20mg 30cp", 14.90),
        ("Atorvastatina 10mg 30cp", 19.90), ("Rosuvastatina 10mg 30cp", 29.90),
        ("Clopidogrel 75mg 30cp", 24.90), ("Varfarina 5mg 30cp", 9.90),
        ("Furosemida 40mg 30cp", 7.90), ("Lisinopril 10mg 30cp", 14.90),
    ],
    "Vitaminas e Suplementos": [
        ("Vitamina C 1000mg 30cp Efervescente", 19.90), ("Ômega 3 1000mg 60 cápsulas", 39.90),
        ("Vitamina D3 2000UI 30 cápsulas", 24.90), ("Centrum Multivitamínico 60cp", 69.90),
        ("Lavitan A-Z 60 drágeas", 29.90), ("Cálcio + Vitamina D 60cp", 24.90),
        ("Magnésio Dimalato 60 cápsulas", 34.90), ("Zinco Quelado 30mg 60cp", 19.90),
        ("Complexo B 100 comprimidos", 14.90), ("Ferro Quelado 30mg 60cp", 19.90),
        ("Coenzima Q10 100mg 60cp", 59.90), ("Colágeno Hidrolisado 300g", 49.90),
        ("Biotina 5mg 60 cápsulas", 29.90), ("Ácido Fólico 5mg 30cp", 9.90),
        ("Melatonina 3mg 60cp", 39.90), ("Spirulina 500mg 60cp", 29.90),
        ("Vitamina E 400UI 30 cápsulas", 19.90), ("Selênio 100mcg 60cp", 24.90),
        ("Probiótico 30 cápsulas", 49.90), ("Glucosamina + Condroitina 30cp", 39.90),
    ],
    "Analgésicos e Antitérmicos": [
        ("Dipirona Sódica 500mg 30cp", 6.90), ("Paracetamol 750mg 20cp", 4.90),
        ("Ibuprofeno 400mg 20cp", 8.90), ("Aspirina 500mg 20cp", 7.90),
        ("Novalgina Gotas 20ml", 12.90), ("Tylenol 750mg 20cp", 9.90),
        ("Dorflex 36cp", 14.90), ("Neosaldina 20 drágeas", 12.90),
        ("Buscopan Composto 20cp", 19.90), ("Torsilax 15cp", 14.90),
        ("Cefaliv 12cp", 9.90), ("Doril 12cp", 6.90),
        ("Cibalena 20cp", 8.90), ("Benegrip 20cp", 12.90),
        ("Naldecon Noite 24cp", 14.90), ("Resfenol 20 cápsulas", 11.90),
        ("Advil 400mg 8cp", 12.90), ("Flanax 275mg 10cp", 14.90),
        ("Naproxeno 500mg 20cp", 12.90), ("Nimesulida 100mg 12cp", 8.90),
    ],
    "Diabetes": [
        ("Metformina 850mg 30cp", 9.90), ("Glibenclamida 5mg 30cp", 6.90),
        ("Fita Reagente Accu-Chek 50un", 89.90), ("Glicosímetro Accu-Chek Guide", 99.90),
        ("Lanceta Accu-Chek 200un", 49.90), ("Insulina Lantus 3ml", 89.90),
        ("Seringa de Insulina 1ml 100un", 29.90), ("Gliclazida 60mg 30cp", 24.90),
        ("Fita Reagente FreeStyle 50un", 79.90), ("Glicosímetro FreeStyle Libre", 199.90),
        ("Agulha para Caneta 32G 100un", 39.90), ("Dapagliflozina 10mg 30cp", 119.90),
        ("Sitagliptina 100mg 30cp", 99.90), ("Empagliflozina 25mg 30cp", 139.90),
        ("Álcool Swab Caixa 100un", 12.90), ("Glimepirida 2mg 30cp", 14.90),
        ("Pioglitazona 30mg 30cp", 29.90), ("Linagliptina 5mg 30cp", 89.90),
        ("Caneta de Insulina NovoPen", 149.90), ("Adoçante Stevia 100ml", 9.90),
    ],
    "Cuidados Capilares": [
        ("Pantene Shampoo Restauração 400ml", 19.90), ("TRESemmé Shampoo Hidratação 400ml", 18.90),
        ("Elseve Shampoo Glycolic Gloss 400ml", 22.90), ("Head & Shoulders Shampoo 200ml", 21.90),
        ("Seda Shampoo Cocriações 325ml", 12.90), ("Dove Shampoo Reconstrução 400ml", 19.90),
        ("Clear Shampoo Anticaspa 200ml", 18.90), ("Johnson's Baby Shampoo 400ml", 24.90),
        ("Pantene Condicionador 400ml", 19.90), ("TRESemmé Máscara Capilar 300g", 16.90),
        ("Elseve Óleo Extraordinário 100ml", 29.90), ("Dove Creme de Pentear 300ml", 14.90),
        ("Kerastase Shampoo Bain 250ml", 149.90), ("Redken All Soft Shampoo 300ml", 119.90),
        ("Skala Expert Shampoo 325ml", 8.90), ("Salon Line SOS Cachos Creme 300ml", 12.90),
        ("Phytoervas Shampoo Antiqueda 250ml", 14.90), ("Vichy Dercos Shampoo 200ml", 69.90),
        ("Ducray Anaphase Shampoo 200ml", 89.90), ("Kérastase Elixir Ultime 100ml", 199.90),
    ],
    "Proteção Solar": [
        ("Neutrogena Sun Fresh FPS 70 200ml", 49.90), ("La Roche-Posay Anthelios FPS 70 40g", 69.90),
        ("Sundown FPS 50 200ml", 34.90), ("Nivea Sun Protect FPS 50 200ml", 39.90),
        ("Australian Gold FPS 50 200g", 59.90), ("Banana Boat FPS 50 200ml", 44.90),
        ("Avène FPS 50+ Fluido 50ml", 79.90), ("Vichy Capital Soleil FPS 50 40g", 69.90),
        ("Bioré UV Aqua Rich FPS 50 50g", 59.90), ("Episol Color FPS 70 40g", 64.90),
        ("Cenoura & Bronze FPS 30 200ml", 24.90), ("Coppertone FPS 50 200ml", 39.90),
        ("Isdin Fotoprotector FPS 50 50ml", 79.90), ("Eucerin Sun FPS 60 52g", 89.90),
        ("ROC Minesol FPS 70 40g", 74.90), ("Adcos Protetor Solar FPS 55 40g", 89.90),
        ("Anthelios Airlicium FPS 70 40g", 79.90), ("Protetor Labial FPS 30 4.5g", 14.90),
        ("Sundown Kids FPS 60 120ml", 39.90), ("Australian Gold Bronzeador 237ml", 44.90),
    ],
    "Perfumaria": [
        ("Natura Essencial Masculino 100ml", 149.90), ("O Boticário Malbec 100ml", 179.90),
        ("Natura Luna Feminino 75ml", 119.90), ("O Boticário Lily 75ml", 159.90),
        ("Natura Kaiak Masculino 100ml", 99.90), ("O Boticário Egeo 90ml", 129.90),
        ("Natura Humor Feminino 75ml", 89.90), ("O Boticário Floratta 75ml", 109.90),
        ("Natura Homem Essence 100ml", 129.90), ("O Boticário Quasar 100ml", 99.90),
        ("Eudora Instance 100ml", 79.90), ("Avon 300km/h 100ml", 69.90),
        ("Phytoderm Feminino 100ml", 39.90), ("Antonio Banderas The Icon 100ml", 129.90),
        ("Carolina Herrera Good Girl 30ml", 349.90), ("Paco Rabanne 1 Million 50ml", 299.90),
        ("Calvin Klein CK One 100ml", 199.90), ("Ralph Lauren Polo 75ml", 289.90),
        ("Dior Sauvage 60ml", 449.90), ("Chanel Chance 50ml", 529.90),
    ],
    "Primeiros Socorros": [
        ("Band-Aid Curativo 40un", 12.90), ("Gaze Estéril 10un", 4.90),
        ("Esparadrapo Micropore 25mm x 10m", 9.90), ("Álcool 70% 1L", 8.90),
        ("Água Oxigenada 10vol 100ml", 3.90), ("Merthiolate Spray 45ml", 14.90),
        ("Atadura de Crepom 10cm", 3.90), ("Luva Descartável 100un", 24.90),
        ("Termômetro Digital", 19.90), ("Algodão Hidrófilo 50g", 5.90),
        ("Soro Fisiológico 500ml", 7.90), ("Clorexidina Tópica 100ml", 9.90),
        ("Cotonete 150un", 6.90), ("Fita Adesiva Cirúrgica 2.5cm", 8.90),
        ("Compressa de Gaze 7.5cm 10un", 4.90), ("Pomada Nebacetin 15g", 19.90),
        ("Rifocina Spray 20ml", 24.90), ("Curativo Líquido Band-Aid", 19.90),
        ("Bolsa Térmica Gel", 14.90), ("Máscara Descartável 50un", 12.90),
    ],
    "Saúde Bucal": [
        ("Colgate Total 12 Creme Dental 90g", 8.90), ("Oral-B Pro-Saúde Escova Dental", 14.90),
        ("Listerine Cool Mint 500ml", 24.90), ("Sensodyne Repair & Protect 100g", 19.90),
        ("Fio Dental Oral-B 50m", 9.90), ("Colgate Luminous White 70g", 11.90),
        ("Listerine Whitening 250ml", 19.90), ("Escova Dental Colgate Slim Soft", 12.90),
        ("Creme Dental Closeup 90g", 5.90), ("Oral-B Escova Elétrica Pro 1000", 199.90),
        ("Enxaguante Bucal Cepacol 500ml", 14.90), ("Corega Fixador Dentadura 40g", 29.90),
        ("Colgate Periogard Enxaguante 250ml", 14.90), ("Elmex Creme Dental 90g", 16.90),
        ("Refil Escova Oral-B 4un", 39.90), ("Plac Control Revelador Placa 10ml", 12.90),
        ("Malvatricin Enxaguante 250ml", 12.90), ("Fio Dental Reach 50m", 8.90),
        ("Escova Interdental TePe", 19.90), ("Creme Dental Aquafresh 90g", 7.90),
    ],
    "Nutrição Esportiva": [
        ("Whey Protein Concentrado 900g", 89.90), ("Creatina Monohidratada 300g", 69.90),
        ("BCAA 2400 100 cápsulas", 39.90), ("Glutamina 300g", 59.90),
        ("Albumina 500g", 29.90), ("Barra de Proteína 40g", 6.90),
        ("Pré-Treino C4 300g", 129.90), ("Whey Isolado 900g", 149.90),
        ("Caseína 900g", 99.90), ("Maltodextrina 1kg", 24.90),
        ("ZMA 90 cápsulas", 39.90), ("Beta Alanina 120g", 49.90),
        ("Hipercalórico 3kg", 89.90), ("L-Carnitina 500ml", 49.90),
        ("Gel de Carboidrato 30g", 7.90), ("Multivitamínico Esportivo 60cp", 34.90),
        ("Isotônico em Pó 1kg", 29.90), ("Dextrose 1kg", 19.90),
        ("Óleo de Coco 200ml", 24.90), ("Pasta de Amendoim 500g", 19.90),
    ],
    "Alergias e Respiratório": [
        ("Loratadina 10mg 12cp", 9.90), ("Allegra 120mg 10cp", 29.90),
        ("Desloratadina 5mg 10cp", 14.90), ("Polaramine 2mg 20cp", 12.90),
        ("Soro Nasal Naridrin 30ml", 9.90), ("Busonid Spray Nasal 120 doses", 39.90),
        ("Avamys Spray Nasal 120 doses", 89.90), ("Prednisolona 3mg/ml 100ml", 14.90),
        ("Cetirizina 10mg 12cp", 8.90), ("Fexofenadina 180mg 10cp", 24.90),
        ("Rinosoro 0.9% 50ml", 7.90), ("Sorine Spray Nasal 50ml", 9.90),
        ("Clenil Spray 200 doses", 59.90), ("Aerolin Spray 200 doses", 24.90),
        ("Xarope de Guaco 120ml", 14.90), ("Fluimucil 600mg 16cp", 24.90),
        ("Bisolvon Xarope 120ml", 19.90), ("Ambroxol Xarope 120ml", 9.90),
        ("Pastilha Benalet 12un", 9.90), ("Spray Nasal Otrivina 15ml", 14.90),
    ],
    "Saúde Feminina": [
        ("Absorvente Always Noturno 8un", 9.90), ("Intimus Gel Absorvente 16un", 11.90),
        ("O.B. Absorvente Interno 10un", 12.90), ("Carefree Protetor Diário 40un", 14.90),
        ("Coletor Menstrual Fleurity", 69.90), ("Inciclo Disco Menstrual", 89.90),
        ("Anticoncepcionl Diane 35 21cp", 24.90), ("Calcinha Absorvente Herself", 49.90),
        ("Gel Íntimo Dermacyd 200ml", 24.90), ("Óvulo Vaginal Gino-Canesten 1un", 29.90),
        ("Teste de Gravidez Confirm", 14.90), ("Teste de Ovulação Clear Blue", 69.90),
        ("Lubrificante Íntimo KY 50g", 19.90), ("Sabonete Íntimo Lactic 200ml", 16.90),
        ("Absorvente Sempre Livre 16un", 8.90), ("Intimus Absorvente Noturno 8un", 10.90),
        ("Isoflavona 60 cápsulas", 34.90), ("Ácido Fólico Gestante 30cp", 12.90),
        ("Suplemento Gestante 30cp", 39.90), ("Lenço Umedecido Íntimo 16un", 9.90),
    ],
    "Oftalmológico": [
        ("Systane UL Colírio 15ml", 49.90), ("Refresh Tears Colírio 15ml", 39.90),
        ("Lacrifilm Colírio 15ml", 29.90), ("Optive Colírio 15ml", 44.90),
        ("Solução Multiuso Renu 355ml", 34.90), ("Líquido de Lentes Opti-Free 300ml", 39.90),
        ("Colírio Moura Brasil 20ml", 9.90), ("Colírio Lacrima Plus 15ml", 34.90),
        ("Estojo para Lentes de Contato", 9.90), ("Máscara para Olhos Gel", 14.90),
        ("Colírio Still 15ml", 29.90), ("Hylo Comod Colírio 10ml", 59.90),
        ("Solução Salina Biotrue 300ml", 34.90), ("Colírio Artelac 10ml", 39.90),
        ("Toalha Umedecida para Olhos 16un", 19.90),
    ],
    "Ortopedia e Dor Muscular": [
        ("Dorflex Gel 60g", 24.90), ("Cataflam Emulgel 60g", 34.90),
        ("Salompas Adesivo 10un", 12.90), ("Gelol Spray 100ml", 14.90),
        ("Tiger Balm Pomada 19g", 29.90), ("Voltaren Emulgel 60g", 39.90),
        ("Calminex Pomada 100g", 19.90), ("Biofreeze Gel 89ml", 49.90),
        ("Faixa Elástica Tensor", 14.90), ("Joelheira Ortopédica", 49.90),
        ("Tornozeleira Elástica", 29.90), ("Munhequeira Ortopédica", 24.90),
        ("Cinta Lombar", 59.90), ("Tipoia Ortopédica", 39.90),
        ("Palmilha Ortopédica", 34.90), ("Meias de Compressão 20-30mmHg", 79.90),
        ("Bengala Alumínio Regulável", 69.90), ("Bolsa de Água Quente 2L", 24.90),
        ("Cataflan Pro XT Spray 60ml", 39.90), ("Moment Gel 60g", 29.90),
    ],
    "Dermatologia Medicamentosa": [
        ("Cetoconazol Creme 30g", 14.90), ("Nistatina Creme Vaginal 60g", 12.90),
        ("Nebacetin Pomada 15g", 19.90), ("Trok-N Creme 30g", 29.90),
        ("Canesten Creme 20g", 24.90), ("Clotrimazol Creme 20g", 9.90),
        ("Bepantol Derma Creme 20g", 24.90), ("Dersani Loção 200ml", 29.90),
        ("Quadriderm Creme 20g", 24.90), ("Terbinafina Creme 20g", 19.90),
        ("Mupirocina Pomada 15g", 24.90), ("Ácido Fusídico Creme 15g", 34.90),
        ("Desonida Creme 30g", 14.90), ("Dexametasona Creme 10g", 8.90),
        ("Betametasona Creme 30g", 12.90), ("Adapaleno Gel 30g", 39.90),
        ("Peróxido de Benzoíla 5% 60g", 29.90), ("Tretinoína Creme 0.05% 30g", 24.90),
        ("Ácido Salicílico Loção 120ml", 19.90), ("Minoxidil 5% 60ml", 49.90),
    ],
    "Produtos Naturais e Fitoterápicos": [
        ("Chá de Camomila 10 sachês", 4.90), ("Própolis Spray 30ml", 14.90),
        ("Mel Puro 500g", 19.90), ("Chá Verde 30 sachês", 9.90),
        ("Óleo de Melaleuca 30ml", 24.90), ("Gengibre Cápsulas 60un", 14.90),
        ("Valeriana 60 cápsulas", 24.90), ("Passiflora 60 cápsulas", 19.90),
        ("Ginkgo Biloba 120mg 60cp", 29.90), ("Cúrcuma 60 cápsulas", 19.90),
        ("Alcachofra 60 cápsulas", 14.90), ("Castanha da Índia 60cp", 16.90),
        ("Cavalinha Cápsulas 60un", 12.90), ("Espinheira Santa 60cp", 14.90),
        ("Boldo do Chile 60cp", 9.90), ("Chá de Hortelã 10 sachês", 4.90),
        ("Chá Detox 20 sachês", 12.90), ("Chá de Hibisco 20 sachês", 9.90),
        ("Spray de Arnica 30ml", 14.90), ("Gel de Babosa 200ml", 16.90),
    ],
    "Aparelhos e Acessórios": [
        ("Termômetro Digital Infravermelho", 89.90), ("Oxímetro de Dedo", 79.90),
        ("Aparelho de Pressão Digital", 129.90), ("Nebulizador Ultrassônico", 149.90),
        ("Balança Digital Corporal", 69.90), ("Umidificador de Ar 3L", 99.90),
        ("Aparelho Glicemia Completo", 99.90), ("Massageador Elétrico", 79.90),
        ("Inalador Portátil", 119.90), ("Medidor de Temperatura Testa", 69.90),
        ("Luz de Emergência Hospitalar", 29.90), ("Aspirador Nasal Bebê", 19.90),
        ("Escova Elétrica Dental", 179.90), ("Tens Eletroestimulador", 89.90),
        ("Aparelho Auditivo Amplificador", 149.90),
    ],
}

for categoria, lista_produtos in categorias_produtos.items():
    for nome_prod, preco in lista_produtos:
        produtos.append((prod_id, nome_prod, categoria, round(preco, 2)))
        prod_id += 1

print(f"Total de produtos gerados: {len(produtos)}")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema_produtos = StructType([
    StructField("id_produto", IntegerType(), False),
    StructField("nome", StringType(), False),
    StructField("categoria", StringType(), False),
    StructField("preco_referencia", DoubleType(), False),
])

df_produtos = spark.createDataFrame(produtos, schema=schema_produtos)
df_produtos.write.mode("overwrite").saveAsTable(f"{catalog_name}.raw.produtos")
print(f"Tabela de produtos criada com {df_produtos.count()} registros!")
df_produtos.groupBy("categoria").count().orderBy("categoria").show(25, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação Final

# COMMAND ----------

print(f"\n{'='*60}")
print(f"DADOS CADASTRAIS GERADOS COM SUCESSO!")
print(f"{'='*60}")
print(f"\nCatálogo: {catalog_name}")
print(f"\nTabelas criadas:")
for tabela in ["clientes", "lojas", "produtos"]:
    count = spark.table(f"{catalog_name}.raw.{tabela}").count()
    print(f"  - {catalog_name}.raw.{tabela}: {count} registros")
print(f"\n{'='*60}")
print(f"Próximo passo: Execute o Lab 1 - SDP")
print(f"Abra o notebook '01a_gerador_vendas_streaming'")
print(f"{'='*60}")
