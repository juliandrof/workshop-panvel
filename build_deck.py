#!/usr/bin/env python3
"""Build the Workshop Panvel Google Slides deck."""

import json
import subprocess
import sys
import time

def get_token():
    result = subprocess.run(
        ["python3", "/Users/juliandro.figueiro/.claude/plugins/cache/fe-vibe/fe-google-tools/1.1.10/skills/google-auth/resources/google_auth.py", "token"],
        capture_output=True, text=True
    )
    return result.stdout.strip()

def api_call(method, url, data=None, token=None):
    import urllib.request
    headers = {
        "Authorization": f"Bearer {token}",
        "x-goog-user-project": "gcp-sandbox-field-eng",
        "Content-Type": "application/json"
    }
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"ERROR: {e}")
        if hasattr(e, 'read'):
            print(e.read().decode())
        sys.exit(1)

def batch_update(pres_id, requests, token):
    url = f"https://slides.googleapis.com/v1/presentations/{pres_id}:batchUpdate"
    return api_call("POST", url, {"requests": requests}, token)

# EMU helpers
def inches(val):
    return int(val * 914400)

def pt(val):
    return int(val * 12700)

def rgb(r, g, b):
    return {"rgbColor": {"red": r, "green": g, "blue": b}}

# Colors
NAVY = rgb(0.071, 0.165, 0.271)
WHITE = rgb(1, 1, 1)
RED = rgb(1, 0.224, 0.161)
ORANGE = rgb(1, 0.439, 0.204)
DARK_TEXT = rgb(0.15, 0.15, 0.15)
GRAY_TEXT = rgb(0.5, 0.5, 0.5)
LIGHT_BG = rgb(0.965, 0.965, 0.965)

def create_textbox(page_id, obj_id, text, x, y, w, h, font_size=14, bold=False, color=None, alignment="START"):
    """Create a text box with text and styling."""
    color = color or DARK_TEXT
    reqs = [
        {
            "createShape": {
                "objectId": obj_id,
                "shapeType": "TEXT_BOX",
                "elementProperties": {
                    "pageObjectId": page_id,
                    "size": {
                        "width": {"magnitude": inches(w), "unit": "EMU"},
                        "height": {"magnitude": inches(h), "unit": "EMU"}
                    },
                    "transform": {
                        "scaleX": 1, "scaleY": 1,
                        "translateX": inches(x), "translateY": inches(y),
                        "unit": "EMU"
                    }
                }
            }
        },
        {
            "insertText": {
                "objectId": obj_id,
                "text": text,
                "insertionIndex": 0
            }
        },
        {
            "updateTextStyle": {
                "objectId": obj_id,
                "textRange": {"type": "ALL"},
                "style": {
                    "bold": bold,
                    "fontSize": {"magnitude": font_size, "unit": "PT"},
                    "fontFamily": "Inter",
                    "foregroundColor": {"opaqueColor": color}
                },
                "fields": "bold,fontSize,fontFamily,foregroundColor"
            }
        },
        {
            "updateParagraphStyle": {
                "objectId": obj_id,
                "textRange": {"type": "ALL"},
                "style": {
                    "alignment": alignment,
                    "lineSpacing": 140,
                    "spaceBelow": {"magnitude": 4, "unit": "PT"}
                },
                "fields": "alignment,lineSpacing,spaceBelow"
            }
        }
    ]
    return reqs

def create_rect(page_id, obj_id, x, y, w, h, fill_color):
    """Create a colored rectangle."""
    return [
        {
            "createShape": {
                "objectId": obj_id,
                "shapeType": "RECTANGLE",
                "elementProperties": {
                    "pageObjectId": page_id,
                    "size": {
                        "width": {"magnitude": inches(w), "unit": "EMU"},
                        "height": {"magnitude": inches(h), "unit": "EMU"}
                    },
                    "transform": {
                        "scaleX": 1, "scaleY": 1,
                        "translateX": inches(x), "translateY": inches(y),
                        "unit": "EMU"
                    }
                }
            }
        },
        {
            "updateShapeProperties": {
                "objectId": obj_id,
                "shapeProperties": {
                    "shapeBackgroundFill": {
                        "solidFill": {"color": fill_color}
                    },
                    "outline": {"propertyState": "NOT_RENDERED"}
                },
                "fields": "shapeBackgroundFill,outline"
            }
        }
    ]

def set_bg(page_id, color):
    return {
        "updatePageProperties": {
            "objectId": page_id,
            "pageProperties": {
                "pageBackgroundFill": {
                    "solidFill": {"color": color}
                }
            },
            "fields": "pageBackgroundFill"
        }
    }

def main():
    token = get_token()
    print("Token obtained.")

    # 1. Create presentation
    pres = api_call("POST", "https://slides.googleapis.com/v1/presentations",
                    {"title": "Workshop Hands-On Databricks - Panvel"}, token)
    pres_id = pres["presentationId"]
    print(f"Created presentation: {pres_id}")
    print(f"URL: https://docs.google.com/presentation/d/{pres_id}/edit")

    # 2. Create all slides
    slide_ids = [
        "sl_title",       # 0 - will replace default "p"
        "sl_agenda",      # 1
        "sl_setup",       # 2
        "sl_arch",        # 3
        "sl_sec1",        # 4
        "sl_lab1",        # 5
        "sl_lab1h",       # 6
        "sl_sec2",        # 7
        "sl_lab2",        # 8
        "sl_lab2h",       # 9
        "sl_sec3",        # 10
        "sl_lab3",        # 11
        "sl_lab3h",       # 12
        "sl_sec4",        # 13
        "sl_lab4",        # 14
        "sl_lab4h",       # 15
        "sl_recap",       # 16
        "sl_closing",     # 17
    ]

    create_reqs = []
    for i, sid in enumerate(slide_ids[1:], 1):  # skip first (use default)
        create_reqs.append({
            "createSlide": {
                "objectId": sid,
                "insertionIndex": i,
                "slideLayoutReference": {"predefinedLayout": "BLANK"}
            }
        })

    batch_update(pres_id, create_reqs, token)
    print(f"Created {len(create_reqs)} slides")

    # Get default slide ID
    pres_info = api_call("GET", f"https://slides.googleapis.com/v1/presentations/{pres_id}?fields=slides(objectId)", token=token)
    default_slide = pres_info["slides"][0]["objectId"]

    # 3. Set backgrounds
    dark_slides = [default_slide, "sl_sec1", "sl_sec2", "sl_sec3", "sl_sec4", "sl_closing"]
    light_slides = ["sl_agenda", "sl_setup", "sl_arch", "sl_lab1", "sl_lab1h",
                    "sl_lab2", "sl_lab2h", "sl_lab3", "sl_lab3h",
                    "sl_lab4", "sl_lab4h", "sl_recap"]

    bg_reqs = [set_bg(s, NAVY) for s in dark_slides]
    bg_reqs += [set_bg(s, LIGHT_BG) for s in light_slides]
    batch_update(pres_id, bg_reqs, token)
    print("Backgrounds set")

    # 4. Build all content
    # We need to batch carefully - API has limits per request
    # Build content in chunks

    # ============ SLIDE 0: TITLE ============
    reqs = []
    # Red accent line at top
    reqs += create_rect(default_slide, "t_bar", 0, 0, 10, 0.06, RED)
    # Title
    reqs += create_textbox(default_slide, "t_title",
        "Workshop Hands-On\nDatabricks",
        0.8, 1.0, 8.4, 1.4, font_size=36, bold=True, color=WHITE)
    # Subtitle
    reqs += create_textbox(default_slide, "t_sub",
        "Grupo Panvel",
        0.8, 2.6, 8.4, 0.6, font_size=24, color=ORANGE)
    # Bottom text
    reqs += create_textbox(default_slide, "t_bottom",
        "Data & AI na prática  |  SDP  |  Jobs  |  ML  |  AI/BI",
        0.8, 4.5, 8.4, 0.4, font_size=12, color=GRAY_TEXT)

    batch_update(pres_id, reqs, token)
    print("Slide 0: Title - done")

    # ============ SLIDE 1: AGENDA ============
    reqs = []
    reqs += create_rect("sl_agenda", "a_bar", 0, 0, 10, 0.06, RED)
    reqs += create_textbox("sl_agenda", "a_title",
        "Agenda",
        0.5, 0.25, 9, 0.55, font_size=28, bold=True, color=NAVY)
    reqs += create_rect("sl_agenda", "a_line", 0.5, 0.85, 1.5, 0.04, RED)

    agenda_text = (
        "Setup Inicial\n"
        "Configuração do catálogo personalizado e geração de dados sintéticos\n\n"
        "Lab 1 — Serverless Delta Pipeline (SDP)\n"
        "Ingestão streaming com Auto Loader e arquitetura Medallion (Bronze/Silver/Gold)\n\n"
        "Lab 2 — Workflows / Jobs\n"
        "Orquestração multi-tarefa com dependências e agendamento\n\n"
        "Lab 3 — Machine Learning\n"
        "Segmentação de clientes com RFM + K-Means + MLflow\n\n"
        "Lab 4 — AI/BI Genie + Dashboard\n"
        "Genie com linguagem natural e AI/BI Dashboard interativo"
    )
    reqs += create_textbox("sl_agenda", "a_body", agenda_text,
        0.5, 1.1, 9, 4.0, font_size=13, color=DARK_TEXT)

    batch_update(pres_id, reqs, token)
    print("Slide 1: Agenda - done")

    # ============ SLIDE 2: SETUP ============
    reqs = []
    reqs += create_rect("sl_setup", "s_bar", 0, 0, 10, 0.06, RED)
    reqs += create_textbox("sl_setup", "s_title",
        "Setup Inicial",
        0.5, 0.25, 9, 0.55, font_size=28, bold=True, color=NAVY)
    reqs += create_rect("sl_setup", "s_line", 0.5, 0.85, 1.5, 0.04, RED)

    setup_text = (
        "1. Preencha o widget nome_participante com seu primeiro nome\n"
        "    (sem espaços, sem acentos, minúsculo)\n\n"
        "2. Execute o notebook  00_configuracao_catalogo.py\n"
        "    Será criado o catálogo:  workshop_panvel_<seu_nome>\n"
        "    Com schemas: raw, bronze, silver, gold, ml\n\n"
        "3. Execute o notebook  01_dados_cadastrais.py\n"
        "    Serão gerados:\n"
        "    • 1.000 clientes em 50 cidades do Rio Grande do Sul\n"
        "    • ~120 lojas no formato \"Panvel - Bairro\"\n"
        "    • 400 produtos em 20 categorias de farmácia\n\n"
        "4. Verifique no Catalog Explorer se tudo foi criado"
    )
    reqs += create_textbox("sl_setup", "s_body", setup_text,
        0.5, 1.1, 9, 4.0, font_size=13, color=DARK_TEXT)

    batch_update(pres_id, reqs, token)
    print("Slide 2: Setup - done")

    # ============ SLIDE 3: ARCHITECTURE ============
    reqs = []
    reqs += create_rect("sl_arch", "ar_bar", 0, 0, 10, 0.06, RED)
    reqs += create_textbox("sl_arch", "ar_title",
        "Arquitetura Medallion",
        0.5, 0.25, 9, 0.55, font_size=28, bold=True, color=NAVY)
    reqs += create_rect("sl_arch", "ar_line", 0.5, 0.85, 1.5, 0.04, RED)

    # Flow arrow
    reqs += create_textbox("sl_arch", "ar_flow",
        "JSON Vendas  →  Auto Loader  →  Bronze  →  Silver  →  Gold  →  AI/BI",
        0.5, 1.1, 9, 0.4, font_size=14, bold=True, color=RED)

    # Bronze card
    reqs += create_rect("sl_arch", "ar_c1bg", 0.4, 1.8, 2.8, 3.0, rgb(0.92, 0.78, 0.65))
    reqs += create_textbox("sl_arch", "ar_c1t",
        "BRONZE",
        0.5, 1.9, 2.6, 0.4, font_size=16, bold=True, color=NAVY)
    reqs += create_textbox("sl_arch", "ar_c1b",
        "Dados brutos\n\n• Vendas (streaming JSON)\n• Cadastro de clientes\n• Cadastro de lojas\n• Cadastro de produtos",
        0.5, 2.35, 2.6, 2.3, font_size=11, color=DARK_TEXT)

    # Silver card
    reqs += create_rect("sl_arch", "ar_c2bg", 3.6, 1.8, 2.8, 3.0, rgb(0.78, 0.78, 0.78))
    reqs += create_textbox("sl_arch", "ar_c2t",
        "SILVER",
        3.7, 1.9, 2.6, 0.4, font_size=16, bold=True, color=NAVY)
    reqs += create_textbox("sl_arch", "ar_c2b",
        "Dados limpos\n\n• Vendas enriquecidas\n• Lojas com coluna bairro\n• Itens explodidos\n• Quality expectations",
        3.7, 2.35, 2.6, 2.3, font_size=11, color=DARK_TEXT)

    # Gold card
    reqs += create_rect("sl_arch", "ar_c3bg", 6.8, 1.8, 2.8, 3.0, rgb(0.98, 0.85, 0.45))
    reqs += create_textbox("sl_arch", "ar_c3t",
        "GOLD",
        6.9, 1.9, 2.6, 0.4, font_size=16, bold=True, color=NAVY)
    reqs += create_textbox("sl_arch", "ar_c3b",
        "Dados agregados\n\n• Vendas por loja\n• Vendas por categoria\n• Vendas por cidade\n• Top produtos",
        6.9, 2.35, 2.6, 2.3, font_size=11, color=DARK_TEXT)

    batch_update(pres_id, reqs, token)
    print("Slide 3: Architecture - done")

    # ============ SECTION BREAKS ============
    sections = [
        ("sl_sec1", "Lab 1", "Serverless Delta Pipeline (SDP)"),
        ("sl_sec2", "Lab 2", "Workflows / Jobs"),
        ("sl_sec3", "Lab 3", "Machine Learning"),
        ("sl_sec4", "Lab 4", "AI/BI — Genie + Dashboard"),
    ]

    for i, (sid, label, title) in enumerate(sections):
        reqs = []
        reqs += create_rect(sid, f"sc{i}_bar", 0, 0, 10, 0.06, ORANGE)
        reqs += create_rect(sid, f"sc{i}_accent", 0.8, 2.0, 0.08, 1.8, ORANGE)
        reqs += create_textbox(sid, f"sc{i}_label", label,
            1.1, 2.0, 8, 0.5, font_size=18, color=ORANGE)
        reqs += create_textbox(sid, f"sc{i}_title", title,
            1.1, 2.5, 8, 0.9, font_size=32, bold=True, color=WHITE)
        batch_update(pres_id, reqs, token)

    print("Section breaks - done")

    # ============ LAB 1 DETAIL ============
    reqs = []
    reqs += create_rect("sl_lab1", "l1_bar", 0, 0, 10, 0.06, RED)
    reqs += create_textbox("sl_lab1", "l1_title",
        "Lab 1: Serverless Delta Pipeline",
        0.5, 0.25, 9, 0.55, font_size=24, bold=True, color=NAVY)
    reqs += create_rect("sl_lab1", "l1_line", 0.5, 0.85, 1.5, 0.04, RED)

    reqs += create_textbox("sl_lab1", "l1_left_t",
        "O que vamos fazer",
        0.5, 1.1, 4.2, 0.35, font_size=14, bold=True, color=NAVY)
    reqs += create_textbox("sl_lab1", "l1_left",
        "• Ingerir arquivos JSON de vendas via Auto Loader\n"
        "• Criar pipeline DLT com 3 camadas\n"
        "• Aplicar quality checks com expectations\n"
        "• Extrair bairro do nome da loja (regexp)\n"
        "• Explodir itens aninhados com explode()\n"
        "• Criar tabelas gold agregadas",
        0.5, 1.5, 4.2, 2.5, font_size=12, color=DARK_TEXT)

    reqs += create_textbox("sl_lab1", "l1_right_t",
        "Conceitos",
        5.3, 1.1, 4.2, 0.35, font_size=14, bold=True, color=NAVY)
    reqs += create_textbox("sl_lab1", "l1_right",
        "• Auto Loader / CloudFiles\n"
        "• Delta Live Tables (DLT)\n"
        "• Streaming Tables\n"
        "• Materialized Views\n"
        "• Data Quality Expectations\n"
        "• Medallion Architecture",
        5.3, 1.5, 4.2, 2.5, font_size=12, color=DARK_TEXT)

    # Notebooks reference
    reqs += create_rect("sl_lab1", "l1_nbg", 0.5, 4.1, 9, 1.0, rgb(0.93, 0.93, 0.93))
    reqs += create_textbox("sl_lab1", "l1_nb",
        "Notebooks:  01a_gerador_vendas_streaming.py  |  01c_sdp_pipeline_todo.py  (ref: 01b_...completo.py)",
        0.7, 4.25, 8.6, 0.6, font_size=11, color=GRAY_TEXT)

    batch_update(pres_id, reqs, token)
    print("Lab 1 detail - done")

    # ============ LAB 1 HANDS-ON ============
    reqs = []
    reqs += create_rect("sl_lab1h", "l1h_bar", 0, 0, 10, 0.06, RED)
    reqs += create_textbox("sl_lab1h", "l1h_title",
        "Lab 1: Hands-On",
        0.5, 0.25, 9, 0.55, font_size=24, bold=True, color=NAVY)
    reqs += create_rect("sl_lab1h", "l1h_line", 0.5, 0.85, 1.5, 0.04, RED)

    handson1 = (
        "Passo 1 — Inicie o gerador de vendas\n"
        "Execute 01a_gerador_vendas_streaming.py e deixe rodando!\n"
        "Ele gera 1 JSON por loja a cada 20 segundos\n\n"
        "Passo 2 — Complete os TO-DOs no pipeline\n"
        "Abra 01c_sdp_pipeline_todo.py e complete:\n"
        "  TODO 1: Criar tabela bronze_produtos\n"
        "  TODO 2: Extrair ano, mês e dia da data_venda\n"
        "  TODO 3: Extrair bairro do nome da loja (regexp)\n"
        "  TODO 4: Usar explode() para explodir itens\n"
        "  TODO 5: Completar agregações por categoria\n"
        "  TODO 6: Criar tabela gold_vendas_por_cidade\n\n"
        "Passo 3 — Configure o pipeline DLT\n"
        "Workflows > Delta Live Tables > Create Pipeline\n"
        "Target catalog: workshop_panvel_<seu_nome>"
    )
    reqs += create_textbox("sl_lab1h", "l1h_body", handson1,
        0.5, 1.1, 9, 4.0, font_size=12, color=DARK_TEXT)

    batch_update(pres_id, reqs, token)
    print("Lab 1 hands-on - done")

    # ============ LAB 2 DETAIL ============
    reqs = []
    reqs += create_rect("sl_lab2", "l2_bar", 0, 0, 10, 0.06, RED)
    reqs += create_textbox("sl_lab2", "l2_title",
        "Lab 2: Workflows / Jobs",
        0.5, 0.25, 9, 0.55, font_size=24, bold=True, color=NAVY)
    reqs += create_rect("sl_lab2", "l2_line", 0.5, 0.85, 1.5, 0.04, RED)

    reqs += create_textbox("sl_lab2", "l2_left_t",
        "O que vamos fazer",
        0.5, 1.1, 4.2, 0.35, font_size=14, bold=True, color=NAVY)
    reqs += create_textbox("sl_lab2", "l2_left",
        "• Criar um Workflow multi-tarefa\n"
        "• Configurar dependências (DAG)\n"
        "• Validação → Pipeline → Qualidade → Resumo\n"
        "• Configurar agendamento\n"
        "• Monitorar execução",
        0.5, 1.5, 4.2, 2.5, font_size=12, color=DARK_TEXT)

    reqs += create_textbox("sl_lab2", "l2_right_t",
        "Arquitetura do Workflow",
        5.3, 1.1, 4.2, 0.35, font_size=14, bold=True, color=NAVY)
    reqs += create_textbox("sl_lab2", "l2_right",
        "Validação Dados\n       ↓\nPipeline DLT (SDP)\n       ↓\nVerificação Qualidade\n       ↓\nNotificação / Resumo",
        5.3, 1.5, 4.2, 2.5, font_size=13, color=DARK_TEXT, alignment="CENTER")

    reqs += create_rect("sl_lab2", "l2_nbg", 0.5, 4.1, 9, 1.0, rgb(0.93, 0.93, 0.93))
    reqs += create_textbox("sl_lab2", "l2_nb",
        "Notebooks:  02b_workflow_todo.py  (ref: 02a_workflow_completo.py)",
        0.7, 4.25, 8.6, 0.6, font_size=11, color=GRAY_TEXT)

    batch_update(pres_id, reqs, token)
    print("Lab 2 detail - done")

    # ============ LAB 2 HANDS-ON ============
    reqs = []
    reqs += create_rect("sl_lab2h", "l2h_bar", 0, 0, 10, 0.06, RED)
    reqs += create_textbox("sl_lab2h", "l2h_title",
        "Lab 2: Hands-On",
        0.5, 0.25, 9, 0.55, font_size=24, bold=True, color=NAVY)
    reqs += create_rect("sl_lab2h", "l2h_line", 0.5, 0.85, 1.5, 0.04, RED)

    handson2 = (
        "Passo 1 — Complete os TO-DOs\n"
        "Abra 02b_workflow_todo.py e complete:\n"
        "  TODO 1: Validar quantidade mínima de registros\n"
        "  TODO 2: Verificar coluna bairro em silver_lojas\n"
        "  TODO 3: Calcular total de vendas e faturamento\n\n"
        "Passo 2 — Crie o Workflow no Databricks\n"
        "Workflows > Create Job\n"
        "Nome: workflow_panvel_<seu_nome>\n\n"
        "Configure 4 tarefas com dependências:\n"
        "  Tarefa 1: Validação (Notebook) → sem dependência\n"
        "  Tarefa 2: Pipeline DLT → depende da Tarefa 1\n"
        "  Tarefa 3: Qualidade → depende da Tarefa 2\n"
        "  Tarefa 4: Resumo → depende da Tarefa 3\n\n"
        "Passo 3 — Configure agendamento (a cada 30 min)"
    )
    reqs += create_textbox("sl_lab2h", "l2h_body", handson2,
        0.5, 1.1, 9, 4.0, font_size=12, color=DARK_TEXT)

    batch_update(pres_id, reqs, token)
    print("Lab 2 hands-on - done")

    # ============ LAB 3 DETAIL ============
    reqs = []
    reqs += create_rect("sl_lab3", "l3_bar", 0, 0, 10, 0.06, RED)
    reqs += create_textbox("sl_lab3", "l3_title",
        "Lab 3: Machine Learning",
        0.5, 0.25, 9, 0.55, font_size=24, bold=True, color=NAVY)
    reqs += create_rect("sl_lab3", "l3_line", 0.5, 0.85, 1.5, 0.04, RED)

    reqs += create_textbox("sl_lab3", "l3_left_t",
        "Segmentação de Clientes — RFM",
        0.5, 1.1, 4.2, 0.35, font_size=14, bold=True, color=NAVY)
    reqs += create_textbox("sl_lab3", "l3_left",
        "Recency (R)\n"
        "Há quantos dias o cliente fez a última compra?\n\n"
        "Frequency (F)\n"
        "Quantas compras o cliente fez?\n\n"
        "Monetary (M)\n"
        "Quanto o cliente gastou no total?",
        0.5, 1.5, 4.2, 2.5, font_size=12, color=DARK_TEXT)

    reqs += create_textbox("sl_lab3", "l3_right_t",
        "Conceitos",
        5.3, 1.1, 4.2, 0.35, font_size=14, bold=True, color=NAVY)
    reqs += create_textbox("sl_lab3", "l3_right",
        "• Feature Engineering com PySpark\n"
        "• K-Means Clustering\n"
        "• StandardScaler para normalização\n"
        "• Silhouette Score para avaliação\n"
        "• MLflow Tracking\n"
        "• Model Registry no Unity Catalog",
        5.3, 1.5, 4.2, 2.5, font_size=12, color=DARK_TEXT)

    reqs += create_rect("sl_lab3", "l3_nbg", 0.5, 4.1, 9, 1.0, rgb(0.93, 0.93, 0.93))
    reqs += create_textbox("sl_lab3", "l3_nb",
        "Notebooks:  03b_ml_todo.py  (ref: 03a_ml_completo.py)",
        0.7, 4.25, 8.6, 0.6, font_size=11, color=GRAY_TEXT)

    batch_update(pres_id, reqs, token)
    print("Lab 3 detail - done")

    # ============ LAB 3 HANDS-ON ============
    reqs = []
    reqs += create_rect("sl_lab3h", "l3h_bar", 0, 0, 10, 0.06, RED)
    reqs += create_textbox("sl_lab3h", "l3h_title",
        "Lab 3: Hands-On",
        0.5, 0.25, 9, 0.55, font_size=24, bold=True, color=NAVY)
    reqs += create_rect("sl_lab3h", "l3h_line", 0.5, 0.85, 1.5, 0.04, RED)

    handson3 = (
        "Passo 1 — Complete os TO-DOs\n"
        "Abra 03b_ml_todo.py e complete:\n"
        "  TODO 1: Calcular métricas RFM por cliente\n"
        "  TODO 2: Montar VectorAssembler\n"
        "  TODO 3: Treinar K-Means (K=3,4,5,6) com MLflow\n"
        "  TODO 4: Analisar perfil dos segmentos\n"
        "  TODO 5: Salvar tabela de segmentação\n\n"
        "Passo 2 — Acompanhe no MLflow\n"
        "Experiments > workshop_panvel_<nome>_rfm\n"
        "Compare os runs com diferentes valores de K\n"
        "O melhor K é escolhido pelo Silhouette Score\n\n"
        "Passo 3 — Verifique o modelo registrado\n"
        "Unity Catalog > Models > modelo_segmentacao_clientes"
    )
    reqs += create_textbox("sl_lab3h", "l3h_body", handson3,
        0.5, 1.1, 9, 4.0, font_size=12, color=DARK_TEXT)

    batch_update(pres_id, reqs, token)
    print("Lab 3 hands-on - done")

    # ============ LAB 4 DETAIL ============
    reqs = []
    reqs += create_rect("sl_lab4", "l4_bar", 0, 0, 10, 0.06, RED)
    reqs += create_textbox("sl_lab4", "l4_title",
        "Lab 4: AI/BI — Genie + Dashboard",
        0.5, 0.25, 9, 0.55, font_size=24, bold=True, color=NAVY)
    reqs += create_rect("sl_lab4", "l4_line", 0.5, 0.85, 1.5, 0.04, RED)

    reqs += create_textbox("sl_lab4", "l4_left_t",
        "Genie",
        0.5, 1.1, 4.2, 0.35, font_size=14, bold=True, color=NAVY)
    reqs += create_textbox("sl_lab4", "l4_left",
        "• Perguntas em linguagem natural\n"
        "• Conectado às tabelas Gold e Views\n"
        "• Instruções customizadas para contexto\n"
        "• Exemplos de perguntas:\n"
        "  \"Qual loja fatura mais?\"\n"
        "  \"Top 5 produtos mais vendidos\"",
        0.5, 1.5, 4.2, 2.5, font_size=12, color=DARK_TEXT)

    reqs += create_textbox("sl_lab4", "l4_right_t",
        "AI/BI Dashboard",
        5.3, 1.1, 4.2, 0.35, font_size=14, bold=True, color=NAVY)
    reqs += create_textbox("sl_lab4", "l4_right",
        "• KPIs: vendas, faturamento, ticket médio\n"
        "• Bar Chart: faturamento por cidade\n"
        "• Pie Chart: vendas por categoria\n"
        "• Tabela: top lojas e produtos\n"
        "• Queries SQL prontas no notebook",
        5.3, 1.5, 4.2, 2.5, font_size=12, color=DARK_TEXT)

    reqs += create_rect("sl_lab4", "l4_nbg", 0.5, 4.1, 9, 1.0, rgb(0.93, 0.93, 0.93))
    reqs += create_textbox("sl_lab4", "l4_nb",
        "Notebooks:  04b_genie_dashboard_todo.py  (ref: 04a_...completo.py)",
        0.7, 4.25, 8.6, 0.6, font_size=11, color=GRAY_TEXT)

    batch_update(pres_id, reqs, token)
    print("Lab 4 detail - done")

    # ============ LAB 4 HANDS-ON ============
    reqs = []
    reqs += create_rect("sl_lab4h", "l4h_bar", 0, 0, 10, 0.06, RED)
    reqs += create_textbox("sl_lab4h", "l4h_title",
        "Lab 4: Hands-On",
        0.5, 0.25, 9, 0.55, font_size=24, bold=True, color=NAVY)
    reqs += create_rect("sl_lab4h", "l4h_line", 0.5, 0.85, 1.5, 0.04, RED)

    handson4 = (
        "Passo 1 — Complete os TO-DOs\n"
        "Abra 04b_genie_dashboard_todo.py e complete:\n"
        "  TODO 1: Criar view de vendas com produtos\n"
        "  TODO 2: Adicionar comentários às tabelas\n"
        "  TODO 4: Escrever instruções do Genie\n\n"
        "Passo 2 — Crie o Genie\n"
        "AI/BI > Genie > New Genie\n"
        "Nome: Análise Panvel - <seu_nome>\n"
        "Adicione as tabelas gold e views listadas no notebook\n"
        "Cole as instruções customizadas\n\n"
        "Passo 3 — Crie o AI/BI Dashboard\n"
        "AI/BI > Dashboards > Create Dashboard\n"
        "Use as queries SQL prontas no notebook\n\n"
        "Passo 4 — Teste o Genie com perguntas!"
    )
    reqs += create_textbox("sl_lab4h", "l4h_body", handson4,
        0.5, 1.1, 9, 4.0, font_size=12, color=DARK_TEXT)

    batch_update(pres_id, reqs, token)
    print("Lab 4 hands-on - done")

    # ============ SLIDE RECAP ============
    reqs = []
    reqs += create_rect("sl_recap", "rc_bar", 0, 0, 10, 0.06, RED)
    reqs += create_textbox("sl_recap", "rc_title",
        "Resumo e Próximos Passos",
        0.5, 0.25, 9, 0.55, font_size=24, bold=True, color=NAVY)
    reqs += create_rect("sl_recap", "rc_line", 0.5, 0.85, 1.5, 0.04, RED)

    reqs += create_textbox("sl_recap", "rc_left_t",
        "O que aprendemos",
        0.5, 1.1, 4.2, 0.35, font_size=14, bold=True, color=NAVY)
    reqs += create_textbox("sl_recap", "rc_left",
        "Lab 1 — SDP\n"
        "Ingestão streaming + Medallion Architecture\n\n"
        "Lab 2 — Jobs\n"
        "Orquestração de pipelines com dependências\n\n"
        "Lab 3 — ML\n"
        "Segmentação de clientes com MLflow\n\n"
        "Lab 4 — AI/BI\n"
        "Análise com linguagem natural + dashboards",
        0.5, 1.5, 4.2, 3.0, font_size=12, color=DARK_TEXT)

    reqs += create_textbox("sl_recap", "rc_right_t",
        "Próximos passos",
        5.3, 1.1, 4.2, 0.35, font_size=14, bold=True, color=NAVY)
    reqs += create_textbox("sl_recap", "rc_right",
        "• Explorar mais funcionalidades do Databricks\n"
        "• Aplicar os conceitos nos dados reais\n"
        "• Unity Catalog para governança\n"
        "• Databricks Assistant para produtividade\n"
        "• Lakeflow Connect para ingestão\n"
        "• Serverless compute para economia",
        5.3, 1.5, 4.2, 3.0, font_size=12, color=DARK_TEXT)

    batch_update(pres_id, reqs, token)
    print("Recap - done")

    # ============ CLOSING ============
    reqs = []
    reqs += create_rect("sl_closing", "cl_bar", 0, 0, 10, 0.06, ORANGE)
    reqs += create_textbox("sl_closing", "cl_title",
        "Obrigado!",
        0.8, 1.5, 8.4, 1.0, font_size=40, bold=True, color=WHITE, alignment="CENTER")
    reqs += create_textbox("sl_closing", "cl_sub",
        "Workshop Hands-On Databricks — Panvel",
        0.8, 2.6, 8.4, 0.6, font_size=20, color=ORANGE, alignment="CENTER")
    reqs += create_textbox("sl_closing", "cl_bottom",
        "Dúvidas? Entre em contato com seu time Databricks!",
        0.8, 4.3, 8.4, 0.4, font_size=13, color=GRAY_TEXT, alignment="CENTER")

    batch_update(pres_id, reqs, token)
    print("Closing - done")

    print(f"\n{'='*60}")
    print(f"DECK CRIADO COM SUCESSO!")
    print(f"{'='*60}")
    print(f"URL: https://docs.google.com/presentation/d/{pres_id}/edit")
    print(f"Total: 18 slides")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
