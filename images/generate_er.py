#!/usr/bin/env python3
"""Generate ER diagram for Workshop Panvel Databricks data model."""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

# ─── Configuration (25% larger) ─────────────────────────────────────────────

FIG_W, FIG_H = 72, 28
DPI = 150
TITLE = "Modelo de Dados — Workshop Panvel"
TEXT_COLOR = "#000000"

FONT_LAYER = 30
FONT_TABLE = 22
FONT_COL = 17
FONT_TAG = 15

# Layer definitions
LAYERS = [
    ("RAW", "#EAF2FB", "#2979FF", [
        ("clientes", [
            ("id_cliente", "PK"), ("nome", ""), ("cidade", ""), ("estado", ""),
        ]),
        ("lojas", [
            ("id_loja", "PK"), ("nome_loja", ""), ("cidade", ""), ("estado", ""),
        ]),
        ("produtos", [
            ("id_produto", "PK"), ("nome", ""), ("categoria", ""), ("preco_referencia", ""),
        ]),
    ]),
    ("BRONZE", "#FFF0E0", "#EF6C00", [
        ("bronze_vendas", [
            ("id_venda", "PK"), ("id_cliente", "FK"), ("id_loja", "FK"),
            ("data_venda", ""), ("valor_total", ""), ("itens[]", ""),
            ("arquivo_origem", ""), ("data_ingestao", ""),
        ]),
        ("bronze_clientes", [
            ("id_cliente", "PK"), ("nome", ""), ("cidade", ""), ("estado", ""),
        ]),
        ("bronze_lojas", [
            ("id_loja", "PK"), ("nome_loja", ""), ("cidade", ""), ("estado", ""),
        ]),
        ("bronze_produtos", [
            ("id_produto", "PK"), ("nome", ""), ("categoria", ""), ("preco_referencia", ""),
        ]),
    ]),
    ("SILVER", "#E8F5E9", "#388E3C", [
        ("silver_vendas", [
            ("id_venda", "PK"), ("data_venda", ""), ("ano", ""), ("mes", ""), ("dia", ""),
            ("id_cliente", "FK"), ("nome_cliente", ""), ("cidade_cliente", ""),
            ("id_loja", "FK"), ("nome_loja", ""), ("cidade_loja", ""),
            ("valor_total", ""), ("data_ingestao", ""),
        ]),
        ("silver_lojas", [
            ("id_loja", "PK"), ("nome_loja", ""), ("cidade", ""), ("bairro", ""),
        ]),
        ("silver_itens_venda", [
            ("id_venda", "FK"), ("id_produto", "FK"), ("id_loja", "FK"), ("id_cliente", "FK"),
            ("data_venda", ""), ("quantidade", ""), ("valor_unitario", ""),
            ("valor_total", ""), ("desconto", ""), ("nome_produto", ""), ("categoria", ""),
        ]),
    ]),
    ("GOLD", "#FFFDE7", "#F9A825", [
        ("gold_vendas_por_loja", [
            ("id_loja", ""), ("nome_loja", ""), ("cidade_loja", ""),
            ("total_vendas", ""), ("faturamento_total", ""), ("ticket_medio", ""), ("clientes_unicos", ""),
        ]),
        ("gold_vendas_por_categoria", [
            ("categoria", ""), ("total_itens_vendidos", ""), ("quantidade_total", ""),
            ("faturamento_total", ""), ("desconto_total", ""), ("preco_medio", ""),
        ]),
        ("gold_vendas_por_cidade", [
            ("cidade", ""), ("total_vendas", ""), ("faturamento_total", ""),
            ("ticket_medio", ""), ("num_lojas", ""), ("clientes_unicos", ""),
        ]),
        ("gold_top_produtos", [
            ("id_produto", ""), ("nome_produto", ""), ("categoria", ""),
            ("quantidade_total", ""), ("faturamento_total", ""), ("num_vendas", ""), ("desconto_medio", ""),
        ]),
    ]),
    ("AI/BI", "#E0F7FA", "#00838F", [
        ("Genie", [
            ("Linguagem natural", ""),
            ("Tabelas Gold", ""),
            ("Instruções customizadas", ""),
        ]),
        ("Dashboard", [
            ("Vendas por loja", ""),
            ("Vendas por categoria", ""),
            ("Vendas por cidade", ""),
            ("Top produtos", ""),
        ]),
    ]),
    ("ML", "#F3E5F5", "#8E24AA", [
        ("ml_segmentacao_clientes", [
            ("id_cliente", ""), ("recency", ""), ("frequency", ""), ("monetary", ""), ("segmento", ""),
        ]),
    ]),
]

# ─── Layout ──────────────────────────────────────────────────────────────────

TABLE_WIDTH = 5.0
ROW_HEIGHT = 0.53
HEADER_HEIGHT = 0.75
TABLE_GAP = 0.44
LAYER_PAD_X = 0.6
LAYER_PAD_Y = 0.75
LAYER_HEADER_H = 1.05
LAYER_GAP = 1.25


def table_height(cols):
    return HEADER_HEIGHT + len(cols) * ROW_HEIGHT + 0.25


STACKED_LAYERS = {"AI/BI", "ML"}  # these stack vertically after GOLD


def layout_layers():
    result = {}
    x = 1.2

    # First pass: layout the main flow layers (RAW→BRONZE→SILVER→GOLD)
    main_layers = [(n, b, bd, t) for n, b, bd, t in LAYERS if n not in STACKED_LAYERS]
    for layer_name, bg, border, tables in main_layers:
        heights = [table_height(cols) for _, cols in tables]
        total_h = sum(heights) + TABLE_GAP * (len(tables) - 1)
        lw = TABLE_WIDTH + 2 * LAYER_PAD_X
        lh = total_h + 2 * LAYER_PAD_Y + LAYER_HEADER_H

        ly = (FIG_H - lh) / 2 - 0.4

        layer = {
            "x": x, "y": ly, "w": lw, "h": lh,
            "bg": bg, "border": border, "name": layer_name, "tables": [],
        }

        ty = ly + lh - LAYER_HEADER_H - LAYER_PAD_Y
        tx = x + LAYER_PAD_X

        for (tname, cols), th in zip(tables, heights):
            ty_top = ty - th
            layer["tables"].append({
                "name": tname, "cols": cols,
                "x": tx, "y": ty_top, "w": TABLE_WIDTH, "h": th,
            })
            ty = ty_top - TABLE_GAP

        result[layer_name] = layer
        x += lw + LAYER_GAP

    # Second pass: stack AI/BI and ML vertically in one column after GOLD
    stacked = [(n, b, bd, t) for n, b, bd, t in LAYERS if n in STACKED_LAYERS]
    stacked_x = x
    lw = TABLE_WIDTH + 2 * LAYER_PAD_X

    # Compute heights for each stacked layer
    stacked_info = []
    for layer_name, bg, border, tables in stacked:
        heights = [table_height(cols) for _, cols in tables]
        total_h = sum(heights) + TABLE_GAP * (len(tables) - 1)
        lh = total_h + 2 * LAYER_PAD_Y + LAYER_HEADER_H
        stacked_info.append((layer_name, bg, border, tables, heights, lh))

    total_stacked_h = sum(s[5] for s in stacked_info) + LAYER_GAP
    start_y = (FIG_H - total_stacked_h) / 2 - 0.4

    # Place top to bottom: AI/BI on top, ML on bottom
    cy = start_y + total_stacked_h
    for layer_name, bg, border, tables, heights, lh in stacked_info:
        ly = cy - lh
        layer = {
            "x": stacked_x, "y": ly, "w": lw, "h": lh,
            "bg": bg, "border": border, "name": layer_name, "tables": [],
        }

        ty = ly + lh - LAYER_HEADER_H - LAYER_PAD_Y
        tx = stacked_x + LAYER_PAD_X

        for (tname, cols), th in zip(tables, heights):
            ty_top = ty - th
            layer["tables"].append({
                "name": tname, "cols": cols,
                "x": tx, "y": ty_top, "w": TABLE_WIDTH, "h": th,
            })
            ty = ty_top - TABLE_GAP

        result[layer_name] = layer
        cy = ly - LAYER_GAP

    return result


def draw_table(ax, tbl, border_color):
    x, y, w, h = tbl["x"], tbl["y"], tbl["w"], tbl["h"]

    box = FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0.10",
        facecolor="white", edgecolor=border_color, linewidth=2.5, zorder=3,
    )
    ax.add_patch(box)

    hdr = FancyBboxPatch(
        (x + 0.05, y + h - HEADER_HEIGHT - 0.04), w - 0.10, HEADER_HEIGHT,
        boxstyle="round,pad=0.05",
        facecolor=border_color, edgecolor="none", alpha=0.10, zorder=4,
    )
    ax.add_patch(hdr)

    ax.text(
        x + w / 2, y + h - HEADER_HEIGHT / 2 - 0.02,
        tbl["name"], ha="center", va="center",
        fontsize=FONT_TABLE, fontweight="bold", color="#000000",
        fontfamily="monospace", zorder=5,
    )

    sep_y = y + h - HEADER_HEIGHT - 0.07
    ax.plot([x + 0.15, x + w - 0.15], [sep_y, sep_y],
            color="#BDBDBD", linewidth=1.2, zorder=4)

    cy = sep_y - ROW_HEIGHT * 0.65
    for col_name, tag in tbl["cols"]:
        if tag == "PK":
            ax.text(x + 0.25, cy, "●", ha="left", va="center",
                    fontsize=FONT_TAG, color="#E65100", fontweight="bold", zorder=5)
            ax.text(x + 0.7, cy, col_name, ha="left", va="center",
                    fontsize=FONT_COL, color="#000000", fontweight="bold",
                    fontfamily="monospace", zorder=5)
            ax.text(x + w - 0.25, cy, "PK", ha="right", va="center",
                    fontsize=FONT_TAG, color="#E65100", fontweight="bold", zorder=5)
        elif tag == "FK":
            ax.text(x + 0.7, cy, col_name, ha="left", va="center",
                    fontsize=FONT_COL, color="#000000",
                    fontfamily="monospace", fontstyle="italic", zorder=5)
            ax.text(x + w - 0.25, cy, "FK", ha="right", va="center",
                    fontsize=FONT_TAG, color="#757575", zorder=5)
        else:
            ax.text(x + 0.7, cy, col_name, ha="left", va="center",
                    fontsize=FONT_COL, color="#000000",
                    fontfamily="monospace", zorder=5)
        cy -= ROW_HEIGHT


def draw_layer(ax, layer):
    x, y, w, h = layer["x"], layer["y"], layer["w"], layer["h"]
    bg, border = layer["bg"], layer["border"]

    bg_patch = FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0.18",
        facecolor=bg, edgecolor=border, linewidth=3.0, linestyle="--",
        alpha=0.7, zorder=1,
    )
    ax.add_patch(bg_patch)

    ax.text(
        x + w / 2, y + h - LAYER_HEADER_H / 2 + 0.10,
        layer["name"], ha="center", va="center",
        fontsize=FONT_LAYER, fontweight="bold", color=border, zorder=5,
        bbox=dict(boxstyle="round,pad=0.4", facecolor="white",
                  edgecolor=border, linewidth=2.5, alpha=0.95),
    )


def draw_arrows(ax, layout):
    # Straight flows: RAW→BRONZE→SILVER→GOLD
    straight = [("RAW", "BRONZE"), ("BRONZE", "SILVER"), ("SILVER", "GOLD")]
    for src_name, dst_name in straight:
        src, dst = layout[src_name], layout[dst_name]
        sx = src["x"] + src["w"]
        dx = dst["x"]
        my = (src["y"] + src["h"] / 2 + dst["y"] + dst["h"] / 2) / 2
        ax.annotate(
            "", xy=(dx - 0.06, my), xytext=(sx + 0.06, my),
            arrowprops=dict(arrowstyle="-|>", color="#424242", lw=3.5,
                            mutation_scale=30),
            zorder=2,
        )

    # GOLD fans out to AI/BI (upper) and ML (lower)
    gold = layout["GOLD"]
    gx = gold["x"] + gold["w"]
    g_mid_y = gold["y"] + gold["h"] / 2

    for dst_name in ["AI/BI", "ML"]:
        dst = layout[dst_name]
        dx = dst["x"]
        dy = dst["y"] + dst["h"] / 2
        ax.annotate(
            "", xy=(dx - 0.06, dy), xytext=(gx + 0.06, g_mid_y),
            arrowprops=dict(arrowstyle="-|>", color="#424242", lw=3.5,
                            mutation_scale=30, connectionstyle="arc3,rad=0.0"),
            zorder=2,
        )


def main():
    fig, ax = plt.subplots(1, 1, figsize=(FIG_W, FIG_H), dpi=DPI)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    ax.set_xlim(0, FIG_W)
    ax.set_ylim(0, FIG_H)
    ax.set_aspect("equal")
    ax.axis("off")

    ax.text(
        FIG_W / 2, FIG_H - 1.2, TITLE,
        ha="center", va="center",
        fontsize=38, fontweight="bold", color=TEXT_COLOR, zorder=10,
    )

    layout = layout_layers()

    for layer_name, _, _, _ in LAYERS:
        li = layout[layer_name]
        draw_layer(ax, li)
        for tbl in li["tables"]:
            draw_table(ax, tbl, li["border"])

    draw_arrows(ax, layout)

    out = "/Users/juliandro.figueiro/workshop-panvel/images/modelo_er.png"
    fig.savefig(out, dpi=DPI, bbox_inches="tight", facecolor="white", pad_inches=0.4)
    plt.close(fig)
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
