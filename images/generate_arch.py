#!/usr/bin/env python3
"""Generate architecture diagram for Workshop Panvel."""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.patheffects as pe

FIG_W, FIG_H = 48, 18
DPI = 150

# ─── Stage definitions ───────────────────────────────────────────────────────
# (name, subtitle, bg_color, border_color, features)

STAGES = [
    ("JSON Vendas\n(Streaming)", None, "#E3F2FD", "#1565C0", None),  # source
    ("BRONZE", "Ingestão", "#FFF0E0", "#EF6C00", [
        "Auto Loader",
        "CloudFiles (JSON)",
        "Streaming Tables",
        "Schema Enforcement",
        "Dados Brutos",
    ]),
    ("SILVER", "Transformação", "#E8F5E9", "#388E3C", [
        "Data Quality (Expectations)",
        "Joins & Enriquecimento",
        "Extração de Bairro",
        "Explode (itens → linhas)",
        "Limpeza & Tipagem",
    ]),
    ("GOLD", "Agregação", "#FFFDE7", "#F9A825", [
        "Materialized Views",
        "Vendas por Loja",
        "Vendas por Categoria",
        "Vendas por Cidade",
        "Top Produtos",
    ]),
]

# Fan-out targets from GOLD
FANOUT = [
    ("AI/BI", "#E0F7FA", "#00838F", [
        "Genie (Linguagem Natural)",
        "AI/BI Dashboard",
        "Visualizações Interativas",
    ]),
    ("ML", "#F3E5F5", "#8E24AA", [
        "RFM + K-Means",
        "MLflow Tracking",
        "Model Registry (UC)",
        "Segmentação de Clientes",
    ]),
]

# Pipeline bar
PIPELINE_LABEL = "Spark Declarative Pipelines (SDP)"
PIPELINE_COLOR = "#FF6F00"

# ─── Drawing ─────────────────────────────────────────────────────────────────

FONT_TITLE = 22
FONT_STAGE = 26
FONT_SUB = 18
FONT_FEAT = 16


def draw_stage_box(ax, x, y, w, h, name, subtitle, bg, border, features):
    """Draw a rounded stage box with name, subtitle and feature list."""
    box = FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0.12",
        facecolor=bg, edgecolor=border, linewidth=2.5, zorder=3,
    )
    ax.add_patch(box)

    # Name at top
    name_y = y + h - 0.85
    ax.text(x + w / 2, name_y, name, ha="center", va="center",
            fontsize=FONT_STAGE, fontweight="bold", color=border,
            zorder=5)

    if subtitle:
        ax.text(x + w / 2, name_y - 0.65, subtitle, ha="center", va="center",
                fontsize=FONT_SUB, color="#616161", fontstyle="italic", zorder=5)

    # Separator
    if features:
        sep_y = name_y - 1.1
        ax.plot([x + 0.3, x + w - 0.3], [sep_y, sep_y],
                color=border, linewidth=1.5, alpha=0.4, zorder=4)

        # Features
        fy = sep_y - 0.6
        for feat in features:
            ax.text(x + 0.5, fy, f"• {feat}", ha="left", va="center",
                    fontsize=FONT_FEAT, color="#000000", zorder=5)
            fy -= 0.6


def draw_arrow(ax, x1, y1, x2, y2, curved=False):
    """Draw an arrow between two points."""
    style = "arc3,rad=0.15" if curved else "arc3,rad=0.0"
    ax.annotate(
        "", xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(arrowstyle="-|>", color="#424242", lw=2.5,
                        mutation_scale=22, connectionstyle=style),
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

    # Title
    ax.text(FIG_W / 2, FIG_H - 1.0,
            "Arquitetura — Workshop Panvel",
            ha="center", va="center",
            fontsize=34, fontweight="bold", color="#000000", zorder=10)

    # ─── Source box (JSON) ───────────────────────────────────────────────
    src_w, src_h = 4.5, 2.8
    src_x = 1.0
    src_y = 8.0
    src = STAGES[0]
    box = FancyBboxPatch(
        (src_x, src_y), src_w, src_h, boxstyle="round,pad=0.12",
        facecolor=src[2], edgecolor=src[3], linewidth=2.5,
        linestyle="--", zorder=3,
    )
    ax.add_patch(box)
    ax.text(src_x + src_w / 2, src_y + src_h / 2, src[0],
            ha="center", va="center",
            fontsize=FONT_STAGE - 2, fontweight="bold", color=src[3], zorder=5)

    # ─── Main pipeline stages (BRONZE, SILVER, GOLD) ────────────────────
    stage_w = 7.5
    stage_h = 7.2
    gap = 1.3
    start_x = src_x + src_w + 1.8
    stage_y = 5.5

    positions = {}
    x = start_x
    for i, (name, subtitle, bg, border, features) in enumerate(STAGES[1:]):
        draw_stage_box(ax, x, stage_y, stage_w, stage_h, name, subtitle, bg, border, features)
        positions[name] = (x, stage_y, stage_w, stage_h)
        x += stage_w + gap

    # ─── Fan-out boxes (AI/BI and ML) ────────────────────────────────────
    fan_w = 6.8
    fan_h_aibi = 4.2
    fan_h_ml = 4.8
    fan_gap = 0.7
    gold_x, gold_y, gold_w, gold_h = positions["GOLD"]
    fan_x = gold_x + gold_w + 1.8

    fan_positions = {}
    # AI/BI on top
    aibi_y = stage_y + stage_h - fan_h_aibi
    name, bg, border, features = FANOUT[0]
    draw_stage_box(ax, fan_x, aibi_y, fan_w, fan_h_aibi, name, None, bg, border, features)
    fan_positions["AI/BI"] = (fan_x, aibi_y, fan_w, fan_h_aibi)

    # ML on bottom
    ml_y = aibi_y - fan_gap - fan_h_ml
    name, bg, border, features = FANOUT[1]
    draw_stage_box(ax, fan_x, ml_y, fan_w, fan_h_ml, name, None, bg, border, features)
    fan_positions["ML"] = (fan_x, ml_y, fan_w, fan_h_ml)

    # ─── Arrows ──────────────────────────────────────────────────────────
    # Source → BRONZE
    bx, by, bw, bh = positions["BRONZE"]
    draw_arrow(ax, src_x + src_w, src_y + src_h / 2, bx, by + bh / 2)

    # BRONZE → SILVER
    sx, sy, sw, sh = positions["SILVER"]
    draw_arrow(ax, bx + bw, by + bh / 2, sx, sy + sh / 2)

    # SILVER → GOLD
    gx, gy, gw, gh = positions["GOLD"]
    draw_arrow(ax, sx + sw, sy + sh / 2, gx, gy + gh / 2)

    # GOLD → AI/BI
    ax2, ay2, aw2, ah2 = fan_positions["AI/BI"]
    draw_arrow(ax, gx + gw, gy + gh / 2, ax2, ay2 + ah2 / 2, curved=True)

    # GOLD → ML
    mx, my, mw, mh = fan_positions["ML"]
    draw_arrow(ax, gx + gw, gy + gh / 2, mx, my + mh / 2, curved=True)

    # ─── Pipeline bar at bottom ──────────────────────────────────────────
    bar_y = stage_y - 2.8
    bar_x1 = bx
    bar_x2 = gx + gw
    bar_w = bar_x2 - bar_x1

    bar = FancyBboxPatch(
        (bar_x1, bar_y), bar_w, 1.2, boxstyle="round,pad=0.12",
        facecolor="#FFF3E0", edgecolor=PIPELINE_COLOR, linewidth=3.0, zorder=3,
    )
    ax.add_patch(bar)
    ax.text(bar_x1 + bar_w / 2, bar_y + 0.6, PIPELINE_LABEL,
            ha="center", va="center",
            fontsize=FONT_STAGE - 4, fontweight="bold", color=PIPELINE_COLOR, zorder=5)

    # Dashed lines from pipeline bar up to stages
    for name in ["BRONZE", "SILVER", "GOLD"]:
        px, py, pw, ph = positions[name]
        cx = px + pw / 2
        ax.plot([cx, cx], [bar_y + 1.2, py],
                color=PIPELINE_COLOR, linewidth=2.0, linestyle=":", alpha=0.6, zorder=2)

    # ─── Databricks Workflows label ──────────────────────────────────────
    wf_y = bar_y - 1.6
    wf_x = bar_x1
    wf_w = fan_x + fan_w - bar_x1

    wf = FancyBboxPatch(
        (wf_x, wf_y), wf_w, 1.1, boxstyle="round,pad=0.12",
        facecolor="#E8EAF6", edgecolor="#3949AB", linewidth=2.5, zorder=3,
    )
    ax.add_patch(wf)
    ax.text(wf_x + wf_w / 2, wf_y + 0.55,
            "Databricks Workflows  •  Unity Catalog  •  Serverless Compute",
            ha="center", va="center",
            fontsize=FONT_SUB, fontweight="bold", color="#3949AB", zorder=5)

    # ─── Save ────────────────────────────────────────────────────────────
    out = "/Users/juliandro.figueiro/workshop-panvel/images/arquitetura.png"
    fig.savefig(out, dpi=DPI, bbox_inches="tight", facecolor="white", pad_inches=0.3)
    plt.close(fig)
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
