import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

fig, ax = plt.subplots(figsize=(28, 10), dpi=200)
ax.set_xlim(0, 28)
ax.set_ylim(0, 10)
ax.axis('off')
fig.patch.set_facecolor('#FAFBFC')

# Title
ax.text(14, 9.4, 'Arquitetura Medallion — Workshop Panvel', fontsize=28, fontweight='bold',
        ha='center', va='center', color='#1B2A4A', fontfamily='sans-serif')
ax.text(14, 8.8, 'Delta Lake  ·  Auto Loader  ·  Unity Catalog  ·  Genie AI  ·  MLflow',
        fontsize=14, ha='center', va='center', color='#6B7B8D', fontfamily='sans-serif')

# Separator line
ax.plot([1, 27], [8.4, 8.4], color='#E0E4E8', linewidth=1.5)

# Stage definitions - more spread out
stages = [
    {
        'x': 2.8, 'label': 'FONTE DE DADOS',
        'color': '#1B7F37', 'light': '#E6F4EA', 'border': '#1B7F37',
        'items': ['JSON Vendas', 'Streaming', 'Tempo Real'],
    },
    {
        'x': 8.4, 'label': 'BRONZE',
        'color': '#8B4513', 'light': '#FDF0E2', 'border': '#8B4513',
        'items': ['Auto Loader', 'Ingestão Contínua', '', 'Raw Data', 'Sem Transformação'],
    },
    {
        'x': 14.0, 'label': 'SILVER',
        'color': '#475569', 'light': '#F0F2F5', 'border': '#475569',
        'items': ['Limpeza & Validação', 'Enriquecimento', 'Extração de Bairro'],
    },
    {
        'x': 19.6, 'label': 'GOLD',
        'color': '#B8860B', 'light': '#FFF8E1', 'border': '#B8860B',
        'items': ['Agregações', 'KPIs & Métricas', 'Rankings'],
    },
    {
        'x': 25.2, 'label': 'AI/BI + ML',
        'color': '#1565C0', 'light': '#E3F2FD', 'border': '#1565C0',
        'items': ['Genie (NL)', 'Dashboard', 'Segmentação ML'],
    },
]

box_w = 4.0
box_h = 5.2
box_y = 1.8

for s in stages:
    cx = s['x']
    # Main box
    rect = FancyBboxPatch((cx - box_w/2, box_y), box_w, box_h,
                           boxstyle="round,pad=0.15", linewidth=2.5,
                           edgecolor=s['border'], facecolor=s['light'])
    ax.add_patch(rect)

    # Header band
    header_h = 1.1
    header = FancyBboxPatch((cx - box_w/2, box_y + box_h - header_h), box_w, header_h,
                             boxstyle="round,pad=0.15", linewidth=0,
                             facecolor=s['color'])
    ax.add_patch(header)
    clip_rect = plt.Rectangle((cx - box_w/2, box_y + box_h - header_h), box_w, 0.3,
                               facecolor=s['color'], edgecolor='none')
    ax.add_patch(clip_rect)

    # Header text
    ax.text(cx, box_y + box_h - header_h/2, s['label'],
            fontsize=16, fontweight='bold', ha='center', va='center',
            color='white', fontfamily='sans-serif')

    # Content items
    content_top = box_y + box_h - header_h - 0.6
    for i, item in enumerate(s['items']):
        if item == '':
            continue
        y = content_top - i * 0.75
        ax.text(cx, y, item, fontsize=14, ha='center', va='center',
                color='#2D3748', fontfamily='sans-serif')

# Arrows between stages
arrow_y = box_y + box_h / 2
arrow_style = "Simple,tail_width=10,head_width=28,head_length=14"
arrow_labels = ['Streaming Ingest', 'Cleanse & Enrich', 'Aggregate & Serve', 'Analytics & AI']

for i in range(len(stages) - 1):
    x_start = stages[i]['x'] + box_w/2 + 0.15
    x_end = stages[i+1]['x'] - box_w/2 - 0.15

    arrow = FancyArrowPatch((x_start, arrow_y), (x_end, arrow_y),
                             arrowstyle=arrow_style, color='#94A3B8',
                             mutation_scale=1, linewidth=0)
    ax.add_patch(arrow)

    # Label centered above arrow
    x_mid = (x_start + x_end) / 2
    ax.text(x_mid, arrow_y + 0.9, arrow_labels[i], fontsize=11, ha='center', va='center',
            color='#475569', fontfamily='sans-serif', fontstyle='italic')

# Bottom bar
ax.fill_between([0, 28], [0.3, 0.3], [0.9, 0.9], color='#1B2A4A')
ax.text(14, 0.6, 'Databricks Workshop  ·  Panvel  ·  Engenharia de Dados com Delta Lake & AI',
        fontsize=12, ha='center', va='center', color='#CBD5E1', fontfamily='sans-serif')

# Colored dots in footer
dot_colors = ['#1B7F37', '#8B4513', '#475569', '#B8860B', '#1565C0']
for i, c in enumerate(dot_colors):
    ax.plot(5.5 + i * 0.5, 0.25, 'o', color=c, markersize=6)

plt.tight_layout(pad=0.5)
plt.savefig('/Users/juliandro.figueiro/workshop-panvel/images/arquitetura_medallion.png',
            bbox_inches='tight', facecolor=fig.get_facecolor(), dpi=200)
plt.close()
print("Done!")
