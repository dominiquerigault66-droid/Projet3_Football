"""
utils.py — Helpers partagés, CSS global et composants réutilisables.
Utilise le système de thèmes (theme/theme_config.py).
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

from theme import get_theme, generate_css

# Carrega o tema corretamente
_T = get_theme()

# Palette accessible globalement
COLORS = {
    "primary": _T.primary,
    "secondary": _T.secondary,
    "text": _T.text,
    "text2": '#666666',
}

POSTES_MAP = {
    "Tous": "Tous",
    "Attaquant": "ATT",
    "Milieu": "MIL",
    "Défenseur": "DEF",
    "Gardien": "GK",
}
POSTES_LABELS = {v: k for k, v in POSTES_MAP.items() if v != "Tous"}


def inject_css():
    theme = get_theme()  # ✅ agora é objeto, não string
    st.markdown(generate_css(theme), unsafe_allow_html=True)


def score_gauge(score: float, label: str, color: str = None) -> str:
    t = get_theme()
    c = color or t.primary
    level = "🔴" if score < 40 else "🟡" if score < 65 else "🟢"
    return f"""
    <div style="text-align:center; padding: 1rem;">
        <div class="score-circle" style="width:140px;height:140px;
             background:conic-gradient({c} {score*3.6}deg,{t.bg3} 0deg);
             box-shadow:0 0 40px {c}33;">
            <div style="width:110px;height:110px;background:{t.bg2};border-radius:50%;
                 display:flex;flex-direction:column;align-items:center;justify-content:center;">
                <span style="color:{c};font-family:var(--font-display);font-size:2rem;font-weight:800;">{score:.0f}</span>
                <span style="font-size:0.9rem;color:{t.text2};">/100</span>
            </div>
        </div>
        <div style="margin-top:8px;font-family:var(--font-display);font-weight:700;
             font-size:0.95rem;color:{t.text};">{level} {label}</div>
    </div>"""


def radar_chart(categories: list, values: list, name: str, color: str) -> go.Figure:
    t = get_theme()
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=values + [values[0]], theta=categories + [categories[0]],
        fill="toself", fillcolor=f"rgba({_hex_to_rgb(color)},0.15)",
        line=dict(color=color, width=2), name=name,
    ))
    fig.update_layout(
        polar=dict(
            bgcolor=t.bg2,
            radialaxis=dict(visible=True, range=[0,100], gridcolor=t.border,
                            tickfont=dict(color=t.text2, size=10)),
            angularaxis=dict(gridcolor=t.border,
                             tickfont=dict(color=t.text, size=11, family=t.font_display)),
        ),
        showlegend=False, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=40,r=40,t=40,b=40), height=380,
    )
    return fig


def bar_chart(df: pd.DataFrame, x: str, y: str, color: str = None, horizontal: bool = False) -> go.Figure:
    t = get_theme()
    c = color or t.primary
    fig = px.bar(df, x=y, y=x, orientation="h", color_discrete_sequence=[c]) if horizontal \
          else px.bar(df, x=x, y=y, color_discrete_sequence=[c])
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=t.text, family=t.font_body),
        xaxis=dict(gridcolor=t.border, tickfont=dict(color=t.text2)),
        yaxis=dict(gridcolor=t.border, tickfont=dict(color=t.text2)),
        margin=dict(l=10,r=10,t=20,b=10), height=320,
    )
    fig.update_traces(marker_color=c, marker_line_width=0)
    return fig


def pie_chart(df: pd.DataFrame, names: str, values: str) -> go.Figure:
    t = get_theme()
    palette = [t.primary, t.secondary, t.accent,
               "#818CF8","#FB923C","#34D399","#F472B6","#60A5FA","#A78BFA","#4ADE80","#FACC15","#38BDF8"]
    fig = px.pie(df, names=names, values=values, color_discrete_sequence=palette, hole=0.45)
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", font=dict(color=t.text, family=t.font_body),
        legend=dict(font=dict(color=t.text2)), margin=dict(l=0,r=0,t=20,b=0), height=320,
    )
    fig.update_traces(textfont_color=t.text)
    return fig


def line_chart(x, y, title: str = "", color: str = None) -> go.Figure:
    t = get_theme()
    c = color or t.primary
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x, y=y, mode="lines+markers",
        line=dict(color=c, width=2.5), marker=dict(color=c, size=7),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(c)},0.08)",
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=t.text, family=t.font_body),
        xaxis=dict(gridcolor=t.border, tickfont=dict(color=t.text2)),
        yaxis=dict(gridcolor=t.border, tickfont=dict(color=t.text2)),
        title=dict(text=title, font=dict(color=t.text2, size=12)),
        margin=dict(l=10,r=10,t=30,b=10), height=260,
    )
    return fig


def _hex_to_rgb(hex_color: str) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
    return f"{r},{g},{b}"


def format_valeur(eur) -> str:
    if eur is None: return "N/A"
    try:
        v = float(eur)
        if v >= 1_000_000: return f"€{v/1_000_000:.1f}M"
        elif v >= 1_000: return f"€{v/1_000:.0f}K"
        return f"€{v:.0f}"
    except Exception: return str(eur)


def format_salaire(annuel) -> str:
    if annuel is None: return "N/A"
    try:
        v = float(annuel)
        return f"€{v/52:,.0f}/sem · €{v/1_000:.0f}K/an"
    except Exception: return str(annuel)


def sidebar_branding():
    t = get_theme()
    st.markdown(f"""
    <div style="padding:0.8rem 0 0.5rem;">
        <div class="sidebar-brand">⚽ Football Analytics</div>
        <div class="sidebar-sub">WildCodeSchool · Bootcamp DA 11/25</div>
    </div>
    <hr style="border-color:{t.border};margin:0.6rem 0 1rem;">
    """, unsafe_allow_html=True)