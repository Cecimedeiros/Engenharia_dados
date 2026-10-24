import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

from src import config

st.set_page_config(page_title="Previsão do tempo", page_icon="🌦️", layout="wide")

ROXO, MAGENTA, CORAL = "#7033CC", "#9B448C", "#C4545C"
BRANCO, BRANCO_SUAVE = "#FFFFFF", "#F3E8FF"
ROXO_ESCURO = "#2E1459"

st.markdown(
    f"""
    <style>
    .stApp {{
        background: linear-gradient(135deg, {ROXO} 0%, {MAGENTA} 55%, {CORAL} 100%);
        background-attachment: fixed;
    }}

    /* barra do Streamlit (Deploy/menu): escondida para não aparecer como caixa branca */
    header[data-testid="stHeader"] {{ background: transparent; }}
    div[data-testid="stToolbar"],
    div[data-testid="stDecoration"],
    .stAppDeployButton {{ display: none !important; }}

    /* textos em branco, só onde importa */
    .stApp h1, .stApp h2, .stApp h3,
    div[data-testid="stWidgetLabel"] *,
    div[data-testid="stMarkdownContainer"] * {{ color: {BRANCO}; }}

    /* cartões do st.metric */
    div[data-testid="stMetric"] {{
        background: rgba(255, 255, 255, 0.14);
        border: 1px solid rgba(255, 255, 255, 0.30);
        border-radius: 14px;
        padding: 16px 20px;
    }}
    div[data-testid="stMetricLabel"] * {{ color: {BRANCO_SUAVE} !important; }}
    div[data-testid="stMetricValue"] * {{ color: {BRANCO} !important; font-weight: 700; }}
    div[data-testid="stMetricDelta"] * {{ color: {BRANCO_SUAVE} !important; }}
    div[data-testid="stMetricDeltaIcon-Up"],
    div[data-testid="stMetricDeltaIcon-Down"] {{ display: none; }}

    /* selectbox fechado: caixa translúcida, texto branco */
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div {{
        background: rgba(255, 255, 255, 0.16) !important;
        border: 1px solid rgba(255, 255, 255, 0.35) !important;
        border-radius: 10px !important;
    }}
    div[data-testid="stSelectbox"] div[data-baseweb="select"] * {{
        color: {BRANCO} !important;
        -webkit-text-fill-color: {BRANCO} !important;
    }}
    div[data-testid="stSelectbox"] svg {{ fill: {BRANCO} !important; }}

    /* lista de opções (abre fora do app, num popover) */
    div[data-baseweb="popover"] * {{
        background: transparent !important;
        color: {BRANCO} !important;
        -webkit-text-fill-color: {BRANCO} !important;
    }}
    div[data-baseweb="popover"] > div,
    div[data-baseweb="popover"] ul,
    div[data-baseweb="popover"] [data-baseweb="menu"] {{
        background: {ROXO_ESCURO} !important;
    }}
    div[data-baseweb="popover"] li:hover,
    div[data-baseweb="popover"] li[aria-selected="true"] {{
        background: {ROXO} !important;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=600)
def carregar() -> pd.DataFrame:
    engine = create_engine(config.DATABASE_URL)
    df = pd.read_sql("SELECT * FROM forecast_7days_daily", engine)
    engine.dispose()
    df["time"] = pd.to_datetime(df["time"])
    return df


df = carregar()

st.title("🌦️ Previsão do tempo - próximos 7 dias")

cidade = st.selectbox("Cidade", sorted(df["city"].unique()))

# limita aos 7 primeiros dias da cidade escolhida (métricas e gráfico usam o mesmo recorte)
d = df[df["city"] == cidade].sort_values("time").head(7).copy()
d["dia"] = d["time"].dt.strftime("%d/%m/%Y")

# ---------- indicadores ----------
col1, col2, col3 = st.columns(3)

linha_max = d.loc[d["temperature_2m_max"].idxmax()]
col1.metric(
    "Temperatura máxima da semana",
    f"{linha_max['temperature_2m_max']:.1f} °C",
    linha_max["dia"],
    delta_color="off",
)

if d["uv_index_max"].notna().any():
    linha_uv = d.loc[d["uv_index_max"].idxmax()]
    col2.metric(
        "Maior índice UV",
        f"{linha_uv['uv_index_max']:.1f}",
        linha_uv["dia"],
        delta_color="off",
    )

if "data_extracao" in d.columns:
    atualizado = pd.to_datetime(d["data_extracao"]).max()
    col3.metric("Atualizado em (UTC)", atualizado.strftime("%d/%m/%Y %H:%M"))

# ---------- gráfico ----------
st.subheader(f"Temperaturas máxima e mínima - {cidade.replace('_', ' ').title()}")

longo = d.melt(
    id_vars=["time", "dia"],
    value_vars=["temperature_2m_max", "temperature_2m_min"],
    var_name="serie",
    value_name="temperatura",
)
longo["serie"] = longo["serie"].map(
    {"temperature_2m_max": "Máxima", "temperature_2m_min": "Mínima"}
)

grafico = (
    alt.Chart(longo)
    .mark_line(point=alt.OverlayMarkDef(size=70, filled=True), strokeWidth=3)
    .encode(
        # eixo de categorias: um rótulo por dia, na ordem das datas (sem repetição)
        x=alt.X("dia:O", sort=d["dia"].tolist(), title=None, axis=alt.Axis(labelAngle=-30)),
        y=alt.Y("temperatura:Q", title="Temperatura (°C)", scale=alt.Scale(zero=False)),
        color=alt.Color(
            "serie:N",
            title=None,
            scale=alt.Scale(domain=["Máxima", "Mínima"], range=[BRANCO, "#FFC2CF"]),
        ),
        tooltip=[
            alt.Tooltip("dia:O", title="Data"),
            alt.Tooltip("serie:N", title="Série"),
            alt.Tooltip("temperatura:Q", title="°C", format=".1f"),
        ],
    )
    .properties(height=380, background="transparent")
    .configure_axis(
        labelColor=BRANCO, titleColor=BRANCO,
        gridColor="rgba(255,255,255,0.20)", domainColor=BRANCO, tickColor=BRANCO,
    )
    .configure_legend(labelColor=BRANCO, orient="top")
    .configure_view(stroke=None)
)

st.altair_chart(grafico, use_container_width=True, theme=None)