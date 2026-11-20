"""Painel de clima e risco urbano em Pernambuco, voltado ao gestor público.

Lê só do Neon (tabelas dim_city, forecast_daily, forecast_hourly e
urban_risk_daily), que são alimentadas pela pipeline:
Open-Meteo -> MongoDB (bronze) -> PySpark -> Neon (prata/ouro) -> este painel.
"""

import html
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

# permite "from src import ..." mesmo rodando de outra pasta (ex.: Streamlit Cloud)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import config  # noqa: E402
from src.config import LIMIARES  # noqa: E402

st.set_page_config(page_title="Clima & Risco Urbano · PE", page_icon="🌦️", layout="wide")

FUSO = "America/Sao_Paulo"
LINK_INMET = "https://portal.inmet.gov.br/"

# ---------- tema (tokens de cor por modo) ----------
TEMAS = {
    "claro": {
        "bg": "#f7f5fb", "surface": "#ffffff", "surface2": "#f1edf8", "border": "#e6e0f0",
        "text": "#1d1630", "text2": "#5b5470", "muted": "#8a83a0", "accent": "#7033CC",
        "grid": "#ece8f3", "s1": "#2a78d6", "s2": "#eb6834",
    },
    "escuro": {
        "bg": "#110d1a", "surface": "#1b1527", "surface2": "#251d35", "border": "#342a48",
        "text": "#f3effa", "text2": "#c9c0dc", "muted": "#9a90b3", "accent": "#b48cf2",
        "grid": "#2a2238", "s1": "#3987e5", "s2": "#d95926",
    },
}

# status: cor fixa nos dois modos, sempre acompanhada de ícone + texto
NIVEIS = {
    "baixo": {"cor": "#0ca30c", "icone": "✅", "rotulo": "Baixo"},
    "moderado": {"cor": "#fab219", "icone": "⚠️", "rotulo": "Moderado"},
    "alto": {"cor": "#d03b3b", "icone": "🚨", "rotulo": "Alto"},
}

RISCOS = {
    "alagamento": {
        "nome": "Alagamento", "icone": "🌊",
        "denuncias": "ruas alagadas, bueiros entupidos, transbordamento de canais",
        "acao": "Limpeza preventiva de bueiros e canais; equipes de drenagem de prontidão.",
    },
    "deslizamento": {
        "nome": "Deslizamento de barreira", "icone": "⛰️",
        "denuncias": "deslizamento de barreiras, rachaduras em encostas e muros",
        "acao": "Acionar a Defesa Civil, vistoriar encostas e preparar abrigos temporários.",
    },
    "queda_arvore": {
        "nome": "Queda de árvore / fiação", "icone": "🌳",
        "denuncias": "queda de árvores e galhos, fios partidos, postes danificados",
        "acao": "Equipes de poda e a concessionária de energia de sobreaviso.",
    },
    "buracos": {
        "nome": "Buracos na via", "icone": "🕳️",
        "denuncias": "buracos e afundamento do asfalto, calçadas danificadas",
        "acao": "Programar operação tapa-buraco após as chuvas, priorizando vias principais.",
    },
    "calor": {
        "nome": "Calor extremo", "icone": "🔥",
        "denuncias": "falta d'água, queimadas em terrenos baldios, focos de incêndio",
        "acao": "Reforçar o abastecimento de água, fiscalizar queimadas e divulgar cuidados com o calor.",
    },
}

ORDEM_REGIOES = ["RMR", "Zona da Mata", "Agreste", "Sertão", "Fernando de Noronha"]
DIAS_SEMANA = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]


def br(valor, casas: int = 1) -> str:
    """Número no formato brasileiro (vírgula decimal). Nulo vira travessão."""
    if valor is None or pd.isna(valor):
        return "–"
    return f"{valor:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def rotulo_dia(dia: pd.Timestamp, hoje: pd.Timestamp) -> str:
    delta = (dia - hoje).days
    if delta == 0:
        return f"Hoje {dia:%d/%m}"
    if delta == 1:
        return f"Amanhã {dia:%d/%m}"
    return f"{DIAS_SEMANA[dia.weekday()]} {dia:%d/%m}"


def categoria_uv(uv) -> str:
    """Categorias de índice UV da OMS."""
    if uv is None or pd.isna(uv):
        return "–"
    if uv < 3:
        return "baixo"
    if uv < 6:
        return "moderado"
    if uv < 8:
        return "alto"
    if uv < 11:
        return "muito alto"
    return "extremo"


def nivel_de(valor) -> str | None:
    if valor is None or pd.isna(valor):
        return None
    return "baixo" if valor < 30 else "moderado" if valor < 60 else "alto"


def nivel_ok(n) -> str:
    return n if n in NIVEIS else "baixo"


def badge(nivel: str, texto: str | None = None) -> str:
    n = NIVEIS[nivel]
    return (
        f'<span class="badge" style="border-color:{n["cor"]};background:{n["cor"]}26">'
        f'{n["icone"]} {texto or n["rotulo"]}</span>'
    )


def secao(titulo: str, sub: str = "") -> None:
    st.markdown(
        f'<p class="sec">{titulo}</p>' + (f'<p class="sec-sub">{sub}</p>' if sub else ""),
        unsafe_allow_html=True,
    )


# ---------- dados (somente leitura do Neon) ----------

@st.cache_data(ttl=600, show_spinner="Carregando dados do Neon...")
def carregar() -> tuple[pd.DataFrame, pd.DataFrame]:
    engine = create_engine(config.DATABASE_URL)
    try:
        diario = pd.read_sql(
            """
            SELECT c.city, c.nome, c.uf, c.mesorregiao, c.regiao, c.latitude, c.longitude,
                   f.date, f.temperature_2m_max, f.temperature_2m_min, f.uv_index_max,
                   f.precipitation_sum, f.rain_sum, f.precipitation_probability_max,
                   f.wind_speed_10m_max, f.wind_gusts_10m_max, f.data_extracao,
                   r.is_forecast, r.chuva_72h, r.chuva_7d,
                   r.risco_alagamento, r.risco_deslizamento, r.risco_queda_arvore,
                   r.risco_buracos, r.risco_calor, r.risco_geral, r.risco_principal, r.nivel_risco
            FROM forecast_daily f
            JOIN dim_city c USING (city)
            LEFT JOIN urban_risk_daily r USING (city, date)
            """,
            engine,
        )
        horario = pd.read_sql("SELECT * FROM forecast_hourly", engine)
    finally:
        engine.dispose()
    diario["date"] = pd.to_datetime(diario["date"])
    horario["time"] = pd.to_datetime(horario["time"])
    return diario, horario


# ---------- cabeçalho e modo claro/escuro ----------

_, col_tema = st.columns([5, 1])
escuro = col_tema.toggle("🌙 Modo escuro", key="escuro")
T = TEMAS["escuro" if escuro else "claro"]

st.markdown(
    f"""<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');
:root {{
  --bg:{T['bg']}; --surface:{T['surface']}; --surface2:{T['surface2']}; --border:{T['border']};
  --text:{T['text']}; --text2:{T['text2']}; --muted:{T['muted']}; --accent:{T['accent']};
}}
.stApp {{ background: var(--bg); color: var(--text); font-family: 'Plus Jakarta Sans', sans-serif; }}
.stApp, .stApp p, .stApp li {{ color: var(--text); }}
.stApp a {{ color: var(--accent) !important; font-weight: 600; }}
header[data-testid="stHeader"] {{ background: transparent; }}
div[data-testid="stToolbar"], div[data-testid="stDecoration"], .stAppDeployButton {{ display: none !important; }}
.block-container {{ padding-top: 1.2rem; padding-bottom: 4rem; max-width: 1240px; }}

/* rótulos dos filtros */
div[data-testid="stWidgetLabel"] p {{ color: var(--text2) !important; font-weight: 600; }}
div[data-testid="stCheckbox"] label p {{ color: var(--text2) !important; font-weight: 600; }}

/* selectbox */
div[data-baseweb="select"] > div {{ background: var(--surface) !important; border-color: var(--border) !important; border-radius: 10px !important; }}
div[data-baseweb="select"] * {{ color: var(--text) !important; -webkit-text-fill-color: var(--text) !important; }}
div[data-baseweb="select"] svg {{ fill: var(--text2) !important; }}
div[data-baseweb="popover"] ul, div[data-baseweb="popover"] li {{ background: var(--surface) !important; color: var(--text) !important; }}
div[data-baseweb="popover"] li:hover, div[data-baseweb="popover"] li[aria-selected="true"] {{ background: var(--surface2) !important; }}

/* filtro de dia (segmented control) */
button[data-testid="stBaseButton-segmented_control"] {{ background: var(--surface) !important; border-color: var(--border) !important; }}
button[data-testid="stBaseButton-segmented_control"] p {{ color: var(--text2) !important; }}
button[data-testid="stBaseButton-segmented_controlActive"] {{ background: var(--accent) !important; border-color: var(--accent) !important; }}
button[data-testid="stBaseButton-segmented_controlActive"] p {{ color: #ffffff !important; }}

/* abas, expander e botão de download */
button[data-baseweb="tab"] {{ background: transparent !important; }}
button[data-baseweb="tab"] p {{ color: var(--text2) !important; font-weight: 600; }}
button[data-baseweb="tab"][aria-selected="true"] p {{ color: var(--accent) !important; }}
div[data-baseweb="tab-highlight"] {{ background: var(--accent) !important; }}
div[data-baseweb="tab-border"] {{ background: var(--border) !important; }}
div[data-testid="stExpander"] details {{ background: var(--surface); border: 1px solid var(--border); border-radius: 14px; }}
div[data-testid="stExpander"] summary p {{ color: var(--text) !important; font-weight: 600; }}
div[data-testid="stExpander"] summary svg {{ fill: var(--text2) !important; color: var(--text2) !important; }}
div[data-testid="stDownloadButton"] button {{ background: var(--surface) !important; border: 1px solid var(--border) !important; border-radius: 10px; }}
div[data-testid="stDownloadButton"] button p {{ color: var(--text) !important; }}

/* tooltip dos gráficos */
#vg-tooltip-element {{ background: var(--surface) !important; color: var(--text) !important; border: 1px solid var(--border) !important; border-radius: 8px; font-family: 'Plus Jakarta Sans', sans-serif; }}
#vg-tooltip-element td.key {{ color: var(--text2) !important; }}

/* componentes do painel */
.hero {{ background: linear-gradient(120deg, #7033CC 0%, #9B448C 55%, #C4545C 100%); border-radius: 20px; padding: 28px 32px; margin-bottom: 26px; }}
.hero h1 {{ color: #fff !important; font-size: 2rem; font-weight: 800; margin: 0; padding: 0; }}
.hero p {{ color: #f3e8ff !important; margin: 8px 0 0 0; font-size: 1rem; }}
.sec {{ color: var(--text) !important; font-weight: 800; font-size: 1.25rem; margin: 56px 0 4px 0; }}
.sec-sub {{ color: var(--muted) !important; font-size: .9rem; margin: 0 0 14px 0; }}
.card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 16px; padding: 18px 20px; }}
.kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin: 6px 0 8px 0; }}
.kpi-label {{ color: var(--text2) !important; font-size: .85rem; font-weight: 600; }}
.kpi-value {{ color: var(--text) !important; font-size: 1.75rem; font-weight: 800; line-height: 1.25; margin-top: 6px; }}
.kpi-sub {{ color: var(--muted) !important; font-size: .82rem; margin-top: 4px; }}
.badge {{ display: inline-block; padding: 2px 10px; border-radius: 999px; font-size: .78rem; font-weight: 700; color: var(--text) !important; border: 1px solid; white-space: nowrap; }}
.alerta {{ border-radius: 16px; padding: 18px 22px; margin: 6px 0 22px 0; border: 1px solid var(--border); border-left: 6px solid; background: var(--surface); }}
.alerta-titulo {{ color: var(--text) !important; font-weight: 800; font-size: 1.05rem; margin: 0; }}
.alerta p {{ color: var(--text2) !important; margin: 8px 0 0 0; font-size: .93rem; }}
.alerta b {{ color: var(--text) !important; }}
.rank {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; }}
.rank-item {{ background: var(--surface); border: 1px solid var(--border); border-left: 5px solid; border-radius: 14px; padding: 14px 16px; }}
.rank-topo {{ display: flex; align-items: center; gap: 10px; }}
.rank-n {{ color: var(--muted) !important; font-weight: 800; font-size: 1.1rem; }}
.rank-nome {{ color: var(--text) !important; font-weight: 700; margin: 0; flex: 1; min-width: 0; }}
.rank-det {{ color: var(--text2) !important; font-size: .84rem; margin: 8px 0 0 0; }}
.tabela {{ width: 100%; border-collapse: collapse; font-size: .88rem; }}
.tabela th {{ color: var(--text2) !important; text-align: right; font-weight: 700; padding: 8px 10px; border-bottom: 2px solid var(--border); background: var(--surface2); }}
.tabela td {{ color: var(--text) !important; text-align: right; padding: 7px 10px; border-bottom: 1px solid var(--border); }}
.tabela th:first-child, .tabela td:first-child {{ text-align: left; }}
.tabela-wrap {{ background: var(--surface); border: 1px solid var(--border); border-radius: 14px; overflow-x: auto; }}
.caminho {{ display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-top: 8px; }}
.etapa {{ background: var(--surface); border: 1px solid var(--border); border-radius: 999px; padding: 6px 14px; font-size: .85rem; font-weight: 600; color: var(--text) !important; }}
.seta {{ color: var(--muted) !important; font-weight: 700; }}
.rodape {{ color: var(--muted) !important; font-size: .82rem; margin-top: 12px; }}
.espaco {{ height: 18px; }}

/* celular */
@media (max-width: 900px) {{
  .rank {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
}}
@media (max-width: 640px) {{
  .hero {{ padding: 20px; border-radius: 16px; }}
  .hero h1 {{ font-size: 1.4rem; }}
  .hero p {{ font-size: .9rem; }}
  .sec {{ font-size: 1.1rem; margin-top: 40px; }}
  .kpis {{ grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }}
  .kpi-value {{ font-size: 1.35rem; }}
  .card {{ padding: 14px; }}
  .rank {{ grid-template-columns: 1fr; }}
}}
</style>""",
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="hero"><h1>🌦️ Clima & Risco Urbano · Pernambuco</h1>'
    "<p>Painel de apoio à gestão: antecipe ocorrências urbanas por região a partir da previsão do tempo "
    "e priorize as equipes e as ações preventivas.</p></div>",
    unsafe_allow_html=True,
)

# ---------- carga ----------
try:
    diario, horario = carregar()
except Exception as erro:  # tabela ausente, banco fora do ar etc.
    st.error(f"Não foi possível ler o Neon. Rode a pipeline antes. Detalhe: {erro}")
    st.stop()

if diario.empty:
    st.warning("Ainda não há dados no Neon. Rode a pipeline (orchestrate_prefect.py).")
    st.stop()

hoje = pd.Timestamp.now(tz=FUSO).normalize().tz_localize(None)
dias = sorted(diario.loc[diario["date"] >= hoje, "date"].unique()) or sorted(diario["date"].unique())[-7:]
dias = [pd.Timestamp(d) for d in dias][:7]

diario["nivel_ok"] = diario["nivel_risco"].map(nivel_ok)
diario["local"] = diario.apply(
    lambda r: r["nome"] if r["nome"] == r["regiao"] else f"{r['regiao']} · {r['nome']}", axis=1
)
cidades = (
    diario.drop_duplicates("city")
    .assign(ordem=lambda d: d["regiao"].map({r: i for i, r in enumerate(ORDEM_REGIOES)}).fillna(99))
    .sort_values(["ordem", "nome"])
)
rotulo_cidade = dict(zip(cidades["city"], cidades["local"]))

# ---------- filtros ----------
f1, f2 = st.columns([1, 2.2], vertical_alignment="bottom")
cidade = f1.selectbox("📍 Região · município-polo", list(rotulo_cidade), format_func=rotulo_cidade.get)
dia = f2.segmented_control(
    "📅 Dia", dias, default=dias[0], required=True,
    format_func=lambda d: rotulo_dia(d, hoje), width="stretch",
)
dia = pd.Timestamp(dia)
rot_dia = rotulo_dia(dia, hoje)

do_dia = diario[diario["date"] == dia].copy()
semana = diario[diario["date"].isin(dias)].copy()

# =====================================================================
# 1. VISÃO GERAL DO ESTADO
# =====================================================================
secao(f"🧭 Visão geral do estado · {rot_dia}", "Situação das regiões de Pernambuco no dia selecionado")

altos = do_dia[do_dia["nivel_ok"] == "alto"]
moderados = do_dia[do_dia["nivel_ok"] == "moderado"]
pior = do_dia.sort_values("risco_geral", ascending=False).iloc[0]
alertas_semana = semana[semana["nivel_ok"] == "alto"]
pior_tipo = RISCOS[pior["risco_principal"]]["nome"] if pior["risco_principal"] in RISCOS else "nenhum"


def nomes(df: pd.DataFrame) -> str:
    return ", ".join(df["nome"]) if not df.empty else "nenhuma"


cards_estado = [
    ("🚨 Em alerta (risco alto)", f"{len(altos)} de {len(do_dia)}", nomes(altos)),
    ("⚠️ Em atenção (moderado)", f"{len(moderados)} de {len(do_dia)}", nomes(moderados)),
    ("📈 Maior risco do dia", f"{br(pior['risco_geral'], 0)}%", f"{pior['local']} · {pior_tipo}"),
    ("🗓️ Alertas na semana", f"{len(alertas_semana)}", f"dias com risco alto, somando {alertas_semana['city'].nunique()} região(ões)"),
]
st.markdown(
    '<div class="kpis">' + "".join(
        f'<div class="card"><div class="kpi-label">{r}</div><div class="kpi-value">{v}</div><div class="kpi-sub">{s}</div></div>'
        for r, v, s in cards_estado
    ) + "</div>",
    unsafe_allow_html=True,
)

st.markdown('<div class="espaco"></div>', unsafe_allow_html=True)
secao("🎯 Onde agir primeiro", f"Regiões ordenadas pelo risco geral · {rot_dia}")
itens = []
for i, r in enumerate(do_dia.sort_values("risco_geral", ascending=False).itertuples(), start=1):
    info = RISCOS.get(r.risco_principal)
    detalhe = (
        f"{info['icone']} {info['nome']} · {info['acao']}"
        if info and r.nivel_ok != "baixo"
        else "Sem ação preventiva necessária."
    )
    itens.append(
        f'<div class="rank-item" style="border-left-color:{NIVEIS[r.nivel_ok]["cor"]}">'
        f'<div class="rank-topo"><span class="rank-n">{i}</span>'
        f'<p class="rank-nome">{html.escape(r.local)}</p>'
        f'{badge(r.nivel_ok, br(r.risco_geral, 0) + "%")}</div>'
        f'<p class="rank-det">{detalhe}</p></div>'
    )
st.markdown('<div class="rank">' + "".join(itens) + "</div>", unsafe_allow_html=True)
st.markdown('<div class="espaco"></div><div class="espaco"></div>', unsafe_allow_html=True)


# ---------- helpers de gráfico ----------

def estilo(chart: alt.TopLevelMixin, altura: int = 260) -> alt.TopLevelMixin:
    """Aplica o tema atual: fundo transparente, eixos e grade discretos."""
    return (
        chart.properties(height=altura, background="transparent")
        .configure(font="Plus Jakarta Sans")
        .configure_axis(
            labelColor=T["text2"], titleColor=T["text2"], gridColor=T["grid"],
            domainColor=T["border"], tickColor=T["border"], labelFontSize=12,
            titleFontSize=12, titleFontWeight="normal",
        )
        .configure_legend(labelColor=T["text2"], titleColor=T["text2"], orient="top", labelFontSize=12)
        .configure_view(stroke=None)
    )


def marca_dia() -> alt.Chart:
    """Linha tracejada no dia selecionado."""
    return alt.Chart(pd.DataFrame({"d": [dia]})).mark_rule(
        color=T["accent"], strokeWidth=2, strokeDash=[4, 3]
    ).encode(x="d:T")


EIXO_DIA = alt.Axis(format="%d/%m", labelAngle=0, tickCount="day", title=None)

# =====================================================================
# 2. PANORAMA DA SEMANA
# =====================================================================
secao("📊 Panorama da semana", "Risco geral (%) por região nos próximos dias · a coluna em destaque é o dia selecionado")
semana["dia"] = semana["date"].map(lambda d: rotulo_dia(d, hoje))
semana["nivel_txt"] = semana["nivel_ok"].map(lambda n: NIVEIS[n]["rotulo"])
semana["nivel_lbl"] = semana["nivel_ok"].map(lambda n: f"{NIVEIS[n]['icone']} {NIVEIS[n]['rotulo']}")
semana["principal_txt"] = semana["risco_principal"].map(lambda p: RISCOS[p]["nome"] if p in RISCOS else "nenhum")
semana["valor_txt"] = semana["risco_geral"].map(lambda v: br(v, 0))
ordem_linhas = list(rotulo_cidade.values())
ordem_dias = [rotulo_dia(d, hoje) for d in dias]

base = alt.Chart(semana).encode(
    x=alt.X("dia:O", sort=ordem_dias, title=None, axis=alt.Axis(orient="top", labelAngle=0, domain=False, ticks=False, labelFontWeight="bold")),
    y=alt.Y("local:N", sort=ordem_linhas, title=None, axis=alt.Axis(domain=False, ticks=False, labelLimit=260)),
)
celulas = base.mark_rect(cornerRadius=6, stroke=T["bg"], strokeWidth=3).encode(
    color=alt.Color(
        "nivel_lbl:N", title="Nível de risco",
        scale=alt.Scale(
            domain=[f"{n['icone']} {n['rotulo']}" for n in NIVEIS.values()],
            range=[n["cor"] for n in NIVEIS.values()],
        ),
    ),
    opacity=alt.condition(alt.datum.dia == rot_dia, alt.value(1), alt.value(0.55)),
    tooltip=[
        alt.Tooltip("local:N", title="Região"), alt.Tooltip("dia:N", title="Dia"),
        alt.Tooltip("valor_txt:N", title="Risco geral (%)"), alt.Tooltip("nivel_txt:N", title="Nível"),
        alt.Tooltip("principal_txt:N", title="Principal"),
    ],
)
textos = base.mark_text(fontWeight="bold", fontSize=13).encode(
    text="valor_txt:N",
    color=alt.condition(alt.datum.nivel_ok == "moderado", alt.value("#1d1630"), alt.value("#ffffff")),
)
st.altair_chart(estilo(celulas + textos, 46 * len(ordem_linhas)), width="stretch", theme=None)

# =====================================================================
# 3. REGIÃO SELECIONADA
# =====================================================================
d_cidade = diario[diario["city"] == cidade].sort_values("date")
linha = d_cidade[d_cidade["date"] == dia].iloc[0]
nome = linha["nome"]
secao(f"📍 {html.escape(linha['local'])}", f"{rot_dia} · município-polo da mesorregião {linha['mesorregiao']} (IBGE)")

# ---------- aviso ----------
nivel = linha["nivel_ok"]
principal = linha["risco_principal"]
uv_cat = categoria_uv(linha["uv_index_max"])

if nivel in ("alto", "moderado") and principal in RISCOS:
    info, n = RISCOS[principal], NIVEIS[nivel]
    titulo = f"{n['icone']} {'Alerta' if nivel == 'alto' else 'Atenção'}: risco {n['rotulo'].lower()} de {info['nome'].lower()} ({br(linha['risco_geral'], 0)}%)"
    corpo = (
        f"<p>Denúncias que podem estar relacionadas: <b>{info['denuncias']}</b>.</p>"
        f"<p>🛠️ Ação recomendada: {info['acao']}</p>"
    )
    cor_alerta = n["cor"]
else:
    titulo = "✅ Sem alertas relevantes para o dia selecionado"
    corpo = "<p>Os índices de risco urbano estão baixos. Nenhuma ação preventiva específica é necessária.</p>"
    cor_alerta = NIVEIS["baixo"]["cor"]

if uv_cat in ("alto", "muito alto", "extremo"):
    corpo += (
        f"<p>☀️ Índice UV <b>{uv_cat}</b> ({br(linha['uv_index_max'])}): reforçar campanhas de proteção solar "
        "em locais de grande circulação e evitar atividades ao ar livre entre 10h e 16h.</p>"
    )
corpo += f'<p>🔗 <a href="{LINK_INMET}" target="_blank">Confira os avisos meteorológicos oficiais vigentes no INMET ↗</a></p>'

st.markdown(
    f'<div class="alerta" style="border-left-color:{cor_alerta}"><p class="alerta-titulo">{titulo}</p>{corpo}</div>',
    unsafe_allow_html=True,
)

# ---------- indicadores do dia ----------
principal_txt = RISCOS[principal]["nome"] if principal in RISCOS else "nenhum"
cards = [
    ("🌡️ Temperatura", f"{br(linha['temperature_2m_max'])}°", f"mínima de {br(linha['temperature_2m_min'])}°C"),
    ("🌧️ Chuva", f"{br(linha['precipitation_sum'])} mm", f"chance de {br(linha['precipitation_probability_max'], 0)}% · 72h: {br(linha['chuva_72h'])} mm"),
    ("💨 Vento", f"{br(linha['wind_gusts_10m_max'], 0)} km/h", f"rajada máx. · vento médio {br(linha['wind_speed_10m_max'], 0)} km/h"),
    ("☀️ Índice UV", br(linha["uv_index_max"]), f"categoria {uv_cat} (OMS)"),
    ("🛡️ Risco geral", f"{br(linha['risco_geral'], 0)}%", f"{badge(nivel)} &nbsp;{principal_txt}"),
]
st.markdown(
    '<div class="kpis">' + "".join(
        f'<div class="card"><div class="kpi-label">{r}</div><div class="kpi-value">{v}</div><div class="kpi-sub">{s}</div></div>'
        for r, v, s in cards
    ) + "</div>",
    unsafe_allow_html=True,
)

# ---------- riscos do dia ----------
secao(f"🛡️ Riscos em {nome}", f"{rot_dia} · índice de 0 a 100% por tipo de ocorrência")
riscos = pd.DataFrame([
    {
        "risco": f"{RISCOS[k]['icone']} {RISCOS[k]['nome']}",
        "valor": linha[f"risco_{k}"],
        "nivel": nivel_de(linha[f"risco_{k}"]) or "baixo",
        "texto": f"{br(linha[f'risco_{k}'], 0)}%",
        "denuncias": RISCOS[k]["denuncias"],
        "acao": RISCOS[k]["acao"],
    }
    for k in RISCOS
])
riscos["valor_plot"] = riscos["valor"].fillna(0)
riscos["nivel_txt"] = riscos["nivel"].map(lambda n: NIVEIS[n]["rotulo"])

base_r = alt.Chart(riscos).encode(
    y=alt.Y("risco:N", sort=riscos["risco"].tolist(), title=None,
            axis=alt.Axis(labelLimit=240, labelFontSize=13, domain=False, ticks=False))
)
trilho = base_r.mark_bar(cornerRadius=4, height=20, color=T["surface2"]).encode(x=alt.datum(100))
barras = base_r.mark_bar(cornerRadius=4, height=20).encode(
    x=alt.X("valor_plot:Q", scale=alt.Scale(domain=[0, 100]), title=None,
            axis=alt.Axis(values=[0, 30, 60, 100], format="d", grid=False)),
    color=alt.Color("nivel:N", legend=None,
                    scale=alt.Scale(domain=list(NIVEIS), range=[n["cor"] for n in NIVEIS.values()])),
    tooltip=[
        alt.Tooltip("risco:N", title="Risco"), alt.Tooltip("texto:N", title="Índice"),
        alt.Tooltip("nivel_txt:N", title="Nível"),
        alt.Tooltip("denuncias:N", title="Denúncias que podem estar relacionadas"),
        alt.Tooltip("acao:N", title="Ação recomendada"),
    ],
)
rotulos = base_r.mark_text(align="left", dx=6, fontWeight="bold", fontSize=13, color=T["text"]).encode(
    x="valor_plot:Q", text="texto:N"
)
limites = alt.Chart(pd.DataFrame({"x": [30, 60]})).mark_rule(strokeDash=[3, 3], color=T["muted"], opacity=0.6).encode(x="x:Q")
st.altair_chart(estilo(trilho + barras + limites + rotulos, 250), width="stretch", theme=None)
st.markdown(
    '<p class="rodape">'
    + " &nbsp;·&nbsp; ".join(f'{n["icone"]} {n["rotulo"]} ({faixa})' for n, faixa in zip(NIVEIS.values(), ["< 30%", "30–60%", "≥ 60%"]))
    + "</p>",
    unsafe_allow_html=True,
)

# =====================================================================
# 4. DETALHES METEOROLÓGICOS
# =====================================================================
secao(f"🔎 Detalhes de {nome}")
aba_temp, aba_chuva, aba_vento, aba_hora, aba_dados = st.tabs(
    ["🌡️ Temperatura", "🌧️ Chuva", "💨 Vento", "🕐 Hora a hora", "📋 Dados"]
)

tooltip_dia = alt.Tooltip("date:T", title="Data", format="%d/%m/%Y")

with aba_temp:
    temp = d_cidade.melt(id_vars="date", value_vars=["temperature_2m_max", "temperature_2m_min"], var_name="serie", value_name="valor")
    temp["serie"] = temp["serie"].map({"temperature_2m_max": "Máxima", "temperature_2m_min": "Mínima"})
    linhas = alt.Chart(temp).mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=60, filled=True)).encode(
        x=alt.X("date:T", axis=EIXO_DIA),
        y=alt.Y("valor:Q", title="°C", scale=alt.Scale(zero=False)),
        color=alt.Color("serie:N", title=None, scale=alt.Scale(domain=["Máxima", "Mínima"], range=[T["s2"], T["s1"]])),
        tooltip=[tooltip_dia, alt.Tooltip("serie:N", title="Série"), alt.Tooltip("valor:Q", title="°C", format=".1f")],
    )
    st.altair_chart(estilo(marca_dia() + linhas), width="stretch", theme=None)

with aba_chuva:
    c1, c2 = st.columns(2, gap="large")
    barras_chuva = alt.Chart(d_cidade).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color=T["s1"], size=18).encode(
        x=alt.X("date:T", axis=EIXO_DIA),
        y=alt.Y("precipitation_sum:Q", title="mm"),
        tooltip=[tooltip_dia, alt.Tooltip("precipitation_sum:Q", title="Chuva (mm)", format=".1f"),
                 alt.Tooltip("chuva_72h:Q", title="Acumulado 72h (mm)", format=".1f"),
                 alt.Tooltip("chuva_7d:Q", title="Acumulado 7 dias (mm)", format=".1f")],
    )
    c1.markdown('<p class="sec-sub"><b>Chuva por dia (mm)</b></p>', unsafe_allow_html=True)
    c1.altair_chart(estilo(marca_dia() + barras_chuva, 240), width="stretch", theme=None)
    linha_prob = alt.Chart(d_cidade).mark_line(strokeWidth=2, color=T["s1"], point=alt.OverlayMarkDef(size=60, filled=True, color=T["s1"])).encode(
        x=alt.X("date:T", axis=EIXO_DIA),
        y=alt.Y("precipitation_probability_max:Q", title="%", scale=alt.Scale(domain=[0, 100])),
        tooltip=[tooltip_dia, alt.Tooltip("precipitation_probability_max:Q", title="Chance máx. (%)", format=".0f")],
    )
    c2.markdown('<p class="sec-sub"><b>Chance máxima de chuva (%)</b></p>', unsafe_allow_html=True)
    c2.altair_chart(estilo(marca_dia() + linha_prob, 240), width="stretch", theme=None)

with aba_vento:
    vento = d_cidade.melt(id_vars="date", value_vars=["wind_gusts_10m_max", "wind_speed_10m_max"], var_name="serie", value_name="valor")
    vento["serie"] = vento["serie"].map({"wind_gusts_10m_max": "Rajada máxima", "wind_speed_10m_max": "Vento médio máximo"})
    linhas_v = alt.Chart(vento).mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=60, filled=True)).encode(
        x=alt.X("date:T", axis=EIXO_DIA),
        y=alt.Y("valor:Q", title="km/h"),
        color=alt.Color("serie:N", title=None, scale=alt.Scale(domain=["Rajada máxima", "Vento médio máximo"], range=[T["s1"], T["s2"]])),
        tooltip=[tooltip_dia, alt.Tooltip("serie:N", title="Série"), alt.Tooltip("valor:Q", title="km/h", format=".0f")],
    )
    alerta_vento = alt.Chart(pd.DataFrame({"y": [40]})).mark_rule(strokeDash=[3, 3], color=NIVEIS["moderado"]["cor"]).encode(y="y:Q")
    st.altair_chart(estilo(marca_dia() + alerta_vento + linhas_v), width="stretch", theme=None)
    st.markdown('<p class="rodape">Linha amarela tracejada: 40 km/h, início do "perigo potencial" para ventos nos avisos do INMET.</p>', unsafe_allow_html=True)

with aba_hora:
    h = horario[(horario["city"] == cidade) & (horario["time"].dt.normalize() == dia)].sort_values("time")
    if h.empty:
        st.info("Sem dados horários para este dia.")
    else:
        c1, c2 = st.columns(2, gap="large")
        eixo_hora = alt.Axis(format="%Hh", labelAngle=0, title=None)
        tooltip_hora = alt.Tooltip("time:T", title="Hora", format="%H:%M")
        temp_h = alt.Chart(h).mark_line(strokeWidth=2, color=T["s2"], point=alt.OverlayMarkDef(size=36, filled=True, color=T["s2"])).encode(
            x=alt.X("time:T", axis=eixo_hora),
            y=alt.Y("temperature_2m:Q", title="°C", scale=alt.Scale(zero=False)),
            tooltip=[tooltip_hora, alt.Tooltip("temperature_2m:Q", title="Temperatura (°C)", format=".1f"),
                     alt.Tooltip("wind_gusts_10m:Q", title="Rajada (km/h)", format=".0f")],
        )
        c1.markdown(f'<p class="sec-sub"><b>Temperatura por hora · {rot_dia}</b></p>', unsafe_allow_html=True)
        c1.altair_chart(estilo(temp_h, 240), width="stretch", theme=None)
        chuva_h = alt.Chart(h).mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3, color=T["s1"], size=9).encode(
            x=alt.X("time:T", axis=eixo_hora),
            y=alt.Y("precipitation_probability:Q", title="%", scale=alt.Scale(domain=[0, 100])),
            tooltip=[tooltip_hora, alt.Tooltip("precipitation_probability:Q", title="Chance (%)", format=".0f"),
                     alt.Tooltip("precipitation:Q", title="Chuva (mm)", format=".1f")],
        )
        c2.markdown(f'<p class="sec-sub"><b>Chance de chuva por hora · {rot_dia}</b></p>', unsafe_allow_html=True)
        c2.altair_chart(estilo(chuva_h, 240), width="stretch", theme=None)

with aba_dados:
    tabela = pd.DataFrame({
        "Data": d_cidade["date"].dt.strftime("%d/%m/%Y"),
        "Tipo": d_cidade["is_forecast"].map({True: "Previsão", False: "Passado"}),
        "Máx. °C": d_cidade["temperature_2m_max"].map(br),
        "Mín. °C": d_cidade["temperature_2m_min"].map(br),
        "Chuva mm": d_cidade["precipitation_sum"].map(br),
        "Chance %": d_cidade["precipitation_probability_max"].map(lambda v: br(v, 0)),
        "Rajada km/h": d_cidade["wind_gusts_10m_max"].map(lambda v: br(v, 0)),
        "UV": d_cidade["uv_index_max"].map(br),
        "Risco %": d_cidade["risco_geral"].map(lambda v: br(v, 0)),
        "Nível": d_cidade["nivel_risco"].map(lambda n: f"{NIVEIS[n]['icone']} {NIVEIS[n]['rotulo']}" if n in NIVEIS else "–"),
        "Principal": d_cidade["risco_principal"].map(lambda p: RISCOS[p]["nome"] if p in RISCOS else "nenhum"),
    })
    st.markdown(
        '<div class="tabela-wrap">' + tabela.to_html(index=False, classes="tabela", border=0, escape=True).replace("\n", "") + "</div>",
        unsafe_allow_html=True,
    )
    st.write("")
    st.download_button(
        "⬇️ Baixar dados (CSV)",
        d_cidade.drop(columns=["latitude", "longitude", "nivel_ok"]).to_csv(index=False).encode("utf-8"),
        file_name=f"clima_risco_{cidade}.csv",
        mime="text/csv",
    )

# =====================================================================
# 5. METODOLOGIA E CAMINHO DO DADO
# =====================================================================
st.markdown('<div class="espaco"></div><div class="espaco"></div>', unsafe_allow_html=True)
with st.expander("ℹ️ Como o índice de risco é calculado"):
    linhas_met = "".join(
        f"<tr><td>{RISCOS[k]['icone']} {RISCOS[k]['nome']}</td><td>{html.escape(var)}</td>"
        f"<td>{br(ini, 0)}</td><td>{br(fim, 0)}</td><td>{RISCOS[k]['denuncias']}</td></tr>"
        for k, (var, ini, fim) in LIMIARES.items()
    )
    st.markdown(
        "<p>Cada risco vai de <b>0%</b> a <b>100%</b> e cresce de forma linear entre dois limiares da variável "
        "meteorológica. Os limiares de chuva e vento seguem os critérios públicos dos avisos do INMET "
        "(chuva: 50 mm/dia = perigo, 100 mm/dia = grande perigo; vento: a partir de 40 km/h = perigo potencial). "
        "Os demais são aproximações ajustáveis.</p>"
        '<div class="tabela-wrap"><table class="tabela"><tr><th>Risco</th><th>Variável</th><th>0% em</th>'
        f"<th>100% em</th><th>Denúncias que podem estar relacionadas</th></tr>{linhas_met}</table></div>"
        f'<p>🔗 <a href="{LINK_INMET}" target="_blank">Portal de avisos meteorológicos do INMET ↗</a>, '
        "para conferir os alertas oficiais vigentes.</p>"
        "<p class='rodape'>⚠️ É um índice heurístico de apoio, não uma probabilidade medida: ele não usa o "
        "histórico de denúncias. <b>Risco geral</b> = o maior dos cinco índices do dia. "
        "Cada região é representada por um município-polo (a Open-Meteo responde por ponto, não por área). "
        "Os dados meteorológicos vêm da Open-Meteo; do INMET usamos apenas os critérios dos avisos.</p>",
        unsafe_allow_html=True,
    )

ultima = pd.to_datetime(diario["data_extracao"]).max().tz_localize("UTC").tz_convert(FUSO)
etapas = ["🌐 API Open-Meteo", "🍃 MongoDB · bronze", "⚡ PySpark", "🐘 Neon · prata e ouro", "📊 Este painel"]
secao("🔗 Caminho do dado")
st.markdown(
    '<div class="caminho">'
    + '<span class="seta">→</span>'.join(f'<span class="etapa">{e}</span>' for e in etapas)
    + f'</div><p class="rodape">Última extração: {ultima:%d/%m/%Y às %H:%M} (horário de Brasília) · '
    "orquestrado com Prefect · fonte: Open-Meteo.</p>",
    unsafe_allow_html=True,
)
