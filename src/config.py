"""Credenciais (lidas uma única vez do .env ou do ambiente) e parâmetros da pipeline.

As credenciais são exigidas só quando usadas: a pipeline precisa do Mongo e do
Neon, mas o dashboard publicado só precisa do DATABASE_URL (menor privilégio).
"""
import os
from dotenv import load_dotenv

load_dotenv()

_OBRIGATORIAS = ("MONGODB_URI", "DATABASE_URL")

# Parâmetros do índice de risco urbano (usados pelo Spark e pelo dashboard).
# Formato: (variável, valor em que o risco começa, valor em que chega a 100%).
# Entre os dois limites o risco cresce de forma linear (rampa 0-100).
# Referências: alertas do INMET para chuva (50 mm/dia = perigo, 100 mm/dia = grande
# perigo) e vento (40-60 km/h perigo potencial, acima de 100 km/h grande perigo).
# Deslizamento, buracos e calor usam limiares aproximados e ajustáveis.
# São índices heurísticos, não probabilidades medidas.
LIMIARES: dict[str, tuple[str, float, float]] = {
    "alagamento":   ("precipitation_sum", 10.0, 100.0),    # mm no dia
    "deslizamento": ("chuva_72h", 50.0, 150.0),            # mm acumulados em 3 dias
    "queda_arvore": ("wind_gusts_10m_max", 40.0, 100.0),   # km/h de rajada
    "buracos":      ("chuva_7d", 50.0, 200.0),             # mm acumulados em 7 dias
    "calor":        ("temperature_2m_max", 32.0, 40.0),    # °C
}


def _obrigatoria(nome: str) -> str:
    valor = os.getenv(nome)
    if not valor:
        raise RuntimeError(f"Variável '{nome}' não definida no .env")
    return valor


def __getattr__(nome: str) -> str:
    """config.MONGODB_URI / config.DATABASE_URL são lidas no momento do uso."""
    if nome in _OBRIGATORIAS:
        return _obrigatoria(nome)
    raise AttributeError(f"module 'src.config' has no attribute '{nome}'")
