import sys

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.schedules import RRule

from src.extract import Extract
from src.load import Loading
from src.transform import Transform

CIDADES = Extract.CIDADES
DB_MONGO = "OPEN_METEO"
COLECOES = {"daily": "WEATHER_DAILY", "hourly": "WEATHER_HOURLY"}
TABELAS_FATO = ["forecast_daily", "forecast_hourly", "urban_risk_daily"]


# --- EXTRAÇÃO (API -> bronze) ---

@task(retries=3, retry_delay_seconds=10)
def extract(cidade: str, tipo: str) -> dict:
    logger = get_run_logger()
    logger.info(f"Extraindo previsão '{tipo}' para {cidade}...")
    with Extract() as extractor:
        return extractor.extract_forecast(cidade, tipo)


@task(retries=3, retry_delay_seconds=5, cache_policy=NO_CACHE)
def load_bronze(doc: dict, collection: str) -> None:
    logger = get_run_logger()
    with Loading() as loader:
        loader.load_mongo(doc, DB_MONGO, collection)
    logger.info(f"Bruto de {doc['cidade']} gravado em {DB_MONGO}.{collection} (upsert)")


@task(retries=3, retry_delay_seconds=5)
def read_bronze(collection: str) -> list[dict]:
    logger = get_run_logger()
    logger.info(f"Lendo a carga mais recente de cada cidade em {DB_MONGO}.{collection}")
    with Extract() as extractor:
        return [extractor.get_from_mongo(DB_MONGO, collection, c) for c in CIDADES]


# --- TRANSFORMAÇÃO (Spark: bronze -> prata e ouro) ---

@task(cache_policy=NO_CACHE)
def transform(docs_daily: list[dict], docs_hourly: list[dict]) -> dict[str, list[dict]]:
    logger = get_run_logger()
    with Transform() as st:
        daily = st.silver(docs_daily, "daily").cache()
        hourly = st.silver(docs_hourly, "hourly")
        gold = st.gold_risk(daily)

        # confere bronze vs. prata, cidade a cidade: nenhuma linha pode se perder
        for tipo, docs, df in (("daily", docs_daily, daily), ("hourly", docs_hourly, hourly)):
            contagem = st.contar_por_cidade(df)
            for doc in docs:
                n_bronze = len(doc[tipo]["time"])
                n_prata = contagem.get(doc["cidade"], 0)
                logger.info(f"{tipo} {doc['cidade']}: bronze={n_bronze} | prata={n_prata}")
                if n_bronze != n_prata:
                    raise ValueError(
                        f"{tipo} {doc['cidade']}: perda de registros ({n_bronze} -> {n_prata})"
                    )

        return {
            "forecast_daily": st.to_records(daily),
            "forecast_hourly": st.to_records(hourly),
            "urban_risk_daily": st.to_records(gold),
        }


# --- CARGA (Neon) ---

@task(retries=3, retry_delay_seconds=10)
def prepare_neon() -> None:
    """Cria as tabelas (se não existirem) e grava a dimensão de cidades."""
    logger = get_run_logger()
    dim = [{"city": c, **info} for c, info in CIDADES.items()]
    with Loading() as loader:
        loader.create_schema()
        loader.upsert_neon(dim, "dim_city")
    logger.info(f"dim_city atualizada com {len(dim)} localidades")


@task(retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
def load_neon(registros: list[dict], table_name: str) -> None:
    logger = get_run_logger()
    with Loading() as loader:
        n = loader.upsert_neon(registros, table_name)
    logger.info(f"{n} registros gravados (upsert) no Neon (tabela: {table_name})")


# --- FLUXO PRINCIPAL ---

@flow(name="ETL OpenMeteo Weather Prefect", log_prints=True)
def run_etl():
    # 1. API -> bronze (Mongo)
    for tipo, colecao in COLECOES.items():
        for cidade in CIDADES:
            doc = extract(cidade, tipo)
            load_bronze(doc, colecao)

    # 2. bronze -> Spark (prata + ouro)
    tabelas = transform(read_bronze(COLECOES["daily"]), read_bronze(COLECOES["hourly"]))

    # 3. Neon (upsert)
    prepare_neon()
    for nome in TABELAS_FATO:
        load_neon(tabelas[nome], nome)


# --- AGENDAMENTO ---
# Todo sábado às 9h (horário de Brasília), de 21/11 até 12/12/2026.
AGENDA = RRule(
    "DTSTART:20261121T090000\n"
    "RRULE:FREQ=WEEKLY;BYDAY=SA;BYHOUR=9;BYMINUTE=0;UNTIL=20261212T235959",
    timezone="America/Sao_Paulo",
)


if __name__ == "__main__":
    if "--agendar" in sys.argv:
        # Registra o deployment com a agenda e fica escutando: o processo
        # (e o `prefect server start`) precisam continuar abertos.
        run_etl.serve(
            name="etl-semanal-sabado-9h",
            schedules=[AGENDA],
            description="ETL Open-Meteo -> Mongo -> Spark -> Neon, todo sábado às 9h até 12/12.",
        )
    else:
        run_etl()  # execução única, na hora
