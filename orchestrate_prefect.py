import pandas as pd
from prefect import flow, task, get_run_logger
from src.extract import Extract
from src.load import Loading
from src.transform import Transform


# --- TAREFAS DE EXTRAÇÃO ---

@task(retries=3, retry_delay_seconds=10)
def extract_daily(cidade: str) -> dict:
    logger = get_run_logger()
    extractor = Extract()
    logger.info(f"Extraindo previsão diária para {cidade}...")
    return extractor.extract_daily_forecast(cidade=cidade)

@task(retries=3, retry_delay_seconds=10)
def extract_hourly(cidade: str) -> dict:
    logger = get_run_logger()
    extractor = Extract()
    logger.info(f"Extraindo previsão horária para {cidade}...")
    return extractor.extract_hourly_forecast(cidade=cidade)

@task(retries=3, retry_delay_seconds=5)
def get_from_mongo(db_name: str, collection_name: str, cidade: str):
    logger = get_run_logger()
    extractor = Extract()
    logger.info(f"Buscando carga mais recente de {cidade} em {db_name}.{collection_name}")
    return extractor.get_from_mongo(db_name, collection_name, cidade)


# --- TAREFAS DE TRANSFORMAÇÃO ---

@task
def transform_data(raw_data, nome_tabela: str, cidade: str):
    logger = get_run_logger()
    transformer = Transform()
    logger.info(f"Transformando dados para a tabela {nome_tabela} ({cidade})...")
    df_tratado = transformer.transform(raw_data, nome_tabela=nome_tabela, cidade=cidade)

    # confere bronze vs. final
    chave = "daily" if "daily" in raw_data else "hourly"
    n_bronze = len(raw_data[chave]["time"])
    logger.info(f"{cidade}: bronze={n_bronze} | final={len(df_tratado)}")
    if n_bronze != len(df_tratado):
        raise ValueError(f"{cidade}: perda de registros ({n_bronze} -> {len(df_tratado)})")

    return df_tratado


# --- TAREFAS DE CARGA ---

@task(retries=3, retry_delay_seconds=5)
def load_mongo(data, db_name: str, collection_name: str):
    logger = get_run_logger()
    loader = Loading()
    loader.load_mongo(data, db_name, collection_name)
    logger.info(f"Dados brutos inseridos no MongoDB ({db_name}.{collection_name})")

@task(retries=3, retry_delay_seconds=10)
def load_sqlite(df, table_name: str):
    logger = get_run_logger()
    loader = Loading()
    loader.load_sqlite(df, table_name)
    logger.info(f"{len(df)} registros inseridos no SQLite (tabela: {table_name})")

@task(retries=3, retry_delay_seconds=10)
def load_neon(df, table_name: str):
    logger = get_run_logger()
    loader = Loading()
    loader.load_neon(df, table_name=table_name)
    logger.info(f"{len(df)} registros inseridos no Neon Postgres (tabela: {table_name})")

@task
def close_connections():
    logger = get_run_logger()
    loader = Loading()
    loader.close_connections()
    logger.info("Conexões encerradas com sucesso.")


# --- FLUXO PRINCIPAL ---

@flow(name="ETL OpenMeteo Weather Prefect", log_prints=True)
def run_etl():
    dfs_daily, dfs_hourly = [], []

    try:
        for cidade in Extract.CIDADES:
            # Daily
            brutos_daily = extract_daily(cidade=cidade)
            load_mongo(brutos_daily, "OPEN_METEO", "WEATHER_DAILY")
            daily_mongo = get_from_mongo("OPEN_METEO", "WEATHER_DAILY", cidade)
            dfs_daily.append(
                transform_data(daily_mongo, nome_tabela="forecast_7days_daily", cidade=cidade)
            )

            # Hourly
            brutos_hourly = extract_hourly(cidade=cidade)
            load_mongo(brutos_hourly, "OPEN_METEO", "WEATHER_HOURLY")
            hourly_mongo = get_from_mongo("OPEN_METEO", "WEATHER_HOURLY", cidade)
            dfs_hourly.append(
                transform_data(hourly_mongo, nome_tabela="forecast_7days_hourly", cidade=cidade)
            )

        # Carrega as 3 cidades juntas
        df_daily = pd.concat(dfs_daily, ignore_index=True)
        df_hourly = pd.concat(dfs_hourly, ignore_index=True)

        load_sqlite(df_daily, "forecast_7days_daily")
        load_neon(df_daily, table_name="forecast_7days_daily")

        load_sqlite(df_hourly, "forecast_7days_hourly")
        load_neon(df_hourly, table_name="forecast_7days_hourly")
    finally:
        close_connections()


if __name__ == "__main__":
    run_etl()