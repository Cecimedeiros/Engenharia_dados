from prefect import flow, task, get_run_logger
from dotenv import load_dotenv

from src.extract import Extract
from src.load import Loading
from src.transform import Transform

load_dotenv()

# --- TAREFAS DE EXTRAÇÃO ---

@task(retries=3, retry_delay_seconds=10)
def extract_daily(cidade: str) -> dict:
    logger = get_run_logger()
    extractor = Extract()
    logger.info(f"Extraindo previsão diária para {cidade}...")
    data = extractor.extract_daily_forecast(cidade=cidade)
    return data

@task(retries=3, retry_delay_seconds=10)
def extract_hourly(cidade: str) -> dict:
    logger = get_run_logger()
    extractor = Extract()
    logger.info(f"Extraindo previsão horária para {cidade}...")
    data = extractor.extract_hourly_forecast(cidade=cidade)
    return data

@task(retries=3, retry_delay_seconds=5)
def get_from_mongo(db_name: str, collection_name: str):
    logger = get_run_logger()
    extractor = Extract()
    logger.info(f"Buscando dados no MongoDB: {db_name}.{collection_name}")
    return extractor.get_from_mongo(db_name, collection_name)


# --- TAREFAS DE TRANSFORMAÇÃO ---

@task
def transform_data(raw_data, nome_tabela: str, cidade: str):
    logger = get_run_logger()
    transformer = Transform()
    logger.info(f"Transformando dados para a tabela {nome_tabela} ({cidade})...")
    df_tratado = transformer.transform(raw_data, nome_tabela=nome_tabela, cidade=cidade)
    logger.info(f"{len(df_tratado)} registros transformados.")
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
def run_etl(cidade_escolhida: str = "rio_de_janeiro"):
    # 1. Processamento Diário (Daily)
    dados_brutos_daily = extract_daily(cidade=cidade_escolhida)
    load_mongo(dados_brutos_daily, "OPEN_METEO", "WEATHER_DAILY")

    dados_daily_mongo = get_from_mongo("OPEN_METEO", "WEATHER_DAILY")
    dados_tratados_daily = transform_data(
        dados_daily_mongo, 
        nome_tabela="forecast_7days_daily", 
        cidade=cidade_escolhida
    )
    load_sqlite(dados_tratados_daily, "forecast_7days_daily")
    load_neon(dados_tratados_daily, table_name="forecast_7days_daily")

    # 2. Processamento Horário (Hourly)
    dados_brutos_hourly = extract_hourly(cidade=cidade_escolhida)
    load_mongo(dados_brutos_hourly, "OPEN_METEO", "WEATHER_HOURLY")

    dados_hourly_mongo = get_from_mongo("OPEN_METEO", "WEATHER_HOURLY")
    dados_tratados_hourly = transform_data(
        dados_hourly_mongo, 
        nome_tabela="forecast_7days_hourly", 
        cidade=cidade_escolhida
    )
    load_sqlite(dados_tratados_hourly, "forecast_7days_hourly")
    load_neon(dados_tratados_hourly, table_name="forecast_7days_hourly")

    # 3. Fechamento de conexões
    close_connections()


if __name__ == "__main__":
    run_etl()