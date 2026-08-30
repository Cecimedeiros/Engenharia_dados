from src.extract import Extract
from src.load import Loading
from src.transform import Transform

if __name__ == "__main__":

    """
        Script de ETL para testar os diferentes fluxos (diário e o de horário)
    
    """
    extractor = Extract()
    loader = Loading()
    transformation = Transform()

    dados_brutos_daily = extractor.extract_daily_forecast(cidade="sao_paulo")
    loader.load_mongo(dados_brutos_daily, "OPEN_METEO", "WEATHER_DAILY")
    
    dados_tratados_daily = transformation.transform(dados_brutos_daily)
    loader.load_sqlite(dados_tratados_daily, "forecast_7days_daily")

    dados_brutos_hourly = extractor.extract_hourly_forecast(cidade="sao_paulo")
    loader.load_mongo(dados_brutos_hourly, "OPEN_METEO", "WEATHER_HOURLY")
    
    dados_tratados_hourly = transformation.transform(dados_brutos_hourly)
    loader.load_sqlite(dados_tratados_hourly, "forecast_7days_hourly")

    loader.close_connections()