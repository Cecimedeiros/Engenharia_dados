from src.extract import Extract
from src.load import Loading
from src.transform import Transform

def main():
    extractor = Extract()
    loader = Loading()
    transformation = Transform()

    cidade_escolhida = "rio_de_janeiro" 
    
    dados_brutos_daily = extractor.extract_daily_forecast(cidade=cidade_escolhida)
    loader.load_mongo(dados_brutos_daily, "OPEN_METEO", "WEATHER_DAILY")

    dados_daily_mongo = extractor.get_from_mongo("OPEN_METEO", "WEATHER_DAILY")
    dados_tratados_daily = transformation.transform(dados_daily_mongo, nome_tabela="forecast_7days_daily", cidade=cidade_escolhida)
    loader.load_sqlite(dados_tratados_daily, "forecast_7days_daily")
    loader.load_neon(dados_tratados_daily, table_name="forecast_7days_daily")
    
    dados_brutos_hourly = extractor.extract_hourly_forecast(cidade=cidade_escolhida)
    dados_hourly_mongo = loader.load_mongo(dados_brutos_hourly, "OPEN_METEO", "WEATHER_HOURLY")

    dados_hourly_mongo = extractor.get_from_mongo("OPEN_METEO", "WEATHER_HOURLY")    
    dados_tratados_hourly = transformation.transform(dados_hourly_mongo, nome_tabela="forecast_7days_hourly", cidade=cidade_escolhida)
    loader.load_sqlite(dados_tratados_hourly, "forecast_7days_hourly")
    loader.load_neon(dados_tratados_hourly, table_name="forecast_7days_hourly")
    
    loader.close_connections()

if __name__ == "__main__":
    main()