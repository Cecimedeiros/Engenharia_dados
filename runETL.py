import pandas as pd
from src.extract import Extract
from src.load import Loading
from src.transform import Transform

def main():
    extractor = Extract()
    loader = Loading()
    transformation = Transform()

    dfs_daily = []
    dfs_hourly = []

    try:
        for cidade in Extract.CIDADES:
            # daily
            brutos_daily = extractor.extract_daily_forecast(cidade=cidade)
            loader.load_mongo(brutos_daily, "OPEN_METEO", "WEATHER_DAILY")
            daily_mongo = extractor.get_from_mongo("OPEN_METEO", "WEATHER_DAILY", cidade)
            dfs_daily.append(transformation.transform(daily_mongo, nome_tabela="forecast_7days_daily", cidade=cidade))

            # hourly
            brutos_hourly = extractor.extract_hourly_forecast(cidade=cidade)
            loader.load_mongo(brutos_hourly, "OPEN_METEO", "WEATHER_HOURLY")
            hourly_mongo = extractor.get_from_mongo("OPEN_METEO", "WEATHER_HOURLY", cidade)
            dfs_hourly.append(transformation.transform(hourly_mongo, nome_tabela="forecast_7days_hourly", cidade=cidade))

        df_daily = pd.concat(dfs_daily, ignore_index=True)
        df_hourly = pd.concat(dfs_hourly, ignore_index=True)

        loader.load_sqlite(df_daily, "forecast_7days_daily")
        loader.load_neon(df_daily, table_name="forecast_7days_daily")

        loader.load_sqlite(df_hourly, "forecast_7days_hourly")
        loader.load_neon(df_hourly, table_name="forecast_7days_hourly")
    finally:
        loader.close_connections()


if __name__ == "__main__":
    main()