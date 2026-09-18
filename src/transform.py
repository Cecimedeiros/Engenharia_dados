import pandas as pd
from unidecode import unidecode

class Transform:

    def __init__(self):
        pass

    def transform(self, raw_data: dict, nome_tabela: str, cidade: str) -> pd.DataFrame:
        """O método recebe o dado bruto extraído da API e limpa/organiza em um DataFrame.
        
        Parameters:
        raw_data : dict
            Dicionário de dados brutos vindo da etapa de Extract.

        Returns:
        pd.DataFrame
            DataFrame estruturado, limpo e organizado.

        """
        if "daily" in raw_data:
            data_payload = raw_data["daily"]

        else:
            data_payload = raw_data["hourly"]

        df = pd.DataFrame(data_payload)

        city_limpa = unidecode(cidade).lower().replace(" ", "_")
        df["city"] = city_limpa

        if nome_tabela == "forecast_7days_hourly" and "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"])

        df = df.dropna().reset_index(drop=True)

        print("Dados transformados com sucesso! ✅")
        print("--------------------------")

        return df
        


