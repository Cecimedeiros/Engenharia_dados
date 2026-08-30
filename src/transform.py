import pandas as pd

class Transform:

    def __init__(self):
        pass

    def transform(self, raw_data: dict) -> pd.DataFrame:
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

        if "date" in df.columns: 
            df["date"] = df["date"].dt.date

        print("Dados transformados com sucesso! ✅")
        print("--------------------------")

        return df
        


