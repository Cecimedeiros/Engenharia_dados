import requests
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from src import config
from datetime import datetime, timezone

class Extract:
    CIDADES: dict[str, dict[str, float]] = {
        "sao_paulo": {"latitude": -23.5505, "longitude": -46.6333},
        "rio_de_janeiro": {"latitude": -22.9068, "longitude": -43.1729},
        "recife": {"latitude": -8.0492, "longitude": -34.9231}
    }

    def __init__(self):
        """Inicializa a URL base da API e a conexão com o Mongo."""
        self.base_url: str = "https://api.open-meteo.com/v1/forecast"
        self.client = MongoClient(config.MONGODB_URI, server_api=ServerApi("1"))
       
    def extract_daily_forecast(self, cidade: str) -> dict:
        """Busca dados de previsão dos próximos 7 dias da API Open-Meteo.

        Parametros:
        ----------
        cidade : str
            Nome da cidade desejada. Deve ser uma das opções válidas em Extract.CIDADES.

        Returns
        -------
        dict
            Dicionário com a resposta JSON bruta enviada pela API.

        """
        cidade_limpa = cidade.lower().strip()
        if cidade_limpa not in self.CIDADES:
            opcoes = ", ".join(self.CIDADES.keys())
            raise ValueError(f"Cidade '{cidade}' inválida. Escolha uma das opções: {opcoes}")

        coords = self.CIDADES[cidade_limpa]

        params = {
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "uv_index_max",
            ],
            "timezone": "America/Sao_Paulo",
        }

        response = requests.get(self.base_url, params=params, timeout=10)
        response.raise_for_status()
        
        print("ados diários (temperatura e índice UV) extraídos com sucesso! ✅")
        print("--------------------------")
        
        dados = response.json()
        dados["cidade"] = cidade_limpa
        dados["data_extracao"] = datetime.now(timezone.utc)
        return dados
    
    def extract_hourly_forecast(self, cidade: str) -> dict:
        """Busca dados de previsão horária da API Open-Meteo.

        Parameters
        ----------
        cidade : str
            Nome da cidade desejada (ex: 'recife', 'sao_paulo').

        Returns
        -------
        dict
            Dicionário com a resposta JSON bruta enviada pela API.

        """

        cidade_limpa = cidade.lower().strip()
        if cidade_limpa not in self.CIDADES:
            opcoes = ", ".join(self.CIDADES.keys())
            raise ValueError(f"Cidade '{cidade}' inválida. Escolha uma das opções: {opcoes}")
        
        coords = self.CIDADES[cidade_limpa]
        params = {
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "hourly": ["temperature_2m", "precipitation_probability"],
            "timezone": "America/Sao_Paulo",
        }

        response = requests.get(self.base_url, params=params, timeout=10)
        response.raise_for_status()

        print("Dados referentes à temperatura e precitação da cidade, por hora do dia, extraídos com sucesso! ✅")
        print("--------------------------")

        dados = response.json()
        dados["cidade"] = cidade_limpa
        dados["data_extracao"] = datetime.now(timezone.utc)
        return dados
    
    def get_from_mongo(self, db_name: str, collection: str, cidade: str) -> dict:
        """Recupera a carga mais recente de uma cidade no MongoDB Atlas.

        Parameters
        ----------
        db_name : str
            Nome do banco de dados no Mongo.
        collection : str
            Nome da coleção (ex: 'WEATHER_DAILY').
        cidade : str
            Cidade cuja carga mais recente será buscada (ex: 'recife').

        Returns
        -------
        dict
            Dicionário com os dados brutos, sem a chave interna '_id'.
        """
        coll = self.client[db_name][collection]

        # Carga mais recente da cidade, pela data de extração
        data = coll.find_one(
            {"cidade": cidade.lower().strip()},
            sort=[("data_extracao", -1)],
        )

        if not data:
            raise ValueError(f"Nenhum dado de '{cidade}' na coleção {collection}")

        data.pop("_id", None)
        return data

