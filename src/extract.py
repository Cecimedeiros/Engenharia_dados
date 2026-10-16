import requests
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi

class Extract:
    CIDADES: dict[str, dict[str, float]] = {
        "sao_paulo": {"latitude": -23.5505, "longitude": -46.6333},
        "rio_de_janeiro": {"latitude": -22.9068, "longitude": -43.1729},
        "recife": {"latitude": -8.0492, "longitude": -34.9231}
    }

    def __init__(self):
        """Inicializa os valores fixos, carrega variáveis do .env e inicializa conexões com o Mongo."""
        load_dotenv()

        self.base_url: str = "https://api.open-meteo.com/v1/forecast"
        self.uri = os.getenv("MONGODB_URI")
        self.client = MongoClient(self.uri, server_api=ServerApi("1"))
        

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
        
        print("Dados diários referentes à temperatura e precitação da cidade extraídos com sucesso! ✅")
        print("--------------------------")
        return response.json()
    
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

        return response.json()
    
    def get_from_mongo(self, db_name: str, collection: str) -> dict:
        """Recupera o registro mais recente salvo no MongoDB Atlas.

        Parameters:
        database_name : str
            Nome do banco de dados no Mongo.
        collection_name : str
            Nome da coleção (ex: 'WEATHER_DAILY').

        Returns:
        dict
            Dicionário com os dados brutos sem a chave internal '_id'.
        """
        db = self.client[db_name]
        coll = db[collection]

        # Busca o documento mais recente adicionado
        data = coll.find_one(sort=[("_id", -1)])

        if not data:
            raise ValueError(f"Nenhum dado encontrado na coleção {collection}")

        data.pop("_id", None)

        return data

