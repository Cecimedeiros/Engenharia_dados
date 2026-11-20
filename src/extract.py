import requests
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from src import config
from datetime import datetime, timezone


class Extract:
    # Localidades consultadas: um município-polo por mesorregião de Pernambuco (IBGE).
    #
    # A Open-Meteo responde por ponto (latitude/longitude), não por área, então a
    # previsão do município-polo representa a região inteira. Cada localidade tem:
    # * mesorregiao -> divisão oficial do IBGE;
    # * regiao      -> região usada no resolve-app (filtro do produto).
    #
    # Casos especiais:
    # * Sertão: o IBGE divide em Sertão e São Francisco, por isso tem dois pontos.
    # * Fernando de Noronha: para o IBGE é da mesorregião Metropolitana do Recife,
    #   mas é um arquipélago a ~350 km da costa, com clima próprio. Ganha ponto próprio.
    #
    # Esse dicionário também alimenta a tabela dim_city no Neon.
    CIDADES: dict[str, dict] = {
        "recife": {
            "nome": "Recife", "uf": "PE",
            "mesorregiao": "Metropolitana do Recife", "regiao": "RMR",
            "latitude": -8.0492, "longitude": -34.9231,
        },
        "vitoria_de_santo_antao": {
            "nome": "Vitória de Santo Antão", "uf": "PE",
            "mesorregiao": "Mata Pernambucana", "regiao": "Zona da Mata",
            "latitude": -8.1181, "longitude": -35.2914,
        },
        "caruaru": {
            "nome": "Caruaru", "uf": "PE",
            "mesorregiao": "Agreste Pernambucano", "regiao": "Agreste",
            "latitude": -8.2760, "longitude": -35.9819,
        },
        "serra_talhada": {
            "nome": "Serra Talhada", "uf": "PE",
            "mesorregiao": "Sertão Pernambucano", "regiao": "Sertão",
            "latitude": -7.9858, "longitude": -38.2958,
        },
        "petrolina": {
            "nome": "Petrolina", "uf": "PE",
            "mesorregiao": "São Francisco Pernambucano", "regiao": "Sertão",
            "latitude": -9.3891, "longitude": -40.5030,
        },
        "fernando_de_noronha": {
            "nome": "Fernando de Noronha", "uf": "PE",
            "mesorregiao": "Metropolitana do Recife", "regiao": "Fernando de Noronha",
            "latitude": -3.8547, "longitude": -32.4247,
        },
    }

    BASE_URL: str = "https://api.open-meteo.com/v1/forecast"

    # Variáveis pedidas à API, por tipo de previsão.
    # Também definem o schema (contrato) que o Spark usa para ler o bronze.
    VARIAVEIS: dict[str, list[str]] = {
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "uv_index_max",
            "precipitation_sum",
            "rain_sum",
            "precipitation_probability_max",
            "wind_speed_10m_max",
            "wind_gusts_10m_max",
        ],
        "hourly": [
            "temperature_2m",
            "precipitation_probability",
            "precipitation",
            "wind_speed_10m",
            "wind_gusts_10m",
        ],
    }

    # Dias passados trazidos junto com a previsão: dão histórico ao banco e
    # permitem calcular a chuva acumulada (72h / 7 dias) já no primeiro dia.
    PAST_DAYS: int = 7
    FORECAST_DAYS: int = 7

    def __init__(self):
        """Inicializa o extrator. A conexão com o Mongo só é aberta se for usada."""
        self._client: MongoClient | None = None

    @property
    def client(self) -> MongoClient:
        if self._client is None:
            self._client = MongoClient(config.MONGODB_URI, server_api=ServerApi("1"))
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def extract_forecast(self, cidade: str, tipo: str) -> dict:
        """Busca a previsão (diária ou horária) de uma cidade na API Open-Meteo.

        Parameters
        ----------
        cidade : str
            Chave da cidade em Extract.CIDADES (ex: 'recife').
        tipo : str
            'daily' ou 'hourly'.

        Returns
        -------
        dict
            Resposta JSON bruta da API, acrescida de 'cidade' e 'data_extracao' (UTC).
        """
        cidade_limpa = cidade.lower().strip()
        if cidade_limpa not in self.CIDADES:
            opcoes = ", ".join(self.CIDADES.keys())
            raise ValueError(f"Cidade '{cidade}' inválida. Escolha uma das opções: {opcoes}")
        if tipo not in self.VARIAVEIS:
            raise ValueError(f"Tipo '{tipo}' inválido. Use 'daily' ou 'hourly'.")

        info = self.CIDADES[cidade_limpa]
        params = {
            "latitude": info["latitude"],
            "longitude": info["longitude"],
            tipo: self.VARIAVEIS[tipo],
            "timezone": "America/Sao_Paulo",
            "past_days": self.PAST_DAYS,
            "forecast_days": self.FORECAST_DAYS,
        }

        response = requests.get(self.BASE_URL, params=params, timeout=10)
        response.raise_for_status()

        print(f"Dados '{tipo}' de {cidade_limpa} extraídos com sucesso! ✅")
        print("--------------------------")

        dados = response.json()
        dados["cidade"] = cidade_limpa
        dados["data_extracao"] = datetime.now(timezone.utc)
        return dados

    def extract_daily_forecast(self, cidade: str) -> dict:
        """Previsão diária (temperatura, UV, chuva e vento)."""
        return self.extract_forecast(cidade, "daily")

    def extract_hourly_forecast(self, cidade: str) -> dict:
        """Previsão horária (temperatura, chuva e vento)."""
        return self.extract_forecast(cidade, "hourly")

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
