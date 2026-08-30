import requests

class Extract:
    # Dicionário de classe com opções fechadas de cidades/coordenadas
    CIDADES: dict[str, dict[str, float]] = {
        "sao_paulo": {"latitude": -23.5505, "longitude": -46.6333},
        "rio_de_janeiro": {"latitude": -22.9068, "longitude": -43.1729},
        "recife": {"latitude": -8.0543, "longitude": -34.8813}
    }

    def __init__(self):
        """Inicializa os valores fixos."""
        self.base_url: str = "https://api.open-meteo.com/v1/forecast"

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
        # Validação do parâmetro contra a lista fechada de opções
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
        
        print("Daily data extraídos com sucesso! ✅")
        print("--------------------------")
        return response.json()
    
    def extract_hourly_forecast(self, cidade: str = "sao_paulo") -> dict:
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

        print("Hourly data extraídos com sucesso! ✅")
        print("--------------------------")

        return response.json()
