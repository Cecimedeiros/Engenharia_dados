from src.extract import Extract
from src.load import Loading


if __name__ == "__main__":  # fluxo

    dados_prontos = Extract().extracao(variavel="4096", sexo="6794")
    carregador = Loading().load(dados_prontos)
    Loading().salvar_json(carregador, "pernambuco.json")

    print(f"Dados: {carregador}")
