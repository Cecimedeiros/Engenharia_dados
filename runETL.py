from src.extract import Extract
from src.loading import Loading
import json

class download():

    def __init__(self):
        pass

    def download_data(self, data, caminho_arquivo):
        with open(caminho_arquivo, 'w', encoding='utf-8') as f:
            return json.dump(data, f, ensure_ascii=False)
            

if __name__=="__main__": #fluxo

    dados_prontos = Extract().extracao(variavel="4099", sexo="5")
    carregador = Loading().load(dados_prontos)
    arquivo = download().download_data(carregador, caminho_arquivo = "dados.json")

    print(f"Dados: {carregador}")