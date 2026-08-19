import json


class Loading:
    def __init__(self):
        pass

    def load(self, data):
        return data

    def salvar_json(self, dados, caminho_arquivo):
        with open(caminho_arquivo, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)
