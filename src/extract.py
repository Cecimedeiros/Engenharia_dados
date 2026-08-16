import requests

"""Adaptem a URL da API (ok) e o método criado na aula para que a solução permita escolher e 
   baixar as diferentes variáveis e categorias de sexo apresentadas acima.

   A ideia é evitar a criação de um código diferente para cada série. A mesma solução deve ser reutilizada, 
   alterando apenas os parâmetros necessários para realizar cada consulta.
"""


class Extract:

    def __init__(self):
        pass

    def extracao(self, variavel="4096|4099|12466", sexo="6794|4|5"):
        """
        Variáveis:
            4099: Taxa de desocupação, na semana de referência, das pessoas de 14 anos ou mais de idade;
            4096: Taxa de participação na força de trabalho, na semana de referência, das pessoas de 14 anos ou mais de idade;
            12466: Taxa de informalidade das pessoas de 14 anos ou mais de idade ocupadas na semana de referência.

        Sexo:
            6794: Total;
            4: Homens;
            5: Mulheres.
        """
        url = f"https://servicodados.ibge.gov.br/api/v3/agregados/4093/periodos/201201-202602/variaveis/{variavel}?localidades=N3[26]&classificacao=2[{sexo}]"
        response = requests.get(url)
        data = response.json()
        return data
