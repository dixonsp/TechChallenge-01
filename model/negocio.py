from enum import Enum


class eTipo(Enum): # enumerador para limitar os Negocios para as constantes
    PRODUCAO = {"Codigo": 2, "Nome": "Producao", "Subtipo":None}
    PROCESSAMENTO = {"Codigo": 3, "Nome":"Processamento", "Subtipo":[
    {"Codigo":1, "Nome":"Viníferas"}
    , {"Codigo":2, "Nome":"Americanas e híbridas"}
    , {"Codigo":3, "Nome":"Uvas de mesa"}
    , {"Codigo":4, "Nome":"Sem classificação"}
    ]}
    COMERCIALIZACAO = {"Codigo": 4, "Nome":"Comercializacao", "Subtipo":None}
    IMPORTACAO = {"Codigo":5, "Nome":"Importacao", "Subtipo":[
        {"Codigo":1, "Nome":"Vinhos de mesa"}
        , {"Codigo":2, "Nome":"Espumantes"}
        , {"Codigo":3, "Nome":"Uvas frescas"}
        , {"Codigo":4, "Nome":"Uvas passas"}
        , {"Codigo":5, "Nome":"Suco de uva"}
        ]}
    EXPORTACAO = {"Codigo":6, "Nome":"Exportacao", "Subtipo":[
    {"Codigo":1, "Nome":"Vinhos de mesa"}
    , {"Codigo":2, "Nome":"Espumantes"}
    , {"Codigo":3, "Nome":"Uvas frescas"}
    , {"Codigo":4, "Nome":"Suco de uva"}
    ]}

    def __init__(self, nome):
        super().__init__()
        self.nome=nome
        print(f"Entrando no init de {self.nome}")
        return self

    def __enter__(self):
        print(f"Entrando no contexto de {self.nome}")
        return self

    def __exit__(self):
        print(f"Saindo do contexto de {self.nome}")
        return False  # Re-raise exceptions, if any
    
"""
    A classe Negocio define o tipo de análise (Produção, Processamento, Comercialização, Importação, Exportação)
"""
class Negocio():
    
    def __init__(self, negocio:eTipo):
        self.negocio=negocio.value['Nome']
        self.codigo=negocio.value['Codigo']
        self.nome=negocio.value['Nome']
        self.tipo=negocio.value['Nome']
        self.subtipo=negocio.value['Subtipo']

    def __enter__(self):
        print(f"Entrando no contexto de {self.nome}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"Saindo do contexto de {self.nome}")
        if exc_type:
            print(f"Tipo de exceção: {exc_type}")
            print(f"Valor da exceção: {exc_val}")
            print(f"Traceback da exceção: {exc_tb}")
        return False  # Re-raise exceptions, if any