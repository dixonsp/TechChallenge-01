from enum import Enum
import json
import ast

class eTipo(str, Enum): # enumerador para limitar os Negocios para as constantes
    PRODUCAO = {"codigo": 2, "nome": "Producao", "subtipo":None}
    PROCESSAMENTO = {"codigo": 3, "nome":"Processamento", "subtipo":[
    {"codigo":1, "nome":"Viníferas"}
    , {"codigo":2, "nome":"Americanas e híbridas"}
    , {"codigo":3, "nome":"Uvas de mesa"}
    , {"codigo":4, "nome":"Sem classificação"}
    ]}
    COMERCIALIZACAO = {"codigo": 4, "nome":"Comercializacao", "subtipo":None}
    IMPORTACAO = {"codigo":5, "nome":"Importacao", "subtipo":[
        {"codigo":1, "nome":"Vinhos de mesa"}
        , {"codigo":2, "nome":"Espumantes"}
        , {"codigo":3, "nome":"Uvas frescas"}
        , {"codigo":4, "nome":"Uvas passas"}
        , {"codigo":5, "nome":"Suco de uva"}
        ]}
    EXPORTACAO = {"codigo":6, "nome":"Exportacao", "subtipo":[
    {"codigo":1, "nome":"Vinhos de mesa"}
    , {"codigo":2, "nome":"Espumantes"}
    , {"codigo":3, "nome":"Uvas frescas"}
    , {"codigo":4, "nome":"Suco de uva"}
    ]}

    def __init__(self, nome):
        super().__init__()
        self.nome=nome
        return self

    def __enter__(self):
        return self

    def __exit__(self):
        return False  # Re-raise exceptions, if any
    
"""
    A classe Negocio define o tipo de análise (Produção, Processamento, Comercialização, Importação, Exportação)
"""
class Negocio():
    
    def __init__(self, negocio:eTipo):
        tipo = ast.literal_eval(negocio.value)
        self.codigo=tipo['codigo']
        self.nome=tipo['nome']
        self.tipo=tipo['nome']
        self.subtipo=tipo['subtipo']

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print(f"Tipo de exceção: {exc_type}")
            print(f"Valor da exceção: {exc_val}")
            print(f"Traceback da exceção: {exc_tb}")
        return False  # Re-raise exceptions, if any