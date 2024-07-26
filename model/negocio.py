from enum import Enum
import ast

from enum import Enum

class eSubTipo(Enum):
    PROCESSAMENTO = [
        {"codigo": 1, "nome": "Viníferas"},
        {"codigo": 2, "nome": "Americanas e híbridas"},
        {"codigo": 3, "nome": "Uvas de mesa"},
        {"codigo": 4, "nome": "Sem classificação"}
    ]

    IMPORTACAO = [
        {"codigo": 1, "nome": "Vinhos de mesa"},
        {"codigo": 2, "nome": "Espumantes"},
        {"codigo": 3, "nome": "Uvas frescas"},
        {"codigo": 4, "nome": "Uvas passas"},
        {"codigo": 5, "nome": "Suco de uva"}
    ]

    EXPORTACAO = [
        {"codigo": 1, "nome": "Vinhos de mesa"},
        {"codigo": 2, "nome": "Espumantes"},
        {"codigo": 3, "nome": "Uvas frescas"},
        {"codigo": 4, "nome": "Suco de uva"}
    ]

    def get_by_codigo(self, codigo):
        for item in self.value:
            if item["codigo"] == codigo:
                return item
        return None

class eTipo(Enum):
    PRODUCAO = {"codigo": 2, "nome": "Producao", "subtipo": None}
    PROCESSAMENTO = {"codigo": 3, "nome": "Processamento", "subtipo": eSubTipo.PROCESSAMENTO}
    COMERCIALIZACAO = {"codigo": 4, "nome": "Comercializacao", "subtipo": None}
    IMPORTACAO = {"codigo": 5, "nome": "Importacao", "subtipo": eSubTipo.IMPORTACAO}
    EXPORTACAO = {"codigo": 6, "nome": "Exportacao", "subtipo": eSubTipo.EXPORTACAO}

    @property
    def codigo(self):
        return self.value["codigo"]

    @property
    def nome(self):
        return self.value["nome"]

    @property
    def subtipo(self):
        return self.value["subtipo"]

"""
    A classe Negocio define o tipo de análise (Produção, Processamento, Comercialização, Importação, Exportação)
"""
class Negocio():
    
    def __init__(self, negocio:eTipo):
        self.codigo = negocio.codigo
        self.tipo = negocio.nome
        self.subtipo = negocio.subtipo

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print(f"Tipo de exceção: {exc_type}")
            print(f"Valor da exceção: {exc_val}")
            print(f"Traceback da exceção: {exc_tb}")
        return False  # Re-raise exceptions, if any