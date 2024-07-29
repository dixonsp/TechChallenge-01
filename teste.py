import glob
from pathlib import Path
from model.negocio import eTipo, eSubTipo
from fastapi import HTTPException

def processa_bronze(tipo:eTipo, path:str):
    caminho_relativo = Path(path)

    # Padrão de arquivo com máscara
    padrao = f"{tipo.nome}_*.parquet"

    # Buscar arquivos que correspondem ao padrão
    arquivos = glob.glob(str(caminho_relativo / padrao))

    # Verificar se algum arquivo foi encontrado
    if arquivos:
        if len(arquivos) > 1:
            raise HTTPException(status_code=500, detail=f"Mais de 1 arquivo: `{tipo.nome}` encontrado.")
        return arquivos[0]
    else:
        return None

print(processa_bronze(eTipo.PRODUCAO, "data/parquet/bronze"))