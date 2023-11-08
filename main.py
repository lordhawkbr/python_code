# BIBLIOTECAS
import asyncio
import os
import urllib.request as requests
import json
from dotenv import load_dotenv
load_dotenv()

# CLASSES
from classes.Download import *
from classes.WorkWithFiles import *
from classes.ETL import *
from configs.dbFuncs import *

# MAIN FUNC
async def main():
    # INSTANCIA AS CLASSES
    downloadClass = Download()
    wwfClass = WorkWithFiles()
    dbFuncs = manageDB()
    dbFuncs.makeSchema()
    ETLMethods = ETL()
    url = "https://dados.gov.br/api/publico/conjuntos-dados/inss-comunicacao-de-acidente-de-trabalho-cat1"
    response = requests.urlopen(url)
    if response.code == 200:
        data = json.loads(response.read())
        recursos = data["conjuntoDadosForm"]["recursos"]
        for cat in recursos:
            if cat["formato"].lower() == "zip":
                await asyncio.gather(
                    downloadClass.testUrls(
                        cat["link"], cat["titulo"], cat["formato"].lower()
                    )
                )
    else:
        print("O link para o recurso está indisponível!")
    # MONTA DOIS ARQUIVOS CSV COM A JUNCAO DOS DOIS TIPOS DOS MODELOS
    await wwfClass.main()
    # EFETUA O PROCESSO ETL E MONTA AS TABELAS FATO E DIMENSAO
    await ETLMethods.main()
# EXECUTA A MAIN FUNC
asyncio.run(main())