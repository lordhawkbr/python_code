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
from classes.Rank import *
from configs.dbFuncs import *
import ML.arvore_classificacao as AC
import ML.arvore_regressao as AR
import ML.regressao as R

# MAIN FUNC
async def main():
    ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
    # INSTANCIA AS CLASSES
    dbFuncs = manageDB()
    dbFuncs.manageSchema()
    downloadClass = Download(ROOT_DIR)
    url = "https://dados.gov.br/api/publico/conjuntos-dados/inss-comunicacao-de-acidente-de-trabalho-cat1"
    response = requests.urlopen(url)
    if response.code == 200:
        data = json.loads(response.read())
        recursos = data["conjuntoDadosForm"]["recursos"]
        for cat in recursos:
            await asyncio.gather(
                downloadClass.testUrls(
                    cat["link"], cat["titulo"].lower(), cat["formato"].lower()
                )
            )
    else:
        print("O link para o recurso está indisponível!")
<<<<<<< HEAD
    # MONTA DOIS ARQUIVOS CSV COM A JUNCAO DOS DOIS TIPOS DOS MODELOS
    wwfClass = WorkWithFiles(ROOT_DIR)
    await wwfClass.main()
    # EFETUA O PROCESSO ETL E MONTA AS TABELAS FATO E DIMENSAO
=======
    # #MONTA DOIS ARQUIVOS CSV COM A JUNCAO DOS DOIS TIPOS DOS MODELOS
    wwfClass = WorkWithFiles(ROOT_DIR)
    await wwfClass.main()
    # #EFETUA O PROCESSO ETL E MONTA AS TABELAS FATO E DIMENSAO
>>>>>>> 1b32c3a7305eacc3eab4431b8f0efb961ec4eaa4
    ETLMethods = ETL(ROOT_DIR)
    await ETLMethods.main()
    # FAZ A CLASIFICAO DE RISCO COM BASE NA PARTE DO CORPO E OBITO
    rankAcidentes = Rank()
    await rankAcidentes.main()
    # FAZ AS CLASSIFICACOES DOS MODELOS
    # grafAC = AC.ArvoreClass() #Apresenta erro na leitura da coluna Class_Risco por possuir valores continuos
    grafAR = AR.ArvoreRegressao()
    grafR = R.Regressao()
    # grafAC.gerarGrafico() #Apresenta erro na leitura da coluna Class_Risco por possuir valores continuos
    grafAR.gerarGrafico()
    grafR.gerarGrafico()

# EXECUTA A MAIN FUNC
asyncio.run(main())