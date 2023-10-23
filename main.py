# BIBLIOTECAS
from datetime import datetime
import asyncio
import os
import sqlalchemy as sa
# CLASSES
from myClasses.ClassDownload import *
from myClasses.ClassWorkWithFiles import *
from myClasses.ETL import *
from configs.dbConfig import *
from configs.dbFuncs import *
# GLOBAL VARS
arrMonthSTR = [
    "jan",
    "fev",
    "mar",
    "abr",
    "mai",
    "jun",
    "jul",
    "ago",
    "set",
    "out",
    "nov",
    "dez",
]
arrMonthsINT = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
atualYear = datetime.now().year + 1
validUrls = []
ROOT_DIR = os.path.abspath(os.curdir)

# MAIN FUNC
async def main():
    # DEFINA O NOME DO SCHEMA / DB A SER UTILIZADO
    schema = "dbpuc"
    # INSTANCIA AS CLASSES
    downloadClass = Download(ROOT_DIR, schema)
    wwfClass = WorkWithFiles(ROOT_DIR, schema)
    dbFuncs = manageDB(schema)
    dbFuncs.makeSchema()
    ETLMethods = ETL(ROOT_DIR, schema)
    if len(downloadClass.returnFiles()) < 26:    #28
        # TESTA A URL E INICIA O DOWNLOAD AUTOMATICAMENTE
        for year in range(2023, atualYear+1):
            for month, mes in enumerate(arrMonthSTR):
                if month - 1 < len(arrMonthSTR) and month + 1 < len(arrMonthSTR):
                    # await asyncio.gather(downloadClass.testUrls(f"cat-{arrMonthSTR[month - 1]}-{arrMonthSTR[month]}-{arrMonthSTR[month + 1]}-{year}.csv"))
                    # await asyncio.gather(downloadClass.testUrls(f"cat-comp-{arrMonthSTR[month - 1]}{arrMonthSTR[month]}{arrMonthSTR[month + 1]}-{year}.csv"))
                    # await asyncio.gather(downloadClass.testUrls(f"cat{year - 1}-comp{arrMonthsINT[month - 1]}-{arrMonthsINT[month]}-{arrMonthsINT[month + 1]}-{year}.csv"))
                    # await asyncio.gather(downloadClass.testUrls(f"cat-comp{arrMonthsINT[month - 1]}-{arrMonthsINT[month]}-{arrMonthsINT[month + 1]}-{year}.csv"))
                    # await asyncio.gather(downloadClass.testUrls(f"cat-competencia-{arrMonthsINT[month - 1]}-{arrMonthsINT[month]}-{arrMonthsINT[month + 1]}-{year}.csv"))
                    # await asyncio.gather(downloadClass.testUrls(f"cat-comp-{arrMonthsINT[month - 1]}-{arrMonthsINT[month]}-{arrMonthsINT[month + 1]}-{year}.zip"))
                    await asyncio.gather(downloadClass.testUrls(f"D.SDA.PDA.005.CAT.{year}{arrMonthsINT[month]}.ZIP"))
    else:
        dbFuncs.insertLog("Todos os arquivos já estão baixados, iniciando criação do BD!")
    # MONTA DOIS ARQUIVOS CSV COM A JUNCAO DOS DOIS TIPOS DOS MODELOS
    await wwfClass.main()
    # EFETUA O PROCESSO ETL E MONTA AS TABELAS FATO E DIMENSAO
    await ETLMethods.main()

# EXECUTA A MAIN FUNC
asyncio.run(main())