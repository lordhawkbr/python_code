# BIBLIOTECAS
from ClassDownload import *
from ClassWorkWithFiles import *
from ETL import *
from datetime import datetime
import asyncio
import os
import sqlalchemy as sa
import dbConfig as dbC
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


def insertLog(text):
    engine = sa.create_engine('%s+%s://%s:%s@%s:%i/%s'%(dbC.dbType, dbC.driver, dbC.dbUser, dbC.dbPass,dbC.dbHost,dbC.dbPort, dbC.dbName),pool_size=0)
    conn = engine.connect()
    try:
        insertLogSQL = sa.text(f"INSERT INTO ft_logs (data_evento,hora_evento,evento) values ('{datetime.today().strftime('%d/%m/%Y')}','{datetime.today().strftime('%H:%M:%S')}','{text}');")
        if conn.execution_options(autocommit=False).execute(insertLogSQL):
            print(text)
            conn.commit()
    except Exception as E:
        print(E)

# MAIN FUNC
async def main():
    insertLog("Iniciado processo de busca e download dos arquivos!")
    # INSTANCIA A CLASSE DOWNLOAD
    downloadClass = Download(ROOT_DIR)
    wwfClass = WorkWithFiles(ROOT_DIR)
    ETLMethods = ETL(ROOT_DIR, "dbpuc")

    # TESTA A URL E INICIA O DOWNLOAD AUTOMATICAMENTE
    for year in range(2021, atualYear):
        for month, mes in enumerate(arrMonthSTR):
            if month - 1 < len(arrMonthSTR) and month + 1 < len(arrMonthSTR):
                await asyncio.gather(downloadClass.testUrls(f"cat-{arrMonthSTR[month - 1]}-{arrMonthSTR[month]}-{arrMonthSTR[month + 1]}-{year}.csv"))
                await asyncio.gather(downloadClass.testUrls(f"cat-comp-{arrMonthSTR[month - 1]}{arrMonthSTR[month]}{arrMonthSTR[month + 1]}-{year}.csv"))
                await asyncio.gather(downloadClass.testUrls(f"cat{year - 1}-comp{arrMonthsINT[month - 1]}-{arrMonthsINT[month]}-{arrMonthsINT[month + 1]}-{year}.csv"))
                await asyncio.gather(downloadClass.testUrls(f"cat-comp{arrMonthsINT[month - 1]}-{arrMonthsINT[month]}-{arrMonthsINT[month + 1]}-{year}.csv"))
                await asyncio.gather(downloadClass.testUrls(f"cat-competencia-{arrMonthsINT[month - 1]}-{arrMonthsINT[month]}-{arrMonthsINT[month + 1]}-{year}.csv"))
                await asyncio.gather(downloadClass.testUrls(f"cat-comp-{arrMonthsINT[month - 1]}-{arrMonthsINT[month]}-{arrMonthsINT[month + 1]}-{year}.zip"))
                await asyncio.gather(downloadClass.testUrls(f"D.SDA.PDA.005.CAT.{year}{arrMonthsINT[month]}.ZIP"))

    # MONTA DOIS ARQUIVOS CSV COM A JUNCAO DOS DOIS TIPOS DOS MODELOS
    await wwfClass.main()

    # EFETUA O PROCESSO ETL E MONTA AS TABELAS FATO E DIMENSAO
    await ETLMethods.main()

# EXECUTA A MAIN FUNC
asyncio.run(main())