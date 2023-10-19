import os
import aiofiles
import asyncio
import csv
import sqlalchemy as sa
from datetime import datetime
import dbConfig as dbC

newColumnsM1 = ["Agente Causador Acidente","Data Acidente","CBO_1","CBO","CID-10_1","CID-10","CNAE2.0 Empregador","CNAE2.0 Empregador_1","Emitente CAT","Especie do beneficio","Filiacao Segurado","Indica obito Acidente","Munic Empr","Natureza da Lesao","Origem de Cadastramento CAT","Parte Corpo Atingida","Sexo","Tipo do Acidente","UF Munic. Acidente","UF Munic. Empregador","Data Afastamento","Data Despacho Beneficio","Data Acidente_1","Data Nascimento","Data Emissao CAT"]
newColumnsM2 = ["Agente Causador Acidente","Data Acidente","CBO","CID-10","CNAE2.0 Empregador","CNAE2.0 Empregador_1","Emitente CAT","Especie do beneficio","Filiacao Segurado","Indica obito Acidente","Munic Empr","Natureza da Lesao","Origem de Cadastramento CAT","Parte Corpo Atingida","Sexo","Tipo do Acidente","UF Munic. Acidente","UF Munic. Empregador","Data Afastamento","Data Despacho Beneficio","Data Acidente_1","Data Nascimento","Data Emissao CAT","CNPJ/CEI Empregador"]

class WorkWithFiles:
    def __init__(self, ROOT_DIR):
        # PARAMETROS DE CONEXAO COM O BANCO
        self.engine = sa.create_engine('%s+%s://%s:%s@%s:%i/%s'%(dbC.dbType, dbC.driver, dbC.dbUser, dbC.dbPass,dbC.dbHost,dbC.dbPort, dbC.dbName),pool_size=0)
        self.conn = self.engine.connect()
        self.ROOT_DIR = ROOT_DIR
        self.downloadPath = os.path.join(ROOT_DIR + "/downloads/")
        self.tempFilesPatch = os.path.join(ROOT_DIR + "/downloads/" + "temp/")
        self.M1Path = os.path.join(ROOT_DIR + "/downloads/" + "modelo_1/")
        self.M2Path = os.path.join(ROOT_DIR + "/downloads/" + "modelo_2/")
        if os.path.exists(self.tempFilesPatch + "/temp_model_1.csv"):
            os.remove(self.tempFilesPatch + "/temp_model_1.csv")
        if os.path.exists(self.tempFilesPatch + "/temp_model_2.csv"):
            os.remove(self.tempFilesPatch + "/temp_model_2.csv")
        try:
            # CRIA AS PASTAS PARA ARMAZENAR OS ARQUIVOS
            if not os.path.exists(self.downloadPath):
                os.makedirs(self.downloadPath)
            if not os.path.exists(self.tempFilesPatch):
                os.makedirs(self.tempFilesPatch)
            if not os.path.exists(self.M1Path):
                os.makedirs(self.M1Path)
            if not os.path.exists(self.M2Path):
                os.makedirs(self.M2Path)
        except Exception as E:
            print("Exception __init__: " + E)

    def insertLog(self, text):
        try:
            insertLogSQL = sa.text(f"INSERT INTO ft_logs (data_evento,hora_evento,evento) values ('{datetime.today().strftime('%d/%m/%Y')}','{datetime.today().strftime('%H:%M:%S')}','{text}');")
            if self.conn.execution_options(autocommit=False).execute(insertLogSQL):
                print(text)
                self.conn.commit()
        except Exception as E:
            print(E)
            
    def returnFiles(self,pathDir):
        try:
            csvFiles = []
            for root, dirs, files in os.walk(pathDir):
                for file in files:
                    csvFiles.append([os.path.join(root, file), root])
            return csvFiles
        except Exception as E:
            print("Exception returnFiles: " + E)

    async def readFiles(self, csvFile, fileNameToSave):
        async with aiofiles.open(
            self.tempFilesPatch + "/" + fileNameToSave, mode="a",encoding="utf-8"
        ) as fileWrite:
            async with aiofiles.open(
                csvFile, mode="r", encoding="latin-1", newline=None
            ) as file:
                first_line = True
                async for row in file:
                    if first_line:
                        first_line = False
                    else:
                        await csv.writer(
                            fileWrite,
                            delimiter=";",
                            skipinitialspace=True,
                            lineterminator="\n",
                        ).writerow([c.rstrip().replace("  ", "") for c in row.strip().split(";")])

    async def addHeader(self):
        for csvFile in self.returnFiles(self.tempFilesPatch):
            if "temp_model_1" in csvFile[0]:
                print(f"Editing Header from file: {csvFile[0]}")
                fieldNames = newColumnsM1
            else:
                print(f"Editing Header from file: {csvFile[0]}")
                fieldNames = newColumnsM2

            with open(csvFile[0], "r", encoding="utf-8") as infile:
                reader = list(csv.reader(infile,delimiter=";"))
                reader.insert(0, fieldNames)

            with open(csvFile[0], "w", encoding="utf-8") as outfile:
                writer = csv.writer(outfile,delimiter=";",lineterminator="\n")
                for line in reader:
                    writer.writerow(line)

    async def main(self):
        self.insertLog("Iniciado processo de junção dos arquivos p/ montagem dos Templates!")
        for pos, csvFile in enumerate(self.returnFiles(self.downloadPath)):
            if os.path.abspath(csvFile[1]) == os.path.abspath(self.M1Path):
                print(f"Reading: {csvFile[0]}")
                await asyncio.gather(self.readFiles(csvFile[0], "temp_model_1.csv"))
                self.insertLog(f"Lendo arquivo {pos}/{len(self.returnFiles(self.downloadPath))} do modelo 1!")
            else:
                print(f"Reading: {csvFile[0]}")
                await asyncio.gather(self.readFiles(csvFile[0], "temp_model_2.csv"))
                self.insertLog(f"Lendo arquivo {pos}/{len(self.returnFiles(self.downloadPath))} do modelo 2!")
        await asyncio.gather(self.addHeader())