# BIBLIOTECAS
import os
import aiofiles
import asyncio
import csv
from dotenv import load_dotenv
load_dotenv()
# CLASSES
from configs.dbFuncs import *

newColumnsM1 = [
    "Agente_Causador_Acidente",
    "Data_Acidente",
    "CBO_1",
    "CBO",
    "CID_10_1",
    "CID_10",
    "CNAE2_0_Empregador",
    "CNAE2_0_Empregador_1",
    "Emitente_CAT",
    "Especie_do_beneficio",
    "Filiacao_Segurado",
    "Indica_obito_Acidente",
    "Munic_Empr",
    "Natureza_da_Lesao",
    "Origem_de_Cadastramento_CAT",
    "Parte_Corpo_Atingida",
    "Sexo",
    "Tipo_do_Acidente",
    "UF_Munic_Acidente",
    "UF_Munic_Empregador",
    "Data_Afastamento",
    "Data_Despacho_Beneficio",
    "Data_Acidente_1",
    "Data_Nascimento",
    "Data_Emissao_CAT"
]
newColumnsM2 = [
    "Agente_Causador_Acidente",
    "Data_Acidente",
    "CBO",
    "CID_10",
    "CNAE2_0_Empregador",
    "CNAE2_0_Empregador_1",
    "Emitente_CAT",
    "Especie_do_beneficio",
    "Filiacao_Segurado",
    "Indica_obito_Acidente",
    "Munic_Empr",
    "Natureza_da_Lesao",
    "Origem_de_Cadastramento_CAT",
    "Parte_Corpo_Atingida",
    "Sexo",
    "Tipo_do_Acidente",
    "UF_Munic_Acidente",
    "UF_Munic_Empregador",
    "Data_Afastamento",
    "Data_Despacho_Beneficio",
    "Data_Acidente_1",
    "Data_Nascimento",
    "Data_Emissao_CAT",
    "CNPJ_CEI_Empregador"
]

class WorkWithFiles:
    
    def __init__(self, ROOT_DIR):
        self.dbFuncs = manageDB()
        self.ROOT_DIR = ROOT_DIR
        self.downloadPath = os.path.join(ROOT_DIR + "/downloads/")
        self.tempFilesPatch = os.path.join(ROOT_DIR + "/downloads/" + "temp/")
        self.M1Path = os.path.join(ROOT_DIR + "/downloads/" + "modelo_1/")
        self.M2Path = os.path.join(ROOT_DIR + "/downloads/" + "modelo_2/")
        self.temp1Path  = os.path.join(self.tempFilesPatch + "/temp_model_1.csv")
        self.temp2Path  = os.path.join(self.tempFilesPatch + "/temp_model_2.csv")
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
    
    def returnFiles(self, pathDir):
        try:
            csvFiles = []
            for root, dirs, files in os.walk(pathDir):
                for file in files:
                    if "dm_" not in file and "ft_temp" not in file:
                        csvFiles.append([os.path.join(root, file), root])
            return csvFiles
        except Exception as E:
            print("Exception returnFiles: " + E)
    
    async def readFiles(self, csvFile, fileNameToSave):
        # DEFINE O ENCODING A SER USADO PARA ABRIR O ARQUIVO
        encodingFile = "UTF-8" if self.isUTF8(csvFile) else "latin-1"
        async with aiofiles.open(
            self.tempFilesPatch + "/" + fileNameToSave, mode="a", encoding="utf-8"
        ) as fileWrite:
            async with aiofiles.open(
                csvFile, mode="r", encoding=encodingFile, newline=None
            ) as file:
                first_line = True
                async for row in file:
                    if first_line:
                        first_line = False
                    else:
                        value = [
                            c.rstrip().replace("  ", "") for c in row.strip().split(";")
                        ]
                        for i in range(len(value)):
                            if value[i].lower() in ["{ñ class}", " {ñ class}", "{ñ", "{ñ class", "", "000000-ignorado", "ignorado", "000000-não informado"]:
                                value[i] = "000-ignorado"
                        await csv.writer(
                            fileWrite,
                            delimiter=";",
                            skipinitialspace=True,
                            lineterminator="\n",
                        ).writerow(value)
    
    def isUTF8(self, fileName):
        data = open(fileName, "rb").read()
        try:
            decoded = data.decode('UTF-8')
        except UnicodeDecodeError:
            return False
        else:
            for ch in decoded:
                if 0xD800 <= ord(ch) <= 0xDFFF:
                    return False
            return True
    
    async def addHeaderM1(self):
        self.dbFuncs.insertLog("Adicionando novo header ao template 1")
        with open(self.temp1Path, "r", encoding="utf-8") as infile:
            reader = list(csv.reader(infile, delimiter=";"))
            reader.insert(0, newColumnsM1)

        with open(self.temp1Path, "w", encoding="utf-8") as outfile:
            writer = csv.writer(outfile, delimiter=";", lineterminator="\n")
            for line in reader:
                writer.writerow(line)

    async def addHeaderM2(self):
        self.dbFuncs.insertLog("Adicionando novo header ao template 2")
        with open(self.temp2Path, "r", encoding="utf-8") as infile:
            reader = list(csv.reader(infile, delimiter=";"))
            reader.insert(0, newColumnsM2)

        with open(self.temp2Path, "w", encoding="utf-8") as outfile:
            writer = csv.writer(outfile, delimiter=";", lineterminator="\n")
            for line in reader:
                writer.writerow(line)

    # async def addHeader(self):
        # for csvFile in self.returnFiles(self.tempFilesPatch):
        #     if "temp_model_1" in csvFile[0]:
        #         print(f"Editing Header from file: {csvFile[0]}")
        #         fieldNames = newColumnsM1
        #     else:
        #         print(f"Editing Header from file: {csvFile[0]}")
        #         fieldNames = newColumnsM2

        #     with open(csvFile[0], "r", encoding="utf-8") as infile:
        #         reader = list(csv.reader(infile, delimiter=";"))
        #         reader.insert(0, fieldNames)

        #     with open(csvFile[0], "w", encoding="utf-8") as outfile:
        #         writer = csv.writer(outfile, delimiter=";", lineterminator="\n")
        #         for line in reader:
        #             writer.writerow(line)
    
    async def main(self):
        totalFiles = len(self.returnFiles(self.downloadPath))
        self.dbFuncs.insertLog(
            "Iniciado processo de junção dos arquivos p/ montagem dos Templates!"
        )
        for pos, csvFile in enumerate(self.returnFiles(self.downloadPath)):
            if os.path.abspath(csvFile[1]) == os.path.abspath(self.M1Path):
                print(f"Reading: {csvFile[0]}")
                await asyncio.gather(self.readFiles(csvFile[0], "temp_model_1.csv"))
                self.dbFuncs.insertLog(
                    f"Lendo arquivo {int(pos)+1}/{totalFiles}"
                )
            else:
                print(f"Reading: {csvFile[0]}")
                await asyncio.gather(self.readFiles(csvFile[0], "temp_model_2.csv"))
                self.dbFuncs.insertLog(
                    f"Lendo arquivo {int(pos)+1}/{totalFiles}"
                )
        # ACESSA OS TEMPLATES E ADICIONA UM NOVO HEADER
        await self.addHeaderM1()
        await self.addHeaderM2()