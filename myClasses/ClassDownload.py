# BIBLIOTECAS
import urllib.request as requests
from urllib.error import HTTPError
import os
import asyncio
import csv
import zipfile
from io import BytesIO
# CLASSES
from configs.dbFuncs import *


# VARIAVEIS GLOBAIS
mainUrl = "https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/Plano+2016_2018_Grupos+de+dados/INSS+-+Comunica%C3%A7%C3%A3o+de+Acidente+de+Trabalho+-+CAT/"

class Download:
    def __init__(self, ROOT_DIR, schema):
        self.dbFuncs = manageDB(schema)
        self.ROOT_DIR = ROOT_DIR
        self.downloadPath = os.path.join(ROOT_DIR + "/downloads/")
        self.tempFilesPatch = os.path.join(ROOT_DIR + "/downloads/" + "temp/")
        self.M1Path = os.path.join(ROOT_DIR + "/downloads/" + "modelo_1/")
        self.M2Path = os.path.join(ROOT_DIR + "/downloads/" + "modelo_2/")
        self.dataframes = os.path.join(ROOT_DIR + "/downloads/" + "dataframes/")
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
            if not os.path.exists(self.dataframes):
                os.makedirs(self.dataframes)
        except Exception as E:
            print("Exception __init__: " + E)

    # TESTA SE A URL É VALIDA E ACESSIVEL
    async def testUrls(self, fileUrl):
        fileExtension = "csv" if "csv" in fileUrl else "zip"
        try:
            response = requests.urlopen(mainUrl + fileUrl)
            if response.code == 200 and fileExtension == "csv":
                print(f"{fileUrl} - Verifing if file not exists and initialing download!")
                await self.downloadFile(fileUrl)
            if response.code == 200 and fileExtension == "zip":
                print(f"{fileUrl} - Verifing if file not exists and initialing download!")
                await self.downloadFile(fileUrl)
        except HTTPError as E:
            # print("Extension not recognized or URL unavailable!")
            return
        except Exception as E:
            print(f"Exception testUrls: {E}")

    # REALIZA O DOWNLOAD DO ARQUIVO SEPARANDO POR PASTA DE ACORDO COM A EXTENSAO
    async def downloadFile(self, fileName):
        self.dbFuncs.insertLog("Iniciado processo de download dos arquivos!")
        try:
            asyncio.get_running_loop()
            # for pos, url in enumerate(validUrl):
            fileUrl = mainUrl + fileName
            fileExtension = "csv" if "csv" in fileName else "zip"

            if fileExtension == "csv":
                if fileName not in self.returnFiles():
                    print(f"File {fileName} not exists, downloading...")
                    if requests.urlretrieve(fileUrl, os.path.join(self.tempFilesPatch, fileName)):
                        await self.moveFiles(fileName)
                else: print(f"File exists, download cancelled!")
            if fileExtension == "zip":
                if await self.ViewZipBeforeDownload(fileUrl) == True:
                    print(f"File {fileName} not exists, downloading...")
                    if requests.urlretrieve(fileUrl, os.path.join(self.tempFilesPatch, fileName)):
                        await self.extractFile(fileName)
                        await self.moveAndDeleteFiles()
                else: print(f"File exists, download cancelled!")
        except Exception as E:
            print("Exception downloadFile: " + E)

    # VISUALIZA O ZIP ANTES DE BAIXAR, CASO ENCONTRE UM ARQUIVO EXISTENTE PULA O DOWNLOAD
    async def ViewZipBeforeDownload(self, fileRequest):
        request = requests.urlopen(fileRequest)
        try:
            myzip = zipfile.ZipFile(BytesIO(request.read()))
            for file in myzip.namelist():
                if "csv" in file and file not in self.returnFiles():
                    return True
        except Exception as E:
            print("Exception ViewZipBeforeDownload: " + E)

    # EXTRAI O ARQUIVO ZIP APOS FEITO O DOWNLOAD
    async def extractFile(self, fileName):
        try:
            with zipfile.ZipFile(
                os.path.join(os.path.join(self.tempFilesPatch + fileName)),
                "r",
            ) as zip:
                file_info_list = zip.infolist()
                for file_info in file_info_list:
                    if (
                        not file_info.filename in self.returnFiles()
                        and "csv" in file_info.filename
                    ):
                        print(f"Extracting file {fileName}")
                        zip.extract(
                            file_info.filename, os.path.join(self.tempFilesPatch)
                        )
                    else:
                        print(f"File {fileName} exist, not necessary unzip.")
        except Exception as E:
            print("Exception extractFile: " + E)

    # RETORNA TODOS OS ARQUIVOS DO DIRETORIO
    def returnFiles(self):
        try:
            tempArr = []
            for root, dirs, files in os.walk(self.downloadPath):
                for name in files:
                    tempArr.append(name)
            return tempArr
        except Exception as E:
            print("Exception returnFiles: " + E)

    # PERCORRE OS ARQUIVOS NA PASTA DOWNLOADS E DIRECIONA PARA OS LOCAIS CORRETOS
    async def moveFiles(self, file):
        filePath = os.path.join(self.tempFilesPatch, file)
        try:
            with open(filePath, mode="r", encoding="latin-1") as csvFile:
                csv_reader = csv.reader(csvFile, delimiter=";")
                # VERIFICA A QUANTIDADE DE COLUNAS(HEADERS)
                if len(next(csv_reader)) == 25:
                    newFilePath = os.path.join(self.M1Path, file)
                else:
                    newFilePath = os.path.join(self.M2Path, file)
            # SE O ARQUIVO JA EXISTIR NO DESTINO, SUBSTITUI PELO NOVO, SE NAO APENAS MOVIMENTA
            if os.path.exists(newFilePath):
                print(f"File {file} moved to folder modelo_1")
                os.remove(newFilePath)
                os.rename(filePath, newFilePath)
            else:
                print(f"File {file} moved to folder modelo_2")
                os.rename(filePath, newFilePath)
        except Exception:
            print("moveFiles: " + Exception)

    # DELETAR ARQUIVOS TEMPORARIOS (ZIPS E DIFERENTES DE CSV)
    async def moveAndDeleteFiles(self):
        try:
            # MOVE OS ARQUIVOS CSV DA PASTA DESCOMPACTADA
            for root, dirs, files in os.walk(self.tempFilesPatch):
                for file in files:
                    if "csv" in file:
                        await self.moveFiles(file)
                    else:
                        # DELETA OS ARQUIVOS QUE NÃO SEJAM CSV
                        print(f"File {file} deleted!")
                        os.remove(os.path.join(self.tempFilesPatch, file))
        except Exception as E:
            print("Exception moveAndDeleteFiles: " + E)
