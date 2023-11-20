# BIBLIOTECAS
import sqlalchemy as sa
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()
import configs.dbFuncs as dbFuncs
import shutil
import pymysql
import psutil

class ETL:
    def __init__(self, ROOT_DIR):
        self.dbFuncs = dbFuncs.manageDB()
        self.useCols = ["Agente_Causador_Acidente", "Data_Acidente", "CBO", "CID_10", "CNAE2_0_Empregador", "CNAE2_0_Empregador_1", "Indica_obito_Acidente", "Munic_Empr", "Natureza_da_Lesao", "Parte_Corpo_Atingida", "Sexo", "Tipo_do_Acidente", "UF_Munic_Acidente", "UF_Munic_Empregador", "Data_Afastamento", "Data_Nascimento"]
        self.ROOT_DIR = ROOT_DIR
        self.engine = sa.create_engine(self.dbFuncs.r_engine())
        self.downloadPath = os.path.join(self.ROOT_DIR + "/downloads/")
        self.tempFilesPatch = os.path.join(self.downloadPath + "temp/")
        self.dataframes = os.path.join(self.downloadPath + "dataframes/")
        self.mainDF = pd.DataFrame()
        self.tempDF = pd.DataFrame()
        systemMemory = psutil.virtual_memory().free / 1024 / 1024 / 1024
        self.chuncksize =  200000 if systemMemory > 4 else 100000
        try:
            if not os.path.exists(self.dataframes):
                os.makedirs(self.dataframes)
            else:
                shutil.rmtree(self.dataframes)
                os.makedirs(self.dataframes)
        except Exception as E:
            print("Exception __init__: " + E)

    def converter_data(self, dt):
        if len(dt) == 6:
            return f"{dt[:4]}/{dt[4:]}"
        if len(dt) >= 10:
            return pd.to_datetime(dt, format="%d/%m/%Y").strftime("%Y/%m")
        else:
            return dt
        
    async def workMainDF(self, dm_to_concat, columnsSelect, columnFilter, op, newColumns, string):
        self.dbFuncs.insertLog(f"Mounting DF {dm_to_concat}")
        fileName = dm_to_concat
        finalDF = pd.DataFrame()
        for i in range(0, len(self.mainDF), self.chuncksize):
            chunk = self.mainDF.iloc[i:i + self.chuncksize]
            if op == 1:
                dm_to_concat = chunk[columnsSelect].str.split(string, n=1, expand=True)
                dm_to_concat.columns = newColumns
            if op == 2:
                if chunk[columnsSelect].str.contains("|".join(["000-ignorado", "000000-Não Informado"])).any():
                    dm_to_concat = chunk[columnsSelect].str.replace("-", " ")
                else:
                    dm_to_concat = chunk[columnsSelect].str.replace(".", "")
                    
                dm_to_concat = dm_to_concat.str.split(string, n=1, expand=True)
                dm_to_concat.columns = newColumns
            if op == 3:
                dm_to_concat = chunk[columnsSelect].copy()
            finalDF = pd.concat([finalDF, dm_to_concat], ignore_index=False)

            # if fileName == "dm_acidentes":
            #     finalDF["Data_Acidente"] = finalDF["Data_Acidente"].apply(self.converter_data)

        if op != 3:
            finalDF = finalDF.drop_duplicates(subset=columnFilter).reset_index(drop=True).sort_values(by=columnFilter)

        finalDF.to_csv(self.dataframes + f"{fileName}.csv", sep=";", encoding="utf-8", index=True, index_label="id", mode="a+")
        finalDF.to_sql(f"{fileName}", self.engine, if_exists="append", index=True, index_label="id")
        self.dbFuncs.insertLog(f"DF {fileName} inserted in DB!")

    async def createFDF(self):
        self.dbFuncs.insertLog("Mounting DB!")
        dm_m1 = pd.read_csv(self.tempFilesPatch + "/temp_model_1.csv", delimiter=";", usecols=self.useCols, low_memory=False, dtype=str, encoding="utf-8")
        dm_m2 = pd.read_csv(self.tempFilesPatch + "/temp_model_2.csv", delimiter=";", usecols=self.useCols, low_memory=False, dtype=str, encoding="utf-8")
        dm_m1["Data_Acidente"] = dm_m1["Data_Acidente"].apply(self.converter_data)
        dm_m2["Data_Acidente"] = dm_m2["Data_Acidente"].apply(self.converter_data)
        self.mainDF = pd.concat([dm_m1, dm_m2], ignore_index=True)

    async def createMDF(self):
        await self.workMainDF("dm_profissoes", "CBO", "CBO", 1, ["CBO", "Ocupacao"], "-")
        await self.workMainDF("dm_doencas", "CID_10", "CID_10", 2, ["CID_10", "Doenca"], " ")
        await self.workMainDF("dm_localidades", "Munic_Empr", "CIM", 1, ["CIM", "Munic_Empr"], "-")
        await self.workMainDF("dm_agentes", ["Agente_Causador_Acidente"], "Agente_Causador_Acidente", 3, ["Agente_Causador_Acidente"], False)
        await self.workMainDF("dm_partescorpo", ["Parte_Corpo_Atingida"], "Parte_Corpo_Atingida", 3, False, False)
        await self.workMainDF("dm_tipo_lesao", ["Natureza_da_Lesao"], "Natureza_da_Lesao", 3, False, False)
        await self.workMainDF("dm_empregadores", ["CNAE2_0_Empregador", "CNAE2_0_Empregador_1"], "CNAE2_0_Empregador", 3, False, False)
        await self.workMainDF("dm_acidentes", ["Data_Acidente", "Indica_obito_Acidente", "Sexo", "Tipo_do_Acidente", "UF_Munic_Acidente", "Data_Nascimento"], ["Data_Acidente", "Indica_obito_Acidente", "Sexo", "Tipo_do_Acidente", "UF_Munic_Acidente", "Data_Nascimento"], 3, False, False)
    
    # async def createTemp(self):
    #     self.dbFuncs.insertLog("Mounting DF TEMP")
    #     for i in range(0, len(self.mainDF), self.chuncksize):
    #         chunk = self.mainDF[i:i + self.chuncksize].copy()
    #         chunk[["CBO", "Ocupacao"]] = chunk["CBO"].str.split("-", n=1, expand=True)
    #         if chunk["CID_10"].str.contains("|".join(["000-ignorado", "000000-Não Informado"])).any():
    #             chunk["CID_10"] = chunk["CID_10"].str.replace("-", " ")
    #         else:
    #             chunk["CID_10"] = chunk["CID_10"].str.replace(".", "")
    #         chunk[["CID_10", "Doenca"]] = chunk["CID_10"].str.split(" ", n=1, expand=True)
    #         chunk[["CIM", "Munic_Empr"]] = chunk["Munic_Empr"].str.split("-", n=1, expand=True)
    #         self.tempDF = pd.concat([self.tempDF, chunk], ignore_index=True)
    #         chunk.to_sql("ft_temp_cats", self.engine, if_exists="append", index=True, index_label="id")
    #         chunk.to_csv(self.tempFilesPatch + "ft_temp_cats.csv", sep=";", encoding="utf-8", index=True, index_label="id", mode="a+")
    #     self.dbFuncs.insertLog("Table ft_temp_cats inserted in DB!")

    def loadCSV(self, fileName, index_col):
        filePath = os.path.join(self.dataframes + fileName)
        return pd.read_csv(filePath, sep=";", dtype=str).set_index(index_col)["id"].to_dict()

    async def createFact(self):
        self.dbFuncs.insertLog("Mounting DF FACT!")
    
        doencas_dict = self.loadCSV("dm_doencas.csv", "CID_10")
        agentes_dict = self.loadCSV("dm_agentes.csv", "Agente_Causador_Acidente")
        profissoes_dict = self.loadCSV("dm_profissoes.csv", "CBO")
        empregadores_dict = self.loadCSV("dm_empregadores.csv", "CNAE2_0_Empregador")
        localidades_dict = self.loadCSV("dm_localidades.csv", "CIM")
        tipo_lesao_dict = self.loadCSV("dm_tipo_lesao.csv", "Natureza_da_Lesao")
        partes_corpo_dict = self.loadCSV("dm_partescorpo.csv", "Parte_Corpo_Atingida")
        acidentes_dict = self.loadCSV("dm_acidentes.csv", ["Data_Acidente", "Indica_obito_Acidente", "Sexo", "Tipo_do_Acidente", "UF_Munic_Acidente", "Data_Nascimento"])
        
        df_ft_cats = self.tempDF.copy()
        df_ft_cats["id_doencas"] = df_ft_cats["CID_10"].map(doencas_dict)
        df_ft_cats["id_agentes"] = df_ft_cats["Agente_Causador_Acidente"].map(agentes_dict)
        df_ft_cats["id_profissoes"] = df_ft_cats["CBO"].map(profissoes_dict)
        df_ft_cats["id_empregadores"] = df_ft_cats["CNAE2_0_Empregador"].map(empregadores_dict)
        df_ft_cats["id_localidades"] = df_ft_cats["CIM"].map(localidades_dict)
        df_ft_cats["id_tipo_lesao"] = df_ft_cats["Natureza_da_Lesao"].map(tipo_lesao_dict)
        df_ft_cats["id_partescorpo"] = df_ft_cats["Parte_Corpo_Atingida"].map(partes_corpo_dict)
        columns_to_map = ["Data_Acidente", "Indica_obito_Acidente", "Sexo", "Tipo_do_Acidente", "UF_Munic_Acidente", "Data_Nascimento"]
        df_ft_cats["id_acidentes"] = df_ft_cats[columns_to_map].apply(tuple, axis=1).map(acidentes_dict)
        df_ft_cats = df_ft_cats[["id_doencas", "id_agentes", "id_profissoes", "id_empregadores", "id_localidades", "id_tipo_lesao", "id_partescorpo", "id_acidentes"]]
        df_ft_cats.to_sql("ft_cats", self.engine, if_exists="append", index=False)
        self.dbFuncs.insertLog("DF FACT inserted in DB!")

    async def main(self):
        await self.createFDF()
        await self.createMDF()
        # await self.createTemp()
        await self.createFact()
        shutil.rmtree(self.dataframes)
        self.dbFuncs.insertLog("Dataframes temporários deletados!")
        self.dbFuncs.insertLog("Processo de montagem do BIG DATA finalizado!")