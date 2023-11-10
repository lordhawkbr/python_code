# BIBLIOTECAS
import sqlalchemy as sa
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()
import configs.dbFuncs as dbFuncs
import configs.script as mkDatabase
import shutil

class ETL:
    def __init__(self, ROOT_DIR):
        self.dbFuncs = dbFuncs.manageDB()
        self.useCols = ["Agente_Causador_Acidente", "Data_Acidente", "CBO", "CID_10", "CNAE2_0_Empregador", "CNAE2_0_Empregador_1", "Indica_obito_Acidente", "Munic_Empr", "Natureza_da_Lesao", "Parte_Corpo_Atingida", "Sexo", "Tipo_do_Acidente", "UF_Munic_Acidente", "UF_Munic_Empregador", "Data_Afastamento", "Data_Nascimento"]
        self.ROOT_DIR = ROOT_DIR
        self.engine = sa.create_engine(self.dbFuncs.r_engine())
        self.conn = self.engine.connect()
        self.downloadPath = os.path.join(self.ROOT_DIR + "/downloads/")
        self.tempFilesPatch = os.path.join(self.downloadPath + "temp/")
        self.dataframes = os.path.join(self.downloadPath + "dataframes/")
        shutil.rmtree(self.dataframes)
        self.mainDF = pd.DataFrame()
        self.tempDF = pd.DataFrame()
        try:
            if not os.path.exists(self.dataframes):
                os.makedirs(self.dataframes)
        except Exception as E:
            print("Exception __init__: " + E)
    
    async def workMainDF(self, dm_to_concat, columnsSelect, columnFilter, op, newColumns, string):
        self.dbFuncs.insertLog(f"Iniciada montagem do DF {dm_to_concat}")
        fileName = dm_to_concat
        finalDF = pd.DataFrame()
        for i in range(0, len(self.mainDF), 25000):
            chunk = self.mainDF.iloc[i:i + 25000]
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
            finalDF = pd.concat([finalDF, dm_to_concat], ignore_index=True)

        if op != 3:
            finalDF = finalDF.drop_duplicates(subset=columnFilter).reset_index(drop=True).sort_values(by=columnFilter)

        finalDF.to_csv(self.dataframes + f"{fileName}.csv", sep=";", encoding="utf-8", index=True, index_label="index", mode="a+")
        finalDF.to_sql(f"{fileName}", self.engine, if_exists="append", index=False)
        self.dbFuncs.insertLog(f"DF {fileName} inserido no BD!")

    async def createFDF(self):
        self.dbFuncs.insertLog("Iniciada montagem do BD!")
        dm_m1 = pd.read_csv(self.tempFilesPatch + "/temp_model_1.csv", delimiter=";", usecols=self.useCols, low_memory=False, dtype=str, encoding="utf-8")
        dm_m2 = pd.read_csv(self.tempFilesPatch + "/temp_model_2.csv", delimiter=";", usecols=self.useCols, low_memory=False, dtype=str, encoding="utf-8")
        mainDF = pd.concat([dm_m1, dm_m2], ignore_index=True)
        self.mainDF = mainDF

    async def createMDF(self):
        await self.workMainDF("dm_profissoes", "CBO", "CBO", 1, ["CBO", "Ocupacao"], "-")
        await self.workMainDF("dm_doencas", "CID_10", "CID_10", 2, ["CID_10", "Doenca"], " ")
        await self.workMainDF("dm_localidades", "Munic_Empr", "CIM", 1, ["CIM", "Munic_Empr"], "-")
        await self.workMainDF("dm_agentes", ["Agente_Causador_Acidente"], "Agente_Causador_Acidente", 3, ["Agente_Causador_Acidente"], False)
        await self.workMainDF("dm_partescorpo", ["Parte_Corpo_Atingida"], "Parte_Corpo_Atingida", 3, False, False)
        await self.workMainDF("dm_tipo_lesao", ["Natureza_da_Lesao"], "Natureza_da_Lesao", 3, False, False)
        await self.workMainDF("dm_empregadores", ["CNAE2_0_Empregador", "CNAE2_0_Empregador_1"], "CNAE2_0_Empregador", 3, False, False)
        await self.workMainDF("dm_acidentes", ["Data_Acidente", "Indica_obito_Acidente", "Sexo", "Tipo_do_Acidente", "UF_Munic_Acidente", "Data_Nascimento"], ["Data_Acidente", "Indica_obito_Acidente", "Sexo", "Tipo_do_Acidente", "UF_Munic_Acidente", "Data_Nascimento"], 3, False, False)
    
    async def createTemp(self):
        self.dbFuncs.insertLog("Iniciada montagem do DF TEMP")
        for i in range(0, len(self.mainDF), 25000):
            chunk = self.mainDF[i:i + 25000].copy()
            chunk[["CBO", "Ocupacao"]] = chunk["CBO"].str.split("-", n=1, expand=True)
            if chunk["CID_10"].str.contains("|".join(["000-ignorado", "000000-Não Informado"])).any():
                chunk["CID_10"] = chunk["CID_10"].str.replace("-", " ")
            else:
                chunk["CID_10"] = chunk["CID_10"].str.replace(".", "")
            # chunk["CID_10"] = chunk["CID_10"].str.replace(".", "")
            chunk[["CID_10", "Doenca"]] = chunk["CID_10"].str.split(" ", n=1, expand=True)
            chunk[["CIM", "Munic_Empr"]] = chunk["Munic_Empr"].str.split("-", n=1, expand=True)
            self.tempDF = pd.concat([self.tempDF, chunk], ignore_index=True)
            chunk.to_sql("ft_temp_cats", self.engine, if_exists="append", index=False)
        self.dbFuncs.insertLog("Tabela ft_temp_cats inserida no BD!")

    async def createFact(self):
        self.dbFuncs.insertLog("Iniciando montagem do DF FATO!")
        dm_doencas = pd.read_csv(self.dataframes + "dm_doencas.csv", sep=";", dtype=str)
        dm_agentes = pd.read_csv(self.dataframes + "dm_agentes.csv", sep=";", dtype=str)
        dm_profissoes = pd.read_csv(self.dataframes + "dm_profissoes.csv", sep=";", dtype=str)
        dm_empregadores = pd.read_csv(self.dataframes + "dm_empregadores.csv", sep=";", dtype=str)
        dm_localidades = pd.read_csv(self.dataframes + "dm_localidades.csv", sep=";", dtype=str)
        dm_tipo_lesao = pd.read_csv(self.dataframes + "dm_tipo_lesao.csv", sep=";", dtype=str)
        dm_partes_corpo = pd.read_csv(self.dataframes + "dm_partescorpo.csv", sep=";", dtype=str)
        dm_acidentes = pd.read_csv(self.dataframes + "dm_acidentes.csv", sep=";", dtype=str)

        doencas_dict = dm_doencas.set_index("CID_10")["index"].to_dict()
        agentes_dict = dm_agentes.set_index("Agente_Causador_Acidente")["index"].to_dict()
        profissoes_dict = dm_profissoes.set_index("CBO")["index"].to_dict()
        empregadores_dict = dm_empregadores.set_index("CNAE2_0_Empregador")["index"].to_dict()
        localidades_dict = dm_localidades.set_index("CIM")["index"].to_dict()
        tipo_lesao_dict = dm_tipo_lesao.set_index("Natureza_da_Lesao")["index"].to_dict()
        partes_corpo_dict = dm_partes_corpo.set_index("Parte_Corpo_Atingida")["index"].to_dict()

        df_ft_cats = self.tempDF.copy()
        df_ft_cats["id_doencas"] = df_ft_cats["CID_10"].map(doencas_dict)
        df_ft_cats["id_agentes"] = df_ft_cats["Agente_Causador_Acidente"].map(agentes_dict)
        df_ft_cats["id_profissoes"] = df_ft_cats["CBO"].map(profissoes_dict)
        df_ft_cats["id_empregadores"] = df_ft_cats["CNAE2_0_Empregador"].map(empregadores_dict)
        df_ft_cats["id_localidades"] = df_ft_cats["CIM"].map(localidades_dict)
        df_ft_cats["id_tipo_lesao"] = df_ft_cats["Natureza_da_Lesao"].map(tipo_lesao_dict)
        df_ft_cats["id_partescorpo"] = df_ft_cats["Parte_Corpo_Atingida"].map(partes_corpo_dict)
        df_ft_cats = df_ft_cats.merge(dm_acidentes, on=["Data_Acidente", "Indica_obito_Acidente", "Sexo", "Tipo_do_Acidente", "UF_Munic_Acidente", "Data_Nascimento"])
        df_ft_cats.rename(columns={"index": "id_acidentes"}, inplace=True)
        df_ft_cats = df_ft_cats[["id_doencas", "id_agentes", "id_profissoes", "id_empregadores", "id_localidades", "id_tipo_lesao", "id_partescorpo", "id_acidentes"]]
        df_ft_cats.to_sql("ft_cats", self.engine, if_exists="append", index=False)
        self.dbFuncs.insertLog("DF FATO inserido no BD!")

    async def main(self):
        await self.createFDF()
        await self.createMDF()
        await self.createTemp()
        await self.createFact()
        shutil.rmtree(self.dataframes)
        self.dbFuncs.insertLog("Dataframes temporários deletados!")
        self.dbFuncs.insertLog("Processo de montagem do BIG DATA finalizado!")