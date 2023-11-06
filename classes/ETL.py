# BIBLIOTECAS
from datetime import datetime
import pandas as pd
import os
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()
# CLASSES
from configs.dbFuncs import *
import configs.script as mkDatabase

class ETL:
    def __init__(self):
        # PARAMETROS DE CONEXAO COM O BANCO
            ROOT_DIR = os.getenv("ROOT_DIR")
            self.dbFuncs = manageDB()
            self.engine = sa.create_engine(self.dbFuncs.r_engine())
            self.conn = self.engine.connect()
            self.tempDF = pd.DataFrame()
            self.mainDF = pd.DataFrame()
            self.downloadPath = os.path.join(ROOT_DIR + "/downloads/")
            self.tempFilesPatch = os.path.join(self.downloadPath + "temp/")
            self.M1Path = os.path.join(self.downloadPath + "modelo_1/")
            self.M2Path = os.path.join(self.downloadPath + "modelo_2/")
            self.dataframes = os.path.join(self.downloadPath + "dataframes/")

    def mountMainDF(self):
        # MONTAGEM DO DATAFRAME FINAL MESCLANDO OS MODELOS 1 E 2
        df_m1 = pd.read_csv(self.tempFilesPatch + "/temp_model_1.csv", delimiter=";", usecols=mkDatabase.useCols,low_memory=False,dtype=str, encoding="utf-8")
        df_m2 = pd.read_csv(self.tempFilesPatch + "/temp_model_2.csv", delimiter=";", usecols=mkDatabase.useCols,low_memory=False,dtype=str, encoding="utf-8")
        self.mainDF = pd.concat([df_m1,df_m2], ignore_index=True)
        self.tempDF = self.mainDF.copy()
        totalRowsTEMP = pd.read_sql_query(mkDatabase.countRowsSQL, self.engine)
        if len(self.tempDF.index) != totalRowsTEMP.values:
            return True

    def sortAndDrop(self,column):
        return self.tempDF[self.tempDF.columns[[i for i, value in enumerate(mkDatabase.useCols) if value in column]]].sort_values(by=column, ascending=False).drop_duplicates().reset_index(drop=True)
    
    def splitColumns(self, column, string, df):
        return df[column].str.split(string, n=1, expand=True)
    
    def makeFact(self):
        dm_doencas = pd.read_csv(self.dataframes + "dm_doencas.csv", sep=";",dtype=str)
        dm_agentes = pd.read_csv(self.dataframes + "dm_agentes.csv", sep=";",dtype=str)
        dm_profissoes = pd.read_csv(self.dataframes + "dm_profissoes.csv", sep=";",dtype=str)
        dm_empregadores = pd.read_csv(self.dataframes + "dm_empregadores.csv", sep=";",dtype=str)
        dm_localidades = pd.read_csv(self.dataframes + "dm_localidades.csv", sep=";",dtype=str)
        dm_tipo_lesao = pd.read_csv(self.dataframes + "dm_tipo_lesao.csv", sep=";",dtype=str)
        dm_partes_corpo = pd.read_csv(self.dataframes + "dm_partescorpo.csv", sep=";",dtype=str)
        dm_acidentes = pd.read_csv(self.dataframes + "dm_acidentes.csv", sep=";",dtype=str)

        doencas_dict = dm_doencas.set_index('CID_10')['index'].to_dict()
        agentes_dict = dm_agentes.set_index('Agente_Causador_Acidente')['index'].to_dict()
        profissoes_dict = dm_profissoes.set_index('CBO')['index'].to_dict()
        empregadores_dict = dm_empregadores.set_index('CNAE2_0_Empregador')['index'].to_dict()
        localidades_dict = dm_localidades.set_index('CIM')['index'].to_dict()
        tipo_lesao_dict = dm_tipo_lesao.set_index('Natureza_da_Lesao')['index'].to_dict()
        partes_corpo_dict = dm_partes_corpo.set_index('Parte_Corpo_Atingida')['index'].to_dict()

        df_ft_cats = self.mainDF.copy()
        df_ft_cats['id_doencas'] = df_ft_cats['CID_10'].map(doencas_dict)
        df_ft_cats['id_agentes'] = df_ft_cats['Agente_Causador_Acidente'].map(agentes_dict)
        df_ft_cats['id_profissoes'] = df_ft_cats['CBO'].map(profissoes_dict)
        df_ft_cats['id_empregadores'] = df_ft_cats['CNAE2_0_Empregador'].map(empregadores_dict)
        df_ft_cats['id_localidades'] = df_ft_cats['CIM'].map(localidades_dict)
        df_ft_cats['id_tipo_lesao'] = df_ft_cats['Natureza_da_Lesao'].map(tipo_lesao_dict)
        df_ft_cats['id_partescorpo'] = df_ft_cats['Parte_Corpo_Atingida'].map(partes_corpo_dict)
        df_ft_cats = df_ft_cats.merge(dm_acidentes, on=['Data_Acidente', 'Indica_obito_Acidente', 'Sexo', 'Tipo_do_Acidente', 'UF_Munic_Acidente', 'Data_Nascimento'])
        df_ft_cats.rename(columns={"index": "id_acidentes"}, inplace=True)
        df_ft_cats = df_ft_cats[["id_doencas", "id_agentes", "id_profissoes", "id_empregadores", "id_localidades", "id_tipo_lesao", "id_partescorpo", "id_acidentes"]]
        
        return df_ft_cats

    async def main(self):
        if self.mountMainDF():
            # LOG INICIAL
            self.dbFuncs.insertLog("Iniciado processo de montagem do BD!")

            # MONTA NOVAS COLUNAS PARA FACILITAR A IDENTIFICACAO
            self.mainDF[["CBO", "Ocupacao"]] = self.splitColumns("CBO","-",self.mainDF)
            self.mainDF["CID_10"] = self.mainDF["CID_10"].apply(lambda value: value if value == "000000-Não informado" else value.replace("-", " "))
            self.mainDF[["CID_10", "Doenca"]] = self.mainDF["CID_10"].apply(lambda value: pd.Series(value.split("-", 1) if "-" in value else (value.split(" ", 1))))
            self.mainDF["CID_10"] = self.mainDF["CID_10"].str.replace(".","")
            self.mainDF[["CIM", "Munic_Empr"]] = self.splitColumns("Munic_Empr","-",self.mainDF)

            # COLUNAS DAS TABELAS DIMENSAO A SEREM USADAS
            c_agentes = ["Agente_Causador_Acidente"]
            c_profissoes = ["CBO"]
            c_doencas = ["CID_10"]
            c_empregadores = ["CNAE2_0_Empregador","CNAE2_0_Empregador_1"]
            c_localidades = ["Munic_Empr","UF_Munic_Empregador"]
            c_partes_corpo = ["Parte_Corpo_Atingida"]
            c_tipo_lesao = ["Natureza_da_Lesao"]
            c_acidentes = ["Data_Acidente","Indica_obito_Acidente","Sexo","Tipo_do_Acidente","UF_Munic_Acidente","Data_Nascimento"]

            # MONTAGEM DOS DATAFRAMES DAS TABELAS DIMENSAO
            dm_agentes = self.sortAndDrop(c_agentes)

            dm_profissoes = self.sortAndDrop(c_profissoes)
            dm_profissoes[["CBO", "Ocupacao"]] = self.splitColumns("CBO","-",dm_profissoes)

            dm_doencas = self.sortAndDrop(c_doencas)
            dm_doencas["CID_10"] = dm_doencas["CID_10"].apply(lambda value: value if value == "000000-Não informado" else value.replace("-", " "))
            dm_doencas[["CID_10", "Doenca"]] = dm_doencas["CID_10"].apply(lambda value: pd.Series(value.split("-", 1) if "-" in value else (value.split(" ", 1))))
            dm_doencas["CID_10"] = dm_doencas["CID_10"].str.replace(".","")

            dm_localidades = self.sortAndDrop(c_localidades)
            dm_localidades[["CIM", "Munic_Empr"]] = self.splitColumns("Munic_Empr","-",dm_localidades)

            dm_tipo_lesao = self.sortAndDrop(c_tipo_lesao)
            dm_partes_corpo = self.sortAndDrop(c_partes_corpo)

            dm_acidentes = self.sortAndDrop(c_acidentes)
            dm_empregadores = self.sortAndDrop(c_empregadores)

            # MATRIZ DE RELACAO ENTRE TABELAS - DATAFRAMES
            matrix = [
                ["dm_agentes",dm_agentes],
                ["dm_profissoes",dm_profissoes],
                ["dm_doencas",dm_doencas],
                ["dm_empregadores",dm_empregadores],
                ["dm_localidades",dm_localidades],
                ["dm_tipo_lesao",dm_tipo_lesao],
                ["dm_partescorpo",dm_partes_corpo],
                ["dm_acidentes",dm_acidentes]
            ]
            
            # POPULA AS TABELAS DIMENSAO
            for pos, value in enumerate(matrix):
                matrix[pos][1].to_sql(matrix[pos][0], self.engine, if_exists="append", index=True, index_label="id")
                matrix[pos][1].to_csv(self.dataframes + "/" + matrix[pos][0] + ".csv", sep=";", encoding="utf-8", index=True, index_label="index")
                self.dbFuncs.insertLog(f"Registros da tabela {matrix[pos][0]} inseridos e exportados para CSV!")
            
            # POPULA TABELA FATO TEMP CATS
            if self.mainDF.to_sql("ft_temp_cats",self.engine,if_exists="append",index=False):
                self.mainDF.to_csv(self.dataframes + "/ft_temp_cats.csv", sep=";", encoding="utf-8", index=True, index_label="index")
                self.dbFuncs.insertLog("Registros da tabela ft_temp_cats inseridos e exportados para CSV!")
            
            # POPULA A TABELA FATO
            df_fato = self.makeFact()
            if df_fato.to_sql("ft_cats", self.engine, if_exists="append", index=False):
                self.dbFuncs.insertLog("Registros da tabela ft_cats inseridos!")
                self.dbFuncs.insertLog("Processo de montagem do BIG DATA finalizado!")

        else:
            self.dbFuncs.insertLog("Processo ETL cancelado!")