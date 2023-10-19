from datetime import datetime
import pandas as pd
import os
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

import asyncio

import dbConfig as dbC
import script as mkDatabase
# VARIAVEIS GLOBAIS
useCols = ["Agente Causador Acidente","Data Acidente","CBO","CID-10","CNAE2.0 Empregador","CNAE2.0 Empregador_1","Emitente CAT","Especie do beneficio","Filiacao Segurado","Indica obito Acidente","Munic Empr","Natureza da Lesao","Origem de Cadastramento CAT","Parte Corpo Atingida","Sexo","Tipo do Acidente","UF Munic. Acidente","UF Munic. Empregador","Data Afastamento","Data Despacho Beneficio","Data Nascimento","Data Emissao CAT"]
selectTBTempSQL = "SELECT acdnt.id id_acidente,agn.id id_agente,cid.id id_doenca,emp.id id_empregador,lcl.id id_local,cbo.id id_profissao,les.id id_tipo_lesao FROM ft_temp_cats temp  JOIN dm_acidentes acdnt  ON acdnt.UF_Munic__Acidente = temp.UF_Munic__Acidente AND acdnt.Data_Acidente = temp.Data_Acidente AND acdnt.Emitente_CAT = temp.Emitente_CAT AND acdnt.Especie_do_beneficio = temp.Especie_do_beneficio AND acdnt.Filiacao_Segurado = temp.Filiacao_Segurado AND acdnt.Indica_obito_Acidente = temp.Indica_obito_Acidente AND acdnt.Origem_de_Cadastramento_CAT = temp.Origem_de_Cadastramento_CAT AND acdnt.Sexo = temp.Sexo AND acdnt.Tipo_do_Acidente = temp.Tipo_do_Acidente AND acdnt.Data_Afastamento = temp.Data_Afastamento AND acdnt.Data_Despacho_Beneficio = temp.Data_Despacho_Beneficio AND acdnt.Data_Nascimento = temp.Data_Nascimento AND acdnt.Data_Emissao_CAT = temp.Data_Emissao_CAT  JOIN dm_agentes agn  ON agn.Agente_Causador_Acidente = temp.Agente_Causador_Acidente  JOIN dm_doencas cid  ON cid.CID_10 = temp.CID_10  JOIN dm_empregadores emp  ON emp.CNAE2_0_Empregador = temp.CNAE2_0_Empregador AND emp.CNAE2_0_Empregador_1 = temp.CNAE2_0_Empregador_1  JOIN dm_localidades lcl  ON lcl.Munic_Empr = temp.Munic_Empr  AND lcl.UF_Munic__Empregador = temp.UF_Munic__Empregador  JOIN dm_profissoes cbo  ON cbo.CBO = temp.CBO  JOIN dm_tipo_lesao les  ON les.Natureza_da_Lesao = temp.Natureza_da_Lesao  AND les.Parte_Corpo_Atingida = temp.Parte_Corpo_Atingida"
countRowsSQL = "SELECT COUNT(*) FROM ft_cats"

class ETL:
    def __init__(self, ROOT_DIR, schema):
        # PARAMETROS DE CONEXAO COM O BANCO
            self.schema = schema
            # VERIFICA SE JA EXISTE UM SCHEMA CRIADO, CASO NAO EXISTA REALIZA A CRIACAO DAS TABELAS
            self.makeSchema()
            self.engine = sa.create_engine(f"{dbC.dbType}+{dbC.driver}://{dbC.dbUser}:{dbC.dbPass}@{dbC.dbHost}:{dbC.dbPort}/{self.schema}")
            self.conn = self.engine.connect()
            self.ROOT_DIR = ROOT_DIR
            self.tempDF = pd.DataFrame()
            self.mainDF = pd.DataFrame()
            self.downloadPath = os.path.join(ROOT_DIR + "/downloads/")
            self.tempFilesPatch = os.path.join(ROOT_DIR + "/downloads/" + "temp/")
            self.M1Path = os.path.join(ROOT_DIR + "/downloads/" + "modelo_1/")
            self.M2Path = os.path.join(ROOT_DIR + "/downloads/" + "modelo_2/")

    def makeSchema(self):
        testEngine = sa.create_engine(f"{dbC.dbType}+{dbC.driver}://{dbC.dbUser}:{dbC.dbPass}@{dbC.dbHost}:{dbC.dbPort}")
        if not self.schema in sa.inspect(testEngine).get_schema_names():
            stmt = sa.text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            testEngine.connect().execute(stmt)
            newEngine = sa.create_engine(f"{dbC.dbType}+{dbC.driver}://{dbC.dbUser}:{dbC.dbPass}@{dbC.dbHost}:{dbC.dbPort}/{self.schema}")
            mkDatabase.metadata.create_all(newEngine)
        else:
            if len(sa.inspect(testEngine).get_table_names(schema=self.schema)) < 9:
                newEngine = sa.create_engine(f"{dbC.dbType}+{dbC.driver}://{dbC.dbUser}:{dbC.dbPass}@{dbC.dbHost}:{dbC.dbPort}/{self.schema}")
                mkDatabase.metadata.create_all(newEngine)

    def insertLog(self, text):
        try:
            insertLogSQL = sa.text(f"INSERT INTO ft_logs (data_evento,hora_evento,evento) values ('{datetime.today().strftime('%d/%m/%Y')}','{datetime.today().strftime('%H:%M:%S')}','{text}');")
            if self.conn.execution_options(autocommit=False).execute(insertLogSQL):
                print(text)
                self.conn.commit()
        except Exception as E:
            print(E)

    def mountMainDF(self):
        # MONTAGEM DO DATAFRAME FINAL MESCLANDO OS MODELOS 1 E 2
        df_m1 = pd.read_csv(self.tempFilesPatch + "/temp_model_1.csv", delimiter=";", usecols=useCols,low_memory=False,dtype=str)
        df_m2 = pd.read_csv(self.tempFilesPatch + "/temp_model_2.csv", delimiter=";", usecols=useCols,low_memory=False,dtype=str)
        self.tempDF = pd.concat([df_m1,df_m2], ignore_index=True)
        self.mainDF = pd.concat([df_m1,df_m2], ignore_index=True)
        totalRowsTEMP = pd.read_sql_query(countRowsSQL, self.engine)
        if len(self.tempDF.index) != totalRowsTEMP.values:
            return True

    async def main(self):
        if self.mountMainDF():
            # LOG INICIAL
            self.insertLog("Iniciado processo de montagem do BD!")

            # SUBSTITUI O ESPAÇO NO HEADER DO ARQUIVO PARA _ IGUALANDO AS COLUNAS DO BANCO
            self.mainDF.columns = [col.replace(" ", "_").replace(".","_").replace("-","_") for col in self.mainDF.columns]

            # EFETUA LIMPEZA DO BANCO ANTES DE INSERIR
            truncate_tables = sa.text(f"TRUNCATE dm_acidentes;TRUNCATE dm_agentes;TRUNCATE dm_doencas;TRUNCATE dm_empregadores;TRUNCATE dm_localidades;TRUNCATE dm_profissoes;TRUNCATE dm_tipo_lesao;TRUNCATE ft_cats;TRUNCATE ft_temp_cats;")
            if self.conn.execution_options(autocommit=True).execute(truncate_tables):
                self.insertLog("Tabelas redefinidas!")

            # POPULA TABELA FATO TEMP CATS
            if self.mainDF.to_sql("ft_temp_cats",self.engine,if_exists="append",index=False):
                self.insertLog("Registros da tabela ft_temp_cats inseridos!")

            # COLUNAS DAS TABELAS DIMENSAO A SEREM USADAS
            c_agentes = ["Agente Causador Acidente"]
            c_profissoes = ["CBO"]
            c_doencas = ["CID-10"]
            c_empregadores = ["CNAE2.0 Empregador","CNAE2.0 Empregador_1"]
            c_localidades = ["Munic Empr", "UF Munic. Empregador"]
            c_tipo_lesao = ["Natureza da Lesao", "Parte Corpo Atingida"]
            c_acidentes = ["Data Acidente","Emitente CAT","Especie do beneficio","Filiacao Segurado","Indica obito Acidente","Origem de Cadastramento CAT","Sexo","Tipo do Acidente","UF Munic. Acidente","Data Afastamento","Data Despacho Beneficio","Data Nascimento","Data Emissao CAT"]

            # MONTAGEM DOS DATAFRAMES DAS TABELAS DIMENSAO
            tb_agentes = self.tempDF[self.tempDF.columns[[i for i, value in enumerate(useCols) if value in c_agentes]]].sort_values(by=c_agentes, ascending=False).drop_duplicates()
            tb_agentes.columns = [col.replace(" ", "_").replace(".","_").replace("-","_") for col in tb_agentes.columns]

            tb_profissoes = self.tempDF[self.tempDF.columns[[i for i, value in enumerate(useCols) if value in c_profissoes]]].sort_values(by=c_profissoes, ascending=False).drop_duplicates()
            tb_profissoes.columns = [col.replace(" ", "_").replace(".","_").replace("-","_") for col in tb_profissoes.columns]

            tb_doencas = self.tempDF[self.tempDF.columns[[i for i, value in enumerate(useCols) if value in c_doencas]]].sort_values(by=c_doencas, ascending=False).drop_duplicates()
            tb_doencas.columns = [col.replace(" ", "_").replace(".","_").replace("-","_") for col in tb_doencas.columns]

            tb_localidades = self.tempDF[self.tempDF.columns[[i for i, value in enumerate(useCols) if value in c_localidades]]].sort_values(by=c_localidades, ascending=False).drop_duplicates()
            tb_localidades.columns = [col.replace(" ", "_").replace(".","_").replace("-","_") for col in tb_localidades.columns]

            tb_tipo_lesao = self.tempDF[self.tempDF.columns[[i for i, value in enumerate(useCols) if value in c_tipo_lesao]]].sort_values(by=c_tipo_lesao, ascending=False).drop_duplicates()
            tb_tipo_lesao.columns = [col.replace(" ", "_").replace(".","_").replace("-","_") for col in tb_tipo_lesao.columns]

            tb_acidentes = self.tempDF[self.tempDF.columns[[i for i, value in enumerate(useCols) if value in c_acidentes]]].sort_values(by=c_acidentes, ascending=False).drop_duplicates()
            tb_acidentes.columns = [col.replace(" ", "_").replace(".","_").replace("-","_") for col in tb_acidentes.columns]

            tb_empregadores = self.tempDF[self.tempDF.columns[[i for i, value in enumerate(useCols) if value in c_empregadores]]].sort_values(by=c_empregadores, ascending=False).drop_duplicates()
            tb_empregadores.columns = [col.replace(" ", "_").replace(".","_").replace("-","_") for col in tb_empregadores.columns]

            # MATRIZ DE RELACAO ENTRE TABELAS - DATAFRAMES
            matrix = [
                ["dm_agentes",tb_agentes],
                ["dm_profissoes",tb_profissoes],
                ["dm_doencas",tb_doencas],
                ["dm_empregadores",tb_empregadores],
                ["dm_localidades",tb_localidades],
                ["dm_tipo_lesao",tb_tipo_lesao],
                ["dm_acidentes",tb_acidentes]
            ]

            # POPULA AS TABELAS DIMENSAO
            for pos, value in enumerate(matrix):
                matrix[pos][1].to_sql(matrix[pos][0], self.engine, if_exists="append", index=False)
                self.insertLog(f"Registros da tabela {matrix[pos][0]} inseridos!")

            # POPULA A TABELA FATO
            df_fato = pd.read_sql_query(selectTBTempSQL, self.engine)
            if df_fato.to_sql("ft_cats", self.engine, if_exists="append", index=False):
                self.insertLog("Registros da tabela ft_cats inseridos!")
                self.insertLog("Processo de montagem do BIG DATA finalizado!")
        else:
            self.insertLog("Processo ETL cancelado: mesma quantidade de registros na origem e destino!")