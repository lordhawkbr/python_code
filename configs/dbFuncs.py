import sqlalchemy as sa
import configs.script as mkDatabase
from datetime import datetime
import os

class manageDB:
    def __init__(self):
        self.dbType = os.getenv("dbType")
        self.dbDriver = os.getenv("dbDriver")
        self.dbUser = os.getenv("dbUser")
        self.dbPass = os.getenv("dbPass")
        self.dbHost = os.getenv("dbHost")
        self.dbPort = os.getenv("dbPort")
        self.dbName = os.getenv("dbName")
        self.engine_string = f"{self.dbType}+{self.dbDriver}://{self.dbUser}:{self.dbPass}@{self.dbHost}:{self.dbPort}"

    def r_engine(self):
        return f"{self.engine_string}/{self.dbName}"

    def makeSchema(self):
        try:
            testEngine = sa.create_engine(f"{self.dbType}+{self.dbDriver}://{self.dbUser}:{self.dbPass}@{self.dbHost}:{self.dbPort}")
            # SE N EXISTIR O dbName EFETUA A CRIACAO DO MESMO E DAS TABELAS
            if not self.dbName in sa.inspect(testEngine).get_schema_names():
                stmt = sa.text(f"CREATE SCHEMA `{self.dbName}` DEFAULT CHARACTER SET utf8;")
                testEngine.connect().execute(stmt)
                newEngine = sa.create_engine(f"{self.engine_string}/{self.dbName}")
                create_tables = sa.text(mkDatabase.create_tables)
                newEngine.connect().execute(create_tables)
                # mkDatabase.metadata.create_all(newEngine)
            else:
                # SE JA EXISTIR, EFETUA A LIMPEZA DAS TABELAS
                newEngine = sa.create_engine(f"{self.engine_string}/{self.dbName}")
                drop_tables = sa.text(f"DROP TABLE IF EXISTS dm_acidentes;DROP TABLE IF EXISTS dm_agentes;DROP TABLE IF EXISTS dm_doencas;DROP TABLE IF EXISTS dm_empregadores;DROP TABLE IF EXISTS dm_localidades;DROP TABLE IF EXISTS dm_profissoes;DROP TABLE IF EXISTS dm_tipo_lesao;DROP TABLE IF EXISTS dm_partescorpo;DROP TABLE IF EXISTS ft_cats;DROP TABLE IF EXISTS ft_temp_cats;")
                if newEngine.connect().execution_options(autocommit=True).execute(drop_tables):
                    create_tables = sa.text(mkDatabase.create_tables)
                    newEngine.connect().execute(create_tables)
        except Exception as e:
            print(e)

    def insertLog(self, text):
        engine = sa.create_engine(f"{self.engine_string}/{self.dbName}")
        conn = engine.connect()
        try:
            insertLogSQL = sa.text(f"INSERT INTO ft_logs (data_evento,hora_evento,evento) values ('{datetime.today().strftime('%d/%m/%Y')}','{datetime.today().strftime('%H:%M:%S')}','{text}');")
            if conn.execution_options(autocommit=False).execute(insertLogSQL):
                print(text)
                conn.commit()
        except Exception as E:
            print(E)
