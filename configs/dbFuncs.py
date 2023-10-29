import sqlalchemy as sa
import dbConfig as dbC
import script as mkDatabase
from datetime import datetime

class manageDB:
    def __init__(self,schema):
        self.schema = schema
        self.engine_string = f"{dbC.dbType}+{dbC.driver}://{dbC.dbUser}:{dbC.dbPass}@{dbC.dbHost}:{dbC.dbPort}"

    def r_engine(self):
        return f"{self.engine_string}/{self.schema}"

    def makeSchema(self):
        testEngine = sa.create_engine(f"{dbC.dbType}+{dbC.driver}://{dbC.dbUser}:{dbC.dbPass}@{dbC.dbHost}:{dbC.dbPort}")
        # SE N EXISTIR O SCHEMA EFETUA A CRIACAO DO MESMO E DAS TABELAS
        if not self.schema in sa.inspect(testEngine).get_schema_names():
            stmt = sa.text(f"CREATE SCHEMA `{self.schema}` DEFAULT CHARACTER SET utf8;")
            testEngine.connect().execute(stmt)
            newEngine = sa.create_engine(f"{self.engine_string}/{self.schema}")
            create_tables = sa.text(mkDatabase.create_tables)
            newEngine.connect().execute(create_tables)
            # mkDatabase.metadata.create_all(newEngine)
        else:
            # SE JA EXISTIR, EFETUA A LIMPEZA DAS TABELAS
            newEngine = sa.create_engine(f"{self.engine_string}/{self.schema}")
            drop_tables = sa.text(f"DROP TABLE IF EXISTS dm_acidentes;DROP TABLE IF EXISTS dm_agentes;DROP TABLE IF EXISTS dm_doencas;DROP TABLE IF EXISTS dm_empregadores;DROP TABLE IF EXISTS dm_localidades;DROP TABLE IF EXISTS dm_profissoes;DROP TABLE IF EXISTS dm_tipo_lesao;DROP TABLE IF EXISTS dm_partescorpo;DROP TABLE IF EXISTS ft_cats;DROP TABLE IF EXISTS ft_temp_cats;")
            if newEngine.connect().execution_options(autocommit=True).execute(drop_tables):
                create_tables = sa.text(mkDatabase.create_tables)
                newEngine.connect().execute(create_tables)

    def insertLog(self, text):
        engine = sa.create_engine(f"{self.engine_string}/{self.schema}")
        conn = engine.connect()
        try:
            insertLogSQL = sa.text(f"INSERT INTO ft_logs (data_evento,hora_evento,evento) values ('{datetime.today().strftime('%d/%m/%Y')}','{datetime.today().strftime('%H:%M:%S')}','{text}');")
            if conn.execution_options(autocommit=False).execute(insertLogSQL):
                print(text)
                conn.commit()
        except Exception as E:
            print(E)