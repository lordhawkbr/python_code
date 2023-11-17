import pymysql
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
        self.engine_string = pymysql.Connect(host=self.dbHost, user=self.dbUser, database=None, password=self.dbPass, charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor,autocommit=True)
    
    def conn(self):
        try:
            conexao = pymysql.Connect(host=self.dbHost,user=self.dbUser,database=self.dbName,password=self.dbPass,charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor,autocommit=True)
            return conexao
        except pymysql.Error as e:
            return None

    def manageSchema(self):
        try:
            if self.conn() is None:
                with self.engine_string as conn:
                    with conn.cursor() as cursor:
                        self.exec(cursor, 1)
            else:
                with self.conn() as conexao:
                    with conexao.cursor() as cursor:
                        self.exec(cursor, 0)
                        
        except Exception as e:
            print(f"Error on manageSchema: {e}")

    def exec(self, cursor, method):
        if method == 1:
            create_sql = "CREATE SCHEMA {} DEFAULT CHARACTER SET utf8;"
            cursor.execute(create_sql.format(self.dbName))
            cursor.execute(f"USE {self.dbName}")
            create_tables = mkDatabase.create_tables
            for create_table in [sql.strip() for sql in create_tables.split(';') if sql.strip()]:
                cursor.execute(create_table)
        else:
            drop_sql = "DROP SCHEMA {};"
            cursor.execute(drop_sql.format(self.dbName))
            create_sql = "CREATE SCHEMA {} DEFAULT CHARACTER SET utf8;"
            cursor.execute(create_sql.format(self.dbName))
            cursor.execute(f"USE {self.dbName}")
            create_tables = mkDatabase.create_tables
            for create_table in [sql.strip() for sql in create_tables.split(';') if sql.strip()]:
                cursor.execute(create_table)

    def insertLog(self, evento):
        try:
            with self.conn() as conexao:
                with conexao.cursor() as cursor:
                    cursor.execute(f"USE {self.dbName}")
                    sql = "INSERT INTO `ft_logs` (`data_evento`, `hora_evento`, `evento`) VALUES (%s, %s, %s)"
                    cursor.execute(sql, (
                        datetime.today().strftime('%Y-%m-%d'),
                        datetime.today().strftime('%H:%M:%S'),
                        evento
                    ))
                    print(evento)
        except Exception as e:
            print(f"Error on insertLog: {e}")

    def r_engine(self):
        return f"mysql+pymysql://{self.dbUser}:{self.dbPass}@{self.dbHost}:{self.dbPort}/{self.dbName}"