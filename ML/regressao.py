# BIBLIOTECAS
import sqlalchemy as sa
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
import configs.dbFuncs as dbFuncs
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import numpy as np
from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()

class Regressao:
    def __init__(self):
        self.dbFuncs = dbFuncs.manageDB()
        self.engine = sa.create_engine(self.dbFuncs.r_engine())

    def gerarGrafico(self):
        # Carregar os dado
        # PARA ACELERAR A CONSULTA  E TORNAR LEGIVEL, FORAM FILTRADAS MIL LINHAS
        data = pd.DataFrame(self.dbFuncs.execQuery("SELECT * FROM ft_rank limit 1000")) # Caso faça a leitura via banco

        # Convertendo as colunas relevantes para numérico, se necessário
        data["CBO"] = le.fit_transform(data["CBO"])
        # data["Class_Risco"] = pd.to_numeric(data["Class_Risco"])
        # Selecionar as colunas relevantes
        X = data[["CBO"]]
        y = data["Class_Risco"]

        # Dividir os dados em conjuntos de treinamento e teste
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

        # Criar e ajustar o modelo de regressão linear
        regression_model = LinearRegression()
        regression_model.fit(X_train, y_train)

        # Fazer previsões nos dados de teste
        y_pred = regression_model.predict(X_test)

        # Plotar os dados e a linha de regressão
        plt.scatter(X_test, y_test, color='black')
        plt.plot(X_test, y_pred, color='blue', linewidth=3)
        plt.xlabel('CBO')
        plt.ylabel('Class_Risco')
        plt.title('Regressão Linear: CBO vs. Class_Risco')
        plt.show()

        # Plotar os dados de teste e a linha de regressão
        plt.scatter(X_test, y_test, color='black', label='Dados de Teste')
        plt.plot(X_test, y_pred, color='blue', linewidth=3, label='Linha de Regressão')
        plt.xlabel('CBO')
        plt.ylabel('Class_Risco')
        plt.title('Regressão Linear: CBO vs. Class_Risco (Dados de Teste)')
        plt.legend()
        plt.show()
