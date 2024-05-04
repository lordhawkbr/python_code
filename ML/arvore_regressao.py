# BIBLIOTECAS
import sqlalchemy as sa
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
import configs.dbFuncs as dbFuncs
from sklearn.model_selection import train_test_split as TTS
import matplotlib.pyplot as plt
from sklearn.tree import DecisionTreeRegressor, plot_tree
from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()

class ArvoreRegressao:
    def __init__(self):
        self.dbFuncs = dbFuncs.manageDB()
        self.engine = sa.create_engine(self.dbFuncs.r_engine())

    def gerarGrafico(self):
        # Carregar os dados
        # PARA ACELERAR A CONSULTA  E TORNAR LEGIVEL, FORAM FILTRADAS MIL LINHAS
        data = pd.DataFrame(self.dbFuncs.execQuery("SELECT * FROM ft_rank LIMIT 1000")) # Caso faça a leitura via banco
        
        # Codificar features categóricas
        le = LabelEncoder()
        data["CBO"] = le.fit_transform(data["CBO"])

        # Selecionar colunas relevantes
        X = data[["CBO"]]
        y = data["Class_Risco"].astype(float)  # Converter para tipo float

        # Dividir os dados em conjunto de treino e teste
        X_train, X_test, y_train, y_test = TTS(X, y, test_size=0.33, random_state=1)

        # Treinar o regressor de árvore de decisão
        regressor = DecisionTreeRegressor(random_state=1)
        regressor.fit(X_train, y_train)

        # Prever com o conjunto de teste
        y_pred = regressor.predict(X_test)
        print(X_train)
        print(y_train)
        print(y_pred)
        # Plotar a árvore de decisão
        fig, ax = plt.subplots(figsize=(20, 10))
        plot_tree(regressor, filled=True, ax=ax)
        plt.show()