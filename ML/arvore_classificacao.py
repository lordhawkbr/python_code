# BIBLIOTECAS
import sqlalchemy as sa
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
import configs.dbFuncs as dbFuncs
import pandas as pd
from sklearn.model_selection import train_test_split as TTS
import matplotlib.pyplot as plt
from sklearn import tree
from sklearn.metrics import confusion_matrix
from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()

class ArvoreClass:
    def __init__(self):
        self.dbFuncs = dbFuncs.manageDB()
        self.engine = sa.create_engine(self.dbFuncs.r_engine())

    def gerarGrafico(self):
        data = pd.DataFrame(self.dbFuncs.execQuery("SELECT * FROM ft_rank LIMIT 1000")) # Caso faça a leitura via banco

        iris = data[['CBO', 'Sexo', 'Class_Risco']]
        # Separa as colunas CBO E Class_Risco para o modelo
        # iris.loc[:, "Sexo"] = le.fit_transform(data["Sexo"])
        iris = pd.get_dummies(iris, columns=["Sexo"], drop_first=True)

        X = iris.drop(['Class_Risco','CBO'],axis=1)
        y = iris.Class_Risco
        X_train, X_test, y_train, y_test = TTS(X, y, test_size=0.33, random_state=42)

        # Criando o classificador e fazendo o fit
        clf2 = tree.DecisionTreeClassifier(random_state=42).fit(X_train,y_train)

        # Exibe o score (quanto mais perto de 0 melhor)
        print(f"Score: {clf2.score(X_train,y_train)}")

        # Prepara a visualização grafica
        fig, ax = plt.subplots(figsize=(20,8))
        tree.plot_tree(clf2)
        y_pred2 = clf2.predict(X_test)

        # Exibe a precisao da classificao
        print(f"Precisão :{confusion_matrix(y_test,y_pred2)}")
        plt.show()