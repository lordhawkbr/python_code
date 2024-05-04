# BIBLIOTECAS
import sqlalchemy as sa
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
import configs.dbFuncs as dbFuncs

class Rank:
    def __init__(self):
        self.dbFuncs = dbFuncs.manageDB()
        self.engine = sa.create_engine(self.dbFuncs.r_engine())

    async def rank(self):
        # # preparação e classificação frequencia
        base_df = pd.DataFrame(self.dbFuncs.execQuery("SELECT * FROM ft_temp_cats"))
        parteDoCorpo_df = base_df.groupby("Parte_Corpo_Atingida").size().reset_index(name="Ocorrencia").sort_values(by="Ocorrencia", ascending=False)
        parteDoCorpo_df["Rank"] = parteDoCorpo_df["Ocorrencia"].rank(ascending=False)
        parteDoCorpo_distinctcount = parteDoCorpo_df["Parte_Corpo_Atingida"].nunique() #quantidade de partes do corpo representadas na base

        parteDoCorpo_df["Class_Frequencia"] = parteDoCorpo_df["Rank"].apply(lambda x:
                                                            4 if x/parteDoCorpo_distinctcount <= 0.25 else
                                                            3 if x/parteDoCorpo_distinctcount <= 0.5 else
                                                            1 if x/parteDoCorpo_distinctcount <= 0.75 else 1)  #divide em 4 classificações (1-4) do mais frequente pro menos frequente
        # # preparação base óbito óbito
        baseObito_df = base_df[base_df["Indica_obito_Acidente"] == "sim"]
        parteDoCorpoObito_df = baseObito_df.groupby("Parte_Corpo_Atingida").size().reset_index(name="Obito").sort_values(by="Obito", ascending=False)
        parteDoCorpoObito_df["Perc_Obito"] = parteDoCorpoObito_df["Obito"] / parteDoCorpoObito_df["Obito"].sum()
        parteDoCorpoObito_df["Cum_Perc_Obito"] = parteDoCorpoObito_df["Perc_Obito"].cumsum()

        # # join das classificações de óbtido com as classificações de frequência
        parteDoCorpo_df = parteDoCorpo_df.merge(parteDoCorpoObito_df,"left",left_on="Parte_Corpo_Atingida",right_on="Parte_Corpo_Atingida")

        # # classificação de óbito
        parteDoCorpo_df["Class_Obito"] = parteDoCorpo_df["Cum_Perc_Obito"].apply(lambda x:
                                                                3 if x <= 0.33 else
                                                                2 if x <= 0.66 else
                                                                1 if x <= 1 else 0)  # divide em 3 classificações (1-3) do mais representativo de óbitos para o menos. Quando não há óbito fica 0.

        # # Nota final (0-9)
        parteDoCorpo_df["ClassParteCorpo"] =  parteDoCorpo_df["Class_Obito"] * parteDoCorpo_df["Class_Frequencia"] / 12 # divide por 12 para que a classificaçao fique entre 0 e 1

        parteDoCorpo_df = parteDoCorpo_df[['Parte_Corpo_Atingida','ClassParteCorpo']]
        # preparação e classificação frequencia
        tipoDeLesao_df = base_df.groupby('Natureza_da_Lesao').size().reset_index(name='Ocorrencia').sort_values(by='Ocorrencia', ascending=False)
        tipoDeLesao_df['Rank'] = tipoDeLesao_df['Ocorrencia'].rank(ascending=False)
        tipoDeLesao_distinctcount = tipoDeLesao_df['Natureza_da_Lesao'].nunique() #quantidade de partes do corpo representadas na base

        tipoDeLesao_df['Class_Frequencia'] = tipoDeLesao_df['Rank'].apply(lambda x:
                                                                4 if x/tipoDeLesao_distinctcount <= 0.25 else
                                                                3 if x/tipoDeLesao_distinctcount <= 0.5 else
                                                                2 if x/tipoDeLesao_distinctcount <= 0.75 else 1)  #divide em 4 classificações (1-4) do mais frequente pro menos frequente

        # preparação base óbito óbito
        baseObito_df = base_df[base_df['Indica_obito_Acidente'] == "Sim"]
        tipoDeLesaoObito_df = baseObito_df.groupby('Natureza_da_Lesao').size().reset_index(name='Obito').sort_values(by='Obito', ascending=False)
        tipoDeLesaoObito_df['Perc_Obito'] = tipoDeLesaoObito_df['Obito'] / tipoDeLesaoObito_df['Obito'].sum()
        tipoDeLesaoObito_df['Cum_Perc_Obito'] = tipoDeLesaoObito_df['Perc_Obito'].cumsum()

        # join das classificações de óbtido com as classificações de frequência
        tipoDeLesao_df = tipoDeLesao_df.merge(tipoDeLesaoObito_df,'left',left_on='Natureza_da_Lesao',right_on='Natureza_da_Lesao')

        # classificação de óbito
        tipoDeLesao_df['Class_Obito'] = tipoDeLesao_df['Cum_Perc_Obito'].apply(lambda x:
                                                                3 if x <= 0.33 else
                                                                2 if x <= 0.66 else
                                                                1 if x <= 1 else 0)  # divide em 3 classificações (1-3) do mais representativo de óbitos para o menos. Quando não há óbito fica 0.

        # Nota final (0-9)
        tipoDeLesao_df['ClassLesao'] =  (tipoDeLesao_df['Class_Obito'] * tipoDeLesao_df['Class_Frequencia']) / 12 # divide por 12 para que a classificaçao fique entre 0 e 1
        parteDoCorpo_df = parteDoCorpo_df[['Parte_Corpo_Atingida','ClassParteCorpo']]
        tipoDeLesao_df = tipoDeLesao_df[['Natureza_da_Lesao', 'ClassLesao']]
        baseClassif_df = base_df.merge(tipoDeLesao_df,'left',left_on='Natureza_da_Lesao',right_on='Natureza_da_Lesao')
        baseClassif_df = baseClassif_df.merge(parteDoCorpo_df,'left',left_on='Parte_Corpo_Atingida',right_on='Parte_Corpo_Atingida')
        baseClassif_df['Class_Risco'] = (baseClassif_df['ClassLesao'] + baseClassif_df['ClassParteCorpo'])/2

        baseClassif_df.to_sql("ft_rank", self.engine, if_exists="replace", index=True, index_label="idx")
        self.dbFuncs.insertLog(f"DF ft_rank inserido no BD!")

    async def main(self):
        await self.rank()
        self.dbFuncs.insertLog("Classificação de risco finalizada!")