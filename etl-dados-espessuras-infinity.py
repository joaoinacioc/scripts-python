import pandas as pd
import numpy as np
import os

file_path = r'caminho do arquivo'
file = 'nome do arquivo'

new_headers = ['Metro', 'Desgaste Esquerdo (mm)', 'Desgaste Direito (mm)', 'Taxa Esquerdo (mm/ton)', 'Taxa Direito (mm/ton)']
new_column_name = ['Date']

linhas_para_pular = 43
linhas_para_ler = 19

# df_dados_espessuras = pd.DataFrame()

# Extraindo informações do diretório --Legenda: LE(Linha de Escória), LG(Linha de Gusa)--
df_LE = pd.read_excel(file_path+file,header=None,usecols="A,C:F", skiprows=linhas_para_pular, nrows=linhas_para_ler)
df_LG = pd.read_excel(file_path+file,header=None,usecols="A,G:J", skiprows=linhas_para_pular, nrows=linhas_para_ler)
df_date = pd.read_excel(file_path+file,header=None,usecols="E", skiprows=6, nrows=1)

# Fazendo transformações estruturais nos dataframes
df_date.columns = new_column_name
df_date = df_date['Date'].dt.date
df_LE.columns = new_headers
df_LG.columns = new_headers
df_LE = df_LE.replace('---', np.nan)
df_LG = df_LG.replace('---', np.nan)

# Variáveis para o loop e dataframe final
lenght = len(df_LE)
width = new_headers
df_final = pd.DataFrame(columns=new_headers)

for y in range(0,lenght, 1):
    for x in width:
        position_x_y_LE = df_LE[x][y]
        position_x_y_LG = df_LG[x][y]
        if np.isnan(position_x_y_LE) == False:
            df_final.loc[y, x] = position_x_y_LE
        else:
            df_final.loc[y, x] = position_x_y_LG

df_final['Data_medicao'] = df_date[0]
# df_dados_espessuras = pd.concat([df_dados_espessuras, df_final], ignore_index=True)

# df_final.to_excel(r'H:/Business Intelligence/Clientes/AMP/Redução/Base_Dados_Espessuras.xlsx')
