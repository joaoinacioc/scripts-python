import numpy as np
import pandas as pd
import openpyxl
import os

# Encontrando o cabeçalho da tabela de desgastes
def find_header_beginning(ws):
    start_row = None
    for row in ws.iter_rows():
        for cell in row:
            if isinstance(cell.value, str) and "DESGASTE POR METRO" in cell.value:
                start_row = cell.row
                break
        if start_row is not None:
            break
    if start_row is None:
        raise ValueError("Starting row not found")
    return start_row


def find_table_end(df):
    for i in range(len(df)):
        value = df.iloc[i, 0]
        if pd.isnull(value) or isinstance(value, str):
            return i
    return len(df)


# Caminho do arquivo
directory = r'Caminho do Arquivo'
files = os.listdir(directory)
headers = ['metro', 'desgaste_le_esq', 'desgaste_le_dir', 'taxa_desg_le_esq', 'taxa_desg_le_dir',
           'desgaste_lg_esq', 'desgaste_lg_dir', 'taxa_desg_lg_esq', 'taxa_desg_lg_dir']
info_headers = ['canal_evento', 'fim_de_campanha', 'producao_acumulada', 'num_dias_em_operacao']

# Criando um dataframe vazio
concatenated_df = pd.DataFrame()

for file in files:
    pathfile = os.path.join(directory, file)
    
    # Lendo o arquivo com openpyxl
    wb = openpyxl.load_workbook(pathfile)
    ws = wb.active
    
    df_all = pd.read_excel(pathfile)
    df_info = pd.read_excel(pathfile, usecols= 'A,G,E,I', skiprows= 4, nrows= 2)
    df_info.columns = info_headers
    
    # Determinando o começo da leitura do arquivo e criando o dataframe para filtragem
    start_row = find_header_beginning(ws)
    rows_to_skip = start_row + 3
    df = pd.read_excel(pathfile, header=None, usecols="A, C:J", skiprows=rows_to_skip)
    df.columns = headers
    
    # Filtragem do dataframe usando a coluna de metro como referência
    end_of_table = find_table_end(df)
    df_final = df.iloc[:end_of_table]
    
    # Transformando dados das colunas numéricas do tipo string em valores nulos
    df_final[headers[1:]] = df_final[headers[1:]].applymap(lambda x: np.nan if isinstance(x, str) else x)
    
    # df_final['desgaste_le_esq'] = df_final['desgaste_le_esq'].replace('---', np.nan)
    # df_final['desgaste_le_dir'] = df_final['desgaste_le_dir'].replace('---', np.nan)
    # df_final['taxa_desg_le_esq'] = df_final['taxa_desg_le_esq'].replace('---', np.nan)
    # df_final['taxa_desg_le_dir'] = df_final['taxa_desg_le_dir'].replace('---', np.nan)
    # df_final['desgaste_lg_esq'] = df_final['desgaste_lg_esq'].replace('---', np.nan)
    # df_final['desgaste_lg_dir'] = df_final['desgaste_lg_dir'].replace('---', np.nan)
    # df_final['taxa_desg_lg_esq'] = df_final['taxa_desg_lg_esq'].replace('---', np.nan)
    # df_final['taxa_desg_lg_dir'] = df_final['taxa_desg_lg_dir'].replace('---', np.nan)
    
    df_final['Sum']= df_final[headers[1:-1]].sum(axis=1)
    if 0 in df_final['Sum'].values:
        df_final = df_final.dropna()
        
    df_final.drop(['Sum'], axis=1, inplace=True)
    
    # Adicionando Informações de Fim da Campanha, Prod Acumulada, Dias em Operação, Canal e Evento
    df_final['canal_evento'] = str(df_info['canal_evento'][0])
    if 'DI_' in pathfile:
        df_final[['canal', 'evento']] = df_final['canal_evento'].str.extract(r'AMP_AF1_(CP\d)_(DI)')
    elif 'RQ_' in pathfile:
        df_final[['canal', 'evento']] = df_final['canal_evento'].str.extract(r'AMP_AF1_(CP\d)_(RQ)')
    df_final['fim_de_campanha'] = df_info['fim_de_campanha'].dt.date[1]
    df_final['producao_acumulada'] = int(df_info['producao_acumulada'][1])
    df_final['num_dias_em_operacao'] = int(df_info['num_dias_em_operacao'][1])
    
    # Concatenando os dataframes
    concatenated_df = pd.concat([concatenated_df, df_final], ignore_index=True)

concatenated_df.drop(['canal_evento'], axis=1, inplace=True)
concatenated_df.to_excel(r'Caminho e nome do arquivo com extensão', sheet_name='Dados Espessuras', index=False)
