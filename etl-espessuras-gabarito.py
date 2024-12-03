import pandas as pd
import os

diretorio_novos_dados = r'caminho do arquivo'
arquivos = os.listdir(diretorio_novos_dados)

cabeçalhos = ['Canal', 'DI', 'Data', 'Prod Acumulada', 'Prod DI']
df_dados_espessuras = pd.DataFrame()

def extrair_dados(df_skip_rows, info_skip_rows,arquivos):   # Extraindo informações do diretório
    df_combinado = pd.DataFrame()
    
    while df_skip_rows <= 63: 
        df = pd.read_excel(diretorio_novos_dados+arquivos, sheet_name='Medições', usecols='A,C:M', skiprows=df_skip_rows, nrows=17)
        
        df_info = pd.read_excel(diretorio_novos_dados+arquivos, sheet_name='Medições', usecols='D:F,H,L', skiprows=info_skip_rows, nrows=2)
        df_info.columns = cabeçalhos
        
        df['Canal'] = int(df_info['Canal'][0])
        di = str(df_info['DI'][1])
        data = df_info['Data'].dt.date[0]
        prod_acumulada = float(df_info['Prod Acumulada'][0])
        prod_DI = float(df_info['Prod DI'][0])
    
        df['DI'] = di
        df['Data'] = data
        df['Prod Acumulada'] = prod_acumulada
        df['Prod DI'] = prod_DI
        
        df_skip_rows += 20
        info_skip_rows += 20
        
        df_combinado = pd.concat([df_combinado, df], ignore_index=True)
    
    return df_combinado


diretorio_dados_coletados = r'caminho do arquivo'
diretorio_arquivo_final = r'caminho do arquivo, nome do arquivo com extensão'

for arquivo in range(len(arquivos)):
    df_temporario = extrair_dados(3,0,arquivos[arquivo])
    df_dados_espessuras = pd.concat([df_dados_espessuras, df_temporario], ignore_index=True)

df_dados_espessuras.to_excel(diretorio_arquivo_final, sheet_name='Dados Espessuras')
