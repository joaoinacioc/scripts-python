import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# Lista de schemas do db MySQL
db = ['cent_cent02', 'cent_cent03', 'cent_cent04']
# Formato do envio mysql+mysqlconnector://<user>:<password>@<host/ip>/dbtogo
url = 'mysql+mysqlconnector://tmxinsertuser:moasf0=!$as9zx@shwdbmysql01.mysql.database.azure.com/csn'

# Função para verificar a quantidade de linhas do df
def last_day_df_length(db, url):
    yesterday = (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")
    
    # Estabelecendo conexão com o banco de dados do MySQL
    engine = create_engine(url)
    connect_to_sql = engine.connect()
    
    # Querys efetuados no banco para formar os dataframes
    painel = f'SELECT * FROM {db}.tblsensores WHERE dt_reg = "{yesterday}" ;'
    df = pd.read_sql(painel, con=engine)
    column_length = df['temp1'].size
    connect_to_sql.close()
    
    return column_length


# Função para criar conexão com o banco de dados e gerar o df
def data_extraction_to_df(db, url):  
    # Estabelecendo conexão com o banco de dados do MySQL
    engine = create_engine(url)
    connect_to_sql = engine.connect()
    
    # Queries efetuados no db para formar os dataframes
    if db == 'cent_cent02':
        painel = f"""(SELECT *, CAST(CONCAT(dt_reg, ' ', horario)AS DATETIME) AS 'DataHora' FROM {db}.tblsensores ORDER BY DataHora DESC LIMIT 144) AS tmp3"""
    elif db == 'cent_cent03':
        painel = f"""(SELECT id, dt_reg, horario, temp1, temp2, temp3, temp4, CAST(CONCAT(dt_reg, ' ', horario)AS DATETIME) AS 'DataHora' FROM {db}.tblsensores ORDER BY DataHora DESC LIMIT 144) AS tmp3"""
    elif db == 'cent_cent04':
        painel = f"""(SELECT id, dt_reg, horario, temp5, temp6, temp7, temp8, CAST(CONCAT(dt_reg, ' ', horario)AS DATETIME) AS 'DataHora' FROM {db}.tblsensores ORDER BY DataHora DESC LIMIT 144) AS tmp4"""
        
    df = pd.read_sql(painel, con=engine)
    df.drop(['id', 'dt_reg', 'horario'], axis=1, inplace=True)
    connect_to_sql.close()
    
    return df

digital_email = ["Carol", "Ian", "João"]
it_email = ["Stêphano", "Rafa"]
csn_email = ["Eudes", "Maciel"]
amt_email = ["Gyiovanny", "Duarte"]


# Laço para percorrer cada um dos schemas do db
for query in db:
    # Armazenando a quantidade de linhas do df na variável capture
    capture = last_day_df_length(query, url)
    receiver = ''
    
    if query == 'cent_cent02':
        panel_name = 'Painel A CSN, lado canhão'
        receiver = digital_email+csn_email
    elif query == 'cent_cent03':
        panel_name = 'Painel B CSN, lado oposto'
        receiver = digital_email+csn_email
    elif query == 'cent_cent04':
        panel_name = 'Painel AMT, AF1 canal 3'
        receiver = digital_email+amt_email
    
    # Verificando a captação de sinal e retornando o envio do email
    if capture == 0:
        receiver.extend(it_email)
        print(f'Sem captação.\nEmail enviado para {receiver}')
    elif capture < 120:
        percent = (capture/120)*100
        print(f'Captação parcial de {percent:.0f}%.\nEmail enviado para {receiver}')
    elif capture >= 120:
        # Gerando DataFrame e armazenando número de colunas
        df = pd.DataFrame(data_extraction_to_df(query, url))
        headers = list(df.columns)
        del headers[-1]
        
        # Laço para percorrer o df, criar um agrupamento por valor de cada coluna e verificar o valor máximo dentre os agrupamentos
        for column in headers:
            zero_counter = 0
            occurrence = []
            df_count = df.groupby(f'{column}').size()
            df_count_max = df_count.max()
            
            if df_count_max > 60:
                for row in range(1,df['temp1'].size,1):
                    temp_value = df[f'{column}'][row]
                    if temp_value == 0: # Condicional que verifica o valor na linha atual, se o valor for igual a zero, contador de Zero aumenta em 1 e armazena a DataHora dessa ocorrência
                        zero_counter += 1
                        occurrence = df['DataHora'][row]
                if zero_counter == 0:
                    if column == 'temp1':
                        column = 'Sensor 1'
                    elif column == 'temp2':
                        column = 'Sensor 2'
                    elif column == 'temp3':
                        column = 'Sensor 3'
                    elif column == 'temp4':
                        column = 'Sensor 4'
                    elif column == 'temp5':
                        column = 'Sensor 5'
                    elif column == 'temp6':
                        column = 'Sensor 6'
                    elif column == 'temp7':
                        column = 'Sensor 7'
                    elif column == 'temp8':
                        column = 'Sensor 8'
                    elif column == 'temp9':
                        column = 'Sensor 9'
                    elif column == 'temp10':
                        column = 'Sensor 10'
                    elif column == 'temp11':
                        column = 'Sensor 11'
                    elif column == 'temp12':
                        column = 'Sensor 12'
                        
                    receiver = digital_email
                    print(f'Captação. O {column} está estático.\nEmail enviado para {receiver}.')
                    break
                elif zero_counter < 140:
                    if column == 'temp1':
                        column = 'Sensor 1'
                    elif column == 'temp2':
                        column = 'Sensor 2'
                    elif column == 'temp3':
                        column = 'Sensor 3'
                    elif column == 'temp4':
                        column = 'Sensor 4'
                    elif column == 'temp5':
                        column = 'Sensor 5'
                    elif column == 'temp6':
                        column = 'Sensor 6'
                    elif column == 'temp7':
                        column = 'Sensor 7'
                    elif column == 'temp8':
                        column = 'Sensor 8'
                    elif column == 'temp9':
                        column = 'Sensor 9'
                    elif column == 'temp10':
                        column = 'Sensor 10'
                    elif column == 'temp11':
                        column = 'Sensor 11'
                    elif column == 'temp12':
                        column = 'Sensor 12'
                    
                    percent = (zero_counter/144)*100
                    receiver = digital_email
                    
                    print(f'Captação. O {column} está estático a partir do período {occurrence}, sendo que {percent:.0f}% do sensor são valores zerados.\nEmail enviado para {receiver}.')
                    break
                elif zero_counter >= 140:
                    receiver = digital_email
                    print(f'Sem captação. Todos os valores zerados.\nEmail enviado para {receiver}.')
                    break
