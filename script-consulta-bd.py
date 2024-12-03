from sqlalchemy import create_engine
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import ssl
import datetime

filepath = r'caminho'
sql_sensors_AMT = 'cent_cent04'
sql_sensors_CSN_CP03_A = 'cent_cent02'
sql_sensors_CSN_CP03_B = 'cent_cent03'
sensors_list = [sql_sensors_AMT, sql_sensors_CSN_CP03_A, sql_sensors_CSN_CP03_B]

def etl(sql):
    # Formato do envio mysql+mysqlconnector://<user>:<password>@<host/ip>/dbtogo
    url = 'mysql+mysqlconnector://"user":"password"@"host ip"/"database"'

    # Estabelecendo conexão com o banco de dados do MySQL
    engine = create_engine(url)
    connect_to_sql = engine.connect()

    # Querys efetuados no banco para formar os dataframes
    sql_sensors = f"SELECT *, CAST(CONCAT(dt_reg, ' ', horario)AS DATETIME) AS 'DataHora' FROM {sql}.tblsensores ORDER BY DataHora DESC LIMIT 1;"
    df = pd.read_sql(sql_sensors,con=engine)

    # Apagando colunas
    df.drop(['id', 'dt_reg', 'horario'], axis=1, inplace=True)
    
    # Salvando os arquivos
    if df.equals(sql_sensors_AMT):
        df_name = 'sensores_AMT.csv'
    elif df.equals(sql_sensors_CSN_CP03_A):
        df_name = 'sensores_CSN_CP03_A.csv'
    elif df.equals(sql_sensors_CSN_CP03_B):
        df_name = 'sensores_CSN_CP03_B.csv'
    return
    df.to_csv(filepath+df_name)


for current_sql_sensor in range(len(sensors_list)):
    etl(sensors_list[current_sql_sensor])

# Extraindo último update dos dataframes salvos localmente
first_row_df_sensors_AMT = pd.read_csv(filepath+'sensores_AMT.csv')
last_update_AMT = first_row_df_sensors_AMT['DataHora'].iloc[:1]
 
first_row_df_sensors_CSN_CP03_A = pd.read_csv(filepath+'sensores_CSN_CP03_A.csv')
last_update_CSN_CP03_A = first_row_df_sensors_CSN_CP03_A['DataHora'].iloc[:1]
 
first_row_df_sensors_CSN_CP03_B = pd.read_csv(filepath+'sensores_CSN_CP03_B.csv')
last_update_CSN_CP03_B = first_row_df_sensors_CSN_CP03_B['DataHora'].iloc[:1]

df_list = [first_row_df_sensors_AMT, first_row_df_sensors_CSN_CP03_A, first_row_df_sensors_CSN_CP03_B]

def check_for_drop_or_peak(df):
    df_column_number = len(df.axes[1])-1
    
    if df.equals(first_row_df_sensors_AMT):
        df_name = 'AMT'
    elif df.equals(first_row_df_sensors_CSN_CP03_A):
        df_name = 'CSN CP03 A'
    elif df.equals(first_row_df_sensors_CSN_CP03_B):
        df_name = 'CSN CP03 B'
    
    # if ou try except para carregar o arquivo local, checar se ja foi enviado o email
    for current_column in range(1, df_column_number):
        temp_value = df.iloc[0][f'temp{current_column}']
        if temp_value == 0:
            # save local do arquivo, criando 1 linha de info mais recente
            # email_sender(current_column, df_name)
            print(f'O sensor em temp{current_column} do painel {df_name} apresentou um valor de temperatura igual a 0°C, podendo estar inoperante')
        elif temp_value < 700:
            # email_sender(current_column, df_name)
            print(f'A temperatura do sensor em temp{current_column} do painel {df_name} apresentou uma queda de temperatura abaixo de 700°C')
        elif temp_value > 1300:
            print(f'A temperatura do sensor em temp{current_column} do painel {df_name} apresentou um pico de temperatura acima de 1300°C')
        else:
            # email_sender(current_column, df_name)
            print(f'A temperatura do sensor em temp{current_column} do painel {df_name} apresentou um valor de temperatura dentro do esperado, sendo assim está operando normalmente')

for current_dataframe in range(len(df_list)):
    check_for_drop_or_peak(df_list[current_dataframe])
    print('-='*55)
