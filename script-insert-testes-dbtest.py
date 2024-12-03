import pandas as pd
from sqlalchemy import create_engine

df_espessuras = pd.read_excel(r'H:\Business Intelligence\Clientes\AMP\Redução\Base_Dados_Espesssuras_Infinity.xlsx')
# Formato do envio mysql+mysqlconnector://<user>:<password>@<host/ip>/dbtogo
url = 'mysql+mysqlconnector://tmxinsertuser:moasf0=!$as9zx@shwdbmysql01.mysql.database.azure.com/amp_testes'

# Estabelecendo conexão com o banco de dados do MySQL
engine = create_engine(url)
connect_to_sql = engine.connect()

df_espessuras.to_sql(name='amp_testes', con=engine, if_exists='append', index=False)

# Querys efetuados no banco para formar os dataframes
# query = 'SELECT * FROM amp_testes.amp_testes ORDER BY id DESC" ;'
# df_db_connection_test = pd.read_sql(query, con=engine)
connect_to_sql.close()
