import pandas as pd
import numpy as np
import pyodbc

df = pd.read_csv(r"C:\Users\EQP1RIS\Desktop\historia.csv",sep=';')

#Conexion bodega
con = pyodbc.connect('DSN=impalanube', autocommit=True)
cursor = con.cursor()

#Crea estructura de la tabla en bodega
sql_create ='''
CREATE EXTERNAL TABLE temporal.bd_historia_prueba (
identificacion STRING,
corte_mes STRING,
saldo STRING
)
STORED AS PARQUET
'''

cursor.execute(sql_create)


#Se agregan los registros a la tabla creada en el paso anterior
sql_insert = '''
INSERT INTO temporal.bd_historia_prueba (
identificacion,
corte_mes,
saldo
)
VALUES
(?,?,?)
'''

values = df.values.tolist()
cursor.fast_executemany = True
cursor.executemany(sql_insert, values)

df2 = pd.read_csv(r"C:\Users\EQP1RIS\Desktop\retiros.csv",sep=';')

sql_create ='''
CREATE EXTERNAL TABLE temporal.bd_retiros_prueba (
identificacion STRING,
fecha_retiro STRING
)
STORED AS PARQUET
'''

cursor.execute(sql_create)

sql_insert = '''
INSERT INTO temporal.bd_retiros_prueba (
identificacion,
fecha_retiro
)
VALUES
(?,?)
'''

values2 = df2.values.tolist()
cursor.fast_executemany = True
cursor.executemany(sql_insert, values2)