# Prueba-Ingeniero-datos
Desarrollo de prueba vacante - Ingeniero de datos


### Desarollo punto 3
Cargue de data
Del archivo insumo original rachas.xlsx se extraen las dos hojas (historia-retiros), se guardan en formato csv y se suben a la bodega a traves del siguiente script:

```python
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


```



### Desarrollo punto 4

a): Recibir bien sea un listado de archivos HTML a procesar o un listado de directorios en los cuales se encuentran archivos HTML para procesar (incluyendo subdirectorios)

```python
def seek_and_store_html(lista):
    bd_html = [] #lista vacia
    for i in lista:
        if os.path.isfile(i) and i.endswith('html'): #valida si es un archivo con extension html
            bd_html.append(i) #agrega el html a la lista
        elif os.path.isdir(i): #si el elemento es una carpeta
            for dirpath, dirnames, filenames in os.walk(i): #recorre la carpeta
                for x in filenames:
                    if os.path.isfile(i) and i.endswith('html'):
                        bd_html.append(os.path.join(dirpath, file))
    
    return bd_html
 ```
 
b): Recorrer el listado completo de archivos HTML y determinar para cada archivo cuáles son las imágenes que tiene asociadas (puede asumir que todas se encuentran con el tag ) y convertirlas a base64 (https://en.wikipedia.org/wiki/Base64).
