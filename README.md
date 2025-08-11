# Prueba-Ingeniero-datos
Desarrollo de prueba vacante - Ingeniero de datos


### Desarollo punto 1
Diseñar e implementar un proceso automatizado y controlado mediante prácticas de CI/CD para la creación,
validación, despliegue y mantenimiento de un dataset confiable de números de teléfono de clientes. Este
dataset será utilizado para mejorar la comunicación y el servicio al cliente.


1.	Se construye un query SQL en donde se integran las diferentes fuentes de donde se extraen los clientes de la compañía y sus respectivos números de contacto, aplicamos filtros de negocio de ser necesarios y dejamos el resultado final a nivel cliente.

2.	Procedemos a ejecutar el query para determinar que no contiene fallos , en caso de darse algun inconveniente devolverse al paso a la construcción del query

3.	Validamos tiempos de ejecución y consumo de recursos , en caso de ser necesario, replantear la construcción del datset

4.	Procedemos a aplicar validaciones de formatos:

* Los campos de números sean formato numérico 
* Los campos tengan las longitudes correspondientes a números celulares y teléfonos fijos
* Comprobar que no haya registros con los campos en nulos

5.	Al comprobar que todo cumpla con lo esperado , procedemos a integrarlo al datalake para consumo del cliente final



### Desarollo punto 2
Con base en el resultado del ejercicio conceptual de creación de dataset, plantea también de forma
conceptual un mecanismo/herramienta que permita hacer veeduría de la calidad de datos, trazabilidad del
dato, etc. Esta será un recurso para los equipos de negocio para obtener KPI's acerca de los télefonos de los
clientes.

- Que tengan los prefijos validos
- Que tengan la cantidad correcta de dígitos
- Que el mismo número no se encuentre repetido entre clientes
- Comprobar la existencia de Nulos
- Validar que dentro de los valores no se incluyan caracteres no numéricos


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

A continuacion, se adjunta el query que genera por cliente unico la solicitado:

```sql
WITH HIST AS 
(
    /* BASE_INICIAL_HISTORIA */
    SELECT 
    identificacion,
    to_timestamp(corte_mes,'dd/MM/yyyy') AS corte_mes,
    SUM(CAST(saldo AS BIGINT)) AS saldo,
    CASE 
    WHEN SUM(CAST(saldo AS BIGINT)) >= 0 AND SUM(CAST(saldo AS BIGINT)) < 300000 THEN 'N0'
    WHEN SUM(CAST(saldo AS BIGINT)) >= 300000 AND SUM(CAST(saldo AS BIGINT)) < 1000000 THEN 'N1'
    WHEN SUM(CAST(saldo AS BIGINT)) >= 1000000 AND SUM(CAST(saldo AS BIGINT)) < 3000000 THEN 'N2'
    WHEN SUM(CAST(saldo AS BIGINT)) >= 3000000 AND SUM(CAST(saldo AS BIGINT)) < 5000000 THEN 'N3'
    WHEN SUM(CAST(saldo AS BIGINT)) >= 5000000 THEN 'N4'
    ELSE 'OJO'
    END AS nivel_deuda
    FROM temporal.bd_historia_prueba
    GROUP BY 1,2
),
RET AS 
(
    /* BASE_INICIAL_RETIROS */
    SELECT 
    identificacion,
    TO_TIMESTAMP(fecha_retiro,'yyyyMMdd') AS fecha_retiro
    FROM temporal.bd_retiros_prueba
),
BD1 AS 
(
    /* SE ESTABLECE LA FECHA DE REFERENCIA */
    SELECT 
    TO_TIMESTAMP('20240622','yyyyMMdd') AS fecha_base,
    HIST.identificacion,
    HIST.corte_mes,
    LAG(HIST.corte_mes,1) OVER(PARTITION BY HIST.identificacion ORDER BY HIST.corte_mes ASC) AS corte_mes_ant,
    HIST.saldo,
    HIST.nivel_deuda,
    LAG(HIST.nivel_deuda,1) OVER(PARTITION BY HIST.identificacion ORDER BY HIST.corte_mes ASC) AS nivel_deuda_ant,
    RET.fecha_retiro
    FROM HIST 
    LEFT JOIN RET ON HIST.identificacion = RET.identificacion
),
BD2 AS 
(
    /* SE VALIDA CUANDO EXISTE UN SALTO DE CORTE_MES */
    SELECT 
    *,
    INT_MONTHS_BETWEEN(corte_mes,corte_mes_ant) AS dist_meses,
    CASE 
    WHEN corte_mes < fecha_retiro AND INT_MONTHS_BETWEEN(corte_mes,corte_mes_ant) > 1 THEN INT_MONTHS_BETWEEN(corte_mes,corte_mes_ant)-1
    ELSE 0
    END AS suma_saldo_n0,
    IF(nivel_deuda = nivel_deuda_ant,1,0) AS meses_consec
    FROM BD1
),
BD3 AS 
(
    SELECT 
    *,
    IF(meses_consec = 1 ,LAG(meses_consec,1) OVER(PARTITION BY identificacion ORDER BY corte_mes),0) AS meses_consec_ant
    FROM BD2
),
BD4 AS 
(
    /* SE VALIDA QUE LAS RACHAS CUMPLAN CON UN NUMERO N , EN ESTE CASO = 1 */
    SELECT 
    fecha_base,
    identificacion,
    corte_mes,
    saldo,
    nivel_deuda,
    fecha_retiro,
    suma_saldo_n0+meses_consec+meses_consec_ant AS racha,
    CASE 
    WHEN suma_saldo_n0+meses_consec+meses_consec_ant >= 1 THEN 'SI'
    ELSE 'NO'
    END AS cumple_n_rachas
    FROM BD3
),
BD5 AS 
(
    /* SE USA ROW_NUMBER PARA JERARQUIZAR Y DEJAR REGISTROS UNICOS */
    SELECT 
    fecha_base,
    identificacion,
    corte_mes,
    saldo,
    nivel_deuda,
    fecha_retiro,
    racha,
    cumple_n_rachas,
    ROW_NUMBER() OVER(PARTITION BY identificacion ORDER BY cumple_n_rachas DESC ,racha DESC, DATEDIFF(fecha_base,corte_mes) ASC) AS rankin
    FROM BD4
)
/* SE FILTRA POR LOS CRITERIOS ESTABLECIDOS */
SELECT 
fecha_base,
identificacion,
corte_mes,
saldo,
nivel_deuda,
fecha_retiro,
racha
FROM BD5
WHERE rankin = 1
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


```python
import os
import re
import base64


# Encuentra en las etiquetas <img> el archvio de la imagen
patron = re.compile(r'<img[^>]+src="([^">]+)"')

# Diccionario
imagenes_base64 = {}

for i in bd_html:
    with open(i, "r", encoding="utf-8") as img:
        html = img.read()

        # Buscar imágenes
        bases_img = patron.findall(html)
        lista_64 = []

        for x in bases_img:
            with open(x, "rb") as img:
                cod_64 = base64.b64encode(img.read()).decode("utf-8")
                lista_64.append((x, cod_64))

imagenes_base64[i] = base64_images
```

c): Reemplazar las imágenes originales del HTML por las codificadas en base64, sin sustituir el archivo
original, es decir, creando uno nuevo.

--Sin resolver

d): Debe generar un objeto que contenga la lista de imágenes procesadas de forma exitosa y las que
fallaron

--Sin resolver
