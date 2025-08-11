# Desarrollo de prueba - Ingeniero de datos
# Ricardo Isaza Barrera



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

**Ver archivo Cargue_bodega.py**

A continuacion, se adjunta el query que genera por cliente unico la solicitado:

**Ver archivo Cargue_bodega.py**


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
