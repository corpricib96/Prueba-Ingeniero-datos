# Prueba-Ingeniero-datos
Desarrollo de prueba vacante - Ingeniero de datos


Desarrollo punto 4

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
