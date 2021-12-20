# tfm-gentrification-prediction

![UOC](https://upload.wikimedia.org/wikipedia/commons/a/a3/Logo_blau_uoc.png)

Este es el repositorio del código correspondiente al desarrollo del proyecto de final
de máster en Ciencia de Datos de la UOC con título "Estudio de los factores de gentrificación de los barrios de las
grandes ciudades".

Para ejecutar el código una vez clonado, es necesario instalar los requerimientos utilizando
el comando `pip install -r requierements.txt`. En Windows puede dar algún problema
al installar la librería `geopandas`, pero en Linux funciona perfectamente.

Una vez ejecutado el comando, ya se puede ejecutar el código con la instrucción `python __main__.py`.

Es importante configurar el fichero `config.yaml` con los parámetros adecuados para
ejecutar las partes deseadas. En este fichero, en el valor de `steps` se pueden definir
estos pasos que se quieren ejecutar, los cuales pueden ser los siguientes:

- **l1**: Transformaciones iniciales de ficheros crudos de origen
- **l2**: Transformaciones intermedias de los ficheros generados en la fase anterior
- **l3**: Generación del dataset que tiene la evolución de variables de 2015 a 2018 por barrio
- **geo**: Añade al dataset anterior los datos geoespaciales de cada barrio
- **analysis**: Genera el dataset que se subirá a Flourish con la distribución de ubicaciones/negocios por barrio en 2019 
- que será representado con gráficos de radar
- **clustering**: Prepara los datos y ejecuta el algoritmo de k-means
- **kpi_evo**: Genera el dataset que se subirá a Flourish para crear la visualización de gráficos de líneas de la evolución 
- de las distintas variables en los barrios

En el repositorio no está incluida la parte de datos, por lo que los ficheros CSV
tanto crudos como procesados deben descargarse del siguiente enlace de 
[Google Drive](https://drive.google.com/file/d/1jUgGEMegduvzZ85p3KPZ2tJiPiMMEyBv/view?usp=sharing).

Una vez descomprimido el fichero, la carpeta `data` debe estar en la raíz del proyecto.

