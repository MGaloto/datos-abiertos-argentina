# datos-abiertos-argentina

<p align="center">
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1280px-Apache_Spark_logo.svg.png" alt="react" width="300" height="150" />
</p>

Analisis y pronosticos de transferencia de automotores en Argentina.

Trabajo en curso sobre datos de transferencias de automotores para Argentina desde 2018 a 2022. El proposito final sera, luego de limpiar y visualizar los datos, generar series temporales a los efectos de poder predecir las transacciones futuras de compra y venta de autos usados. Se utilizaran distintos modelos de Time Series evaluando sus metricas y precision para estimar valores futuros.


<p>
<a href="https://scikit-learn.org/stable/" rel="nofollow"><img src="https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/Python.svg/1024px-Python.svg.png" align="right" width="200" style="max-width: 100%;"></a>
</p>


# Presentacion

El trabajo final se presenta en una Notebook de Jupyter y en un documentos R Markdown con el analisis de las series temporales y los pronosticos.

# Incluye

<ui>

<li>
Extraccion, Transformacion y Carga de Datos.
</li>

<li>
Limpieza del Data Set.
</li>

<li>
Graficos Dinamicos y Estaticos.
</li>

<li>
Time Series.
</li>

</ui>

# Serie Temporal 

A continuacion una muestra de la serie temporal diaria con una media movil para suavizarla y poder visualizar si existe estacionalidad. 

<p align="center">
  <img 
    width="550"
    height="300"
    src="img/gf.gif"
  >
</p>


Los datos se pueden descargar de: 

<ui>
<li>
https://www.datos.gob.ar/
</li>
</ui>

Para descomprimir los archivos necesitamos las siguientes librerias y tener las url de los documentos .zip y guardarlos en _urls.txt_. 

Con el siguiente codigo se descrompimen los archivos en la carpeta y, por ultimo, se concatenan cada uno de los .csv que hay dentro usando concat de pandas.

```python

from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
from os import listdir

def find_csv_filenames(path_to_dir, suffix=".csv" ):
    filenames = listdir(path_to_dir)
    return [ filename for filename in filenames if filename.endswith( suffix ) ]

csv_files = [l for l in find_csv_filenames(folder_archivos, suffix=".csv" ) if 'autos' in l]

```


```python

with open('urls.txt', 'r') as folder_urls:
    lines_urls = folder_urls.readlines()

if len(csv_files) == 0:
    zipurls = lines_urls

    for zipurl in zipurls:
        with urlopen(zipurl) as zipresp:
            with ZipFile(BytesIO(zipresp.read())) as zfile:
                zfile.extractall(folder_archivos)

```


```python

list_data_frames = []

for file in csv_files:
    df_new = pd.read_csv(file)
    list_data_frames.append(df_new)

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

df = unionAll(*list_data_frames)

df = df.groupBy(['tramite_fecha','registro_seccional_provincia']).agg(count("registro_seccional_provincia").alias("count"))

df.toPandas().to_csv('df.csv')

```

Si estuvieramos usando una base de datos estructurada se pueden hacer consultas desde la misma base de datos utilizando Pyspark con codigo Sql.

Ejemplo del codigo para la query:

```python

import findspark
findspark.init()
# Se importa el contexto de Spark
from pyspark import SparkContext 
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# El inicializador se encuentra en local bajo el nombre autos
sc = SparkSession.builder.master('local').appName('autos').getOrCreate()

# Con SQLContext vamos a poder utilizar lenguaje SQL
sqlContext = SQLContext(sc)


df = sc.read.format("jdbc") \
    .option("url","jdbc:mysql://localhost/santander_db") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("dbtable","clientes_hb") \
    .option("user","<user>") \
    .option("password","<password>") \
    .option("query", "<query sql>").load()

# Accion
df.show()


```

Continua..