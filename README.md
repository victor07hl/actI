# Gobernanza del dato
## Instrucciones de uso
instalar las librerias documentadas en el archivo de `requirements.txt`, preferiblemente en un ambiente virtual de python para evitar conflictos de dependencias.

las credenciales para la conexion al sql server son creadas utilizando variables de entorno, se deja el siguiente ejemplo:

`msql_user=sa` <br>
`msql_pwd=1234` <br>
`server_ip=localhost` <br>
`db=master`

## Configuracion de ambiente
- SQL Server: <br>
&nbsp;&nbsp;&nbsp;&nbsp; El servidor de SQL utilizado se desplego utilizando una
imagen de docker desarrollada por microsoft para realizar pruebas. <br>
Para utilizarlo ejecuta estos dos comandos <br>
1. `docker pull mcr.microsoft.com/mssql/server` <br>
2. `docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=1234" -p 1433:1433 --name mysql --hostname mysql mcr.microsoft.com/mssql/server` <br>
- Enlace de la documentacion oficial: <https://hub.docker.com/r/microsoft/mssql-server>.


## Archivos de configuracion
En la carpeta configs estan los archivos de `terrazas_dimensions.json` y `variables.json`. El primero contiene la informacion de las columnas que cada dimension tendra a a partir del archivo de Licencias_Terrazas_Integradas.csv, este se puede modificar para crear mas dimensiones o modificar las que ya estan escritas. El segundo archivo contiene las rutas desde donde se leean los archivos sin procesar y las rutas en donde se quiere guardar la data procesada.

## Ejecucion del Proceso
El archivo encargado de ejecutar todo el proceso es `dwh.py`, es decir, es el unico que se necesita ejecutar para hacer todo el proceso de transformacion y creacion del datawarehouse. 


