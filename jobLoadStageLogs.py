#
## 
from _env.env import DBBASE, DBHOST, DBPASS, DBUSER
import time 
import findspark
from datetime import datetime, date
from mod.connDB import conectaBase
from mod.customLogs import gravaLog

#
## Spark Session
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *

#
## Inicializacao de Variaveis
horaAtual = datetime.now().strftime('%H:%M')
timestampLog = datetime.now().strftime('%Y%m%d_%H%M%S')
logName = "pysqlfix_" + timestampLog + ".log"
tbStg = 'iiot_bi_stg_manut_logs'
pathJDBC='/opt/di/spark/iotpipelines/lib/mysql-connector-java-8.0.25.jar'
urlDB = 'jdbc:mysql://'+DBHOST+':3306/'+DBBASE

#
## Ano mes atual
def getAnoMes(conectaBase):
  sqlCursor = conectaBase.cursor()
  sqlQuery = "select year(now()) ano, month(now()) mes"
  sqlCursor.execute(sqlQuery)
  sqlDados = sqlCursor.fetchall()
  for sqlRecord in sqlDados:
    ano = sqlRecord[0]
    mes = sqlRecord[1]
  sqlCursor.close()
  return ano, mes
ano, mes = getAnoMes(conectaBase)

#
## Main
def main():
    ano, mes = getAnoMes(conectaBase)
    msg="Iniciando manutencao no banco de dados:" + str(ano) + "-" + str(mes)
    gravaLog(1, logName, timestampLog, msg)

    spark = SparkSession \
        .builder \
        .appName('IoT Pipelines - Stage Logs') \
        .master('local[*]') \
        .config("spark.driver.extraClassPath", pathJDBC) \
        .getOrCreate()

    sch = StructType() \
        .add("cd_exec",StringType(),True) \
        .add("ds_exec",StringType(),True) 

    df = spark.read.format("csv") \
        .option("header", False) \
        .option("delimiter", "|") \
        .schema(sch) \
        .load("/opt/di/logs/pysqlfix*")

    df.show()
    df.printSchema()

    # write it to the table
    df.write.format('jdbc').options(
            url=urlDB,
            driver='com.mysql.cj.jdbc.Driver', 
            dbtable=tbStg,
            user=DBUSER,
            password=DBPASS,
        ).mode('append').save()

if __name__=="__main__":
    main()
