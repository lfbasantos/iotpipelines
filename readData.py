#
## Spark Session
import time 
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from datetime import datetime, date
from pyspark.sql import Row


#
## Main
def main():
    conf=SparkConf().setAppName("IoT Pipelines - ReadData")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    df = sql.createDataFrame ([
        Row(a=1, b=2, c='string1', d=date(2021,7,14), e=datetime(2021,7,14,9,0)),
        Row(a=3, b=4, c='string2', d=date(2021,7,14), e=datetime(2021,7,14,9,1)),
        Row(a=5, b=6, c='string3', d=date(2021,7,14), e=datetime(2021,7,14,9,2))
    ])
    df.show()
    time.sleep(10)
    df.printSchema()
    time.sleep(5)

if __name__=="__main__":
    main()
