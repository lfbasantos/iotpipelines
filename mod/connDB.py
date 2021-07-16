#
##
import sys 
sys.path.insert(0, '/opt/di/spark/iotpipelines/_env')
from env import DBBASE, DBHOST, DBPASS, DBUSER
import mysql.connector

#
## Conexao com a base
conectaBase = mysql.connector.connect(
  host=DBHOST,
  user=DBUSER,
  passwd=DBPASS,
  database=DBBASE
)
