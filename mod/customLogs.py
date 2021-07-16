#
## Integraiot Pipelines
import sys 
sys.path.insert(0, '/opt/di/spark/iotpipelines/_env')
from datetime import datetime
from env import PATHLOG

#
##
def gravaLog(tp, logName, timestampLog, msg):
  fileLog = open(PATHLOG + logName, "a")
  hr = datetime.now().strftime('%Y%m%d_%H%M%S')
  if tp==1:
    flag_tp = "[i]"
  elif tp==2:
    flag_tp = "[k]"
  elif tp==3:
    flag_tp = "[w]"
  elif tp==4:
    flag_tp = "[E]"
  else:
    flag_tp = "[X]"
  fileLog.write(timestampLog + "|" + hr + ": "+ flag_tp + ":" + msg + "\n")
  print(timestampLog + "|" + hr + "|"+ flag_tp + "|" + msg)
  fileLog.close()