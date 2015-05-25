from ts2.etl.feeder import Feeder
import os

# Very simple launch script...
etl_conf = os.environ['ETL_CONFIG']
f = Feeder(etl_conf)
f.start()
