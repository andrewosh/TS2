from ts2.etl.feeder import Feeder
from ts2.etl.indexed_file_loader import ETLConfiguration
from ts2.util.copier import Copier
import os

# Very simple launch script...
etl_conf = os.environ['ETL_CONFIG']
f = Feeder(etl_conf)
f.start()
