from ts2.etl.feeder import Feeder
from ts2.etl.indexed_file_loader import ETLConfiguration
from ts2.util.copier import Copier

dirs = [('/home/andrew/data/nick/behaviors_temp', '/home/andrew/data/nick/registered_bv'),\
	('/home/andrew/data/nick/images_temp', '/home/andrew/data/nick/registered_im')]
copier = Copier(dirs)
copier.start()
