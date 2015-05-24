from ts2.etl.indexed_file_loader import FileLoadManager, ETLConfiguration
from ts2.etl.hbase_synchronizer import HBaseSynchronizer

class Feeder(object):
    """
    The Feeder loads in time-indexed files and inserts them into an HBase database
    """

    def start(self, conf_file):
        conf = ETLConfiguration.load_from_json(conf_file)
        hbs = HBaseSynchronizer()
        flm = FileLoadManager(conf, hbs)

        flm.start()