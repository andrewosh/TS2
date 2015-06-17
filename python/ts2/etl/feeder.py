from ts2.etl.indexed_file_loader import FileLoadManager, ETLConfiguration
from ts2.db.manager import HBaseManager

class Feeder(object):
    """
    The Feeder loads in time-indexed files and inserts them into an HBase database
    """

    def __init__(self, conf_file, db_manager):
        self.conf_file = conf_file
        self.db_manager =  db_manager
        self.conf = ETLConfiguration.load_from_json(self.conf_file)

        # Ensure that the database manager knows which columns to read
        self.db_manager.set_sequence_names(self.conf.get_sequence_names())

    def start(self):
        flm = FileLoadManager(self.conf, self.db_manager)
        flm.start()