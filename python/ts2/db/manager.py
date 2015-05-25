from ts2.etl.indexed_file_loader import Synchronizer
import happybase
import ts2.settings as settings
from ts2.util.log import debugLog

class HBaseManager(Synchronizer):
    """
    Synchronizes a time-indexed list of byte-arrays, each list associated with a given sequence ID (so that multiple
    sequences can be associated with the same time indices) by inserting the data into an HBase table
    """

    def initialize(self):
        self.conn = happybase.Connection(settings.HBASE_HOST)
        if settings.HBASE_TABLE not in set(self.conn.tables()):
            self.conn.create_table(settings.HBASE_TABLE, settings.HBASE_FAMILIES)

        self.table = self.conn.table(settings.HBASE_TABLE)

    def terminate(self):
        self.conn.close()
        self.table = None

    def synchronize(self, sequence_id, data_list):
        """
        Inserts a list of time-indexed files, associated with the 'id' dataset, into an HBase table for
        time synchronization and later retrieval.

        :param sequence_id:
        :param data_list:
        :return:
        """
        for (id, idx, data)  in data_list:
            data_dict = {
                settings.BASE_COL_FAM + ':' + id: data
            }
            debugLog("Inserting %s into %s" % (data_dict.keys()[0], str(idx)))
            self.table.put(idx, data_dict)

    def get_all_rows(self, id):
        """
        Returns all the rows, in order, from the dataset specified by id

        :param id: the dataset identifier
        :return: a byte array
        """
        pass

    def get_rows(self, id, first, last):
        """
        Returns a range of rows, in order, from the dataset specified by id.

        :param first: row value in the range [0, len(dataset)), or None for first index
        :param last: row value in the range (first, len(dataset)), or None for last index
        :return: a byte array
        """


