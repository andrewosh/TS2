from ts2.etl.indexed_file_loader import Synchronizer
import happybase
import ts2.settings as settings
from ts2.util.log import debugLog
from math import log

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

        # The base columns are set later by set_sequence_names
        self.base_cols = None

        # The scan filter can be set once the base columns are known
        self.scan_filter = None

    def set_sequence_names(self, names):
        self.base_cols = names
        self.scan_filter = ' AND '.join(map(lambda name: 'new SingleColumnValueFilter("%s", "%s", CompareOp.EQUAL, "%s")\.setFilterIfMissing(true)'\
                                            % (settings.BASE_COL_FAM, settings.BASE_COL_QUALIFIER, name), names))

    def terminate(self):
        self.conn.close()
        self.table = None

    def _get_padded_key(self, key):
        """
        Since keys are stored lexicographically, they must be zero-padded based on a maximum key value

        :param key:
        :return:
        """
        key_str = str(key)
        return ('0' * ((int(log(settings.MAX_KEY, 10)) + 1) - len(key_str))) + key_str

    def _get_qualified_name(self, col):
        return ':'.join([settings.BASE_COL_FAM, settings.BASE_COL_QUALIFIER, col])

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
                ':'.join([settings.BASE_COL_FAM, settings.BASE_COL_QUALIFIER, id]): data
            }
            debugLog("Inserting %s into %s" % (data_dict.keys()[0], str(idx)))
            self.table.put(self._get_padded_key(idx), data_dict)

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
        rows = self.table.scan(row_start=self._get_padded_key(first), row_stop=self._get_padded_key(last),
                               filter=self.scan_filter)
        if len(rows) != (last - first):
            # Don't return a set of rows that don't contain a complete set of synchronized data
            return None
        # Remove the zero padding and extract the dataset's column
        return map(lambda (row_key, row_data): (int(row_key), row_data[self._get_qualified_name(id)]))
