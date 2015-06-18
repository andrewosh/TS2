from ts2.db.manager import HBaseManager
from ts2.data.dstream_loader import DStreamLoader
from ts2.util.utils import grouper
from ts2.etl.feeder import Feeder
from pyspark.streaming import StreamingContext
from pyspark.streaming.dstream import DStream
from pyspark.serializers import NoOpSerializer
import os

from ts2.util.log import warningLog

import ts2.settings as settings

from py4j.java_gateway import java_import

class ThunderStreamingContext(object):
    """
    The streaming context is responsible for creating DStreams and RDDs based on data stored in a time-ordered
    database.
    """

    DATA_KEY = 'data'
    REGRESSOR_KEY = 'regressor'

    def __init__(self, tsc, sc, batch_time=5):
        self._tsc = tsc
        self._sc = sc
        self.ssc = StreamingContext(sc, batch_time)

        self.rows_per_partition = 5

        self.dstream_loaders = []
        self._poll_time = 3
        self.batch_time = batch_time

        self._feeder = None
        self._hbase_manager = None

    def loadConfig(self, filename=None):
        """
        :param filename:  The name of the ETL configuration file to load (by default, it will use the file passed in
        as the first argument to thunder_streaming, which is stored in the ETL_CONFIG environment variable
        :return:
        """
        if not filename:
            filename = os.environ.get('ETL_CONFIG')
        if not filename:
            warningLog("Could not load a configuration file (did you pass one in as an argument to thunder_streaming?).")
            return
        manager = HBaseManager()
        feeder = Feeder(filename, manager)
        self._hbase_manager = manager
        self._feeder = feeder

    def set_partition_size(self, rows_per_partition):
        """
        Sets the number of rows to include in each partition

        :param batch_size:
        :return:
        """
        self.rows_per_partition = rows_per_partition

    def start(self):
        if not self._feeder:
            warningLog("Cannot start until a streaming configuration file has been loaded.")
        self._feeder.start()

    def stop(self):
        for loader in self.dstream_loaders:
            loader.stop()

    def loadBytes(self, datasetId=DATA_KEY, minTime=0, maxTime=10):
        def _lb(first, last):
            # TODO optimize this
            manager = HBaseManager()
            manager.initialize()
            keyed_byte_arrs = manager.get_rows(datasetId, minTime, maxTime)
            if len(keyed_byte_arrs) == 0:
                return []
            return keyed_byte_arrs
        chunk_iter = grouper(range(minTime, maxTime), self.rows_per_partition)
        chunk_rdd = self._sc.parallelize([(group[0], group[-1] + 1) for group in chunk_iter])
        return chunk_rdd.flatMap(lambda (first, last): _lb(first, last))

    def loadImages(self, datasetId=DATA_KEY, minTime=0, maxTime=-1):
        pass

    def loadSeries(self, datasetId=DATA_KEY, minTime=0, maxTime=-1):
        pass

    def loadBytesDStream(self, datasetId=DATA_KEY):
        from py4j.java_collections import ListConverter
        jvm = self._sc._jvm
        java_import(jvm, "thunder_streaming.receivers.*")
        feeder_conf = self._feeder.conf
        return DStream(jvm.HBaseReceiver.apply(
            self.ssc._jssc,
            ListConverter().convert(feeder_conf.get_sequence_names(), jvm._gateway_client),
            settings.BASE_COL_FAM,
            datasetId,
            settings.MAX_KEY,
            self.batch_time
        ), self, NoOpSerializer())

    def loadSeriesDStream(self, datasetId=DATA_KEY):
        pass

    def loadImagesDStream(self, datasetId=DATA_KEY):
        pass

    # TODO: Remove the following obsolete methods

    """
    def getBytesDStream(self, datasetId=DATA_KEY):
        q = []

        dstream = self.ssc.queueStream(q)
        loader = DStreamLoader(self, datasetId, q, self._poll_time)
        self.dstream_loaders.append(loader)
        print "Starting DStreamLoader in background..."
        loader.start()
        return dstream

    def getImagesDStream(self, datasetId=DATA_KEY):
        pass

    def getSeriesDStream(self, datasetId=DATA_KEY):
        pass
    """

    # TODO: Load regressors