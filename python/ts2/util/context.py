from ts2.db.manager import HBaseManager
from ts2.util.utils import grouper
from ts2.etl.feeder import Feeder
from ts2.data.streaming_data import StreamingData
from ts2.data.streaming_series import StreamingSeries
from ts2.data.streaming_images import StreamingImages
from pyspark.streaming import StreamingContext
from pyspark.streaming.dstream import DStream
from pyspark.serializers import NoOpSerializer, PairDeserializer
import os

from thunder import Series, Images

from ts2.util.log import warningLog
import ts2.settings as settings

from py4j.java_gateway import java_import, Py4JJavaError
from py4j.java_collections import ListConverter

import numpy as np

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

    def start_feeder(self):
        if not self._feeder:
            warningLog("Cannot start until a streaming configuration file has been loaded.")
        self._feeder.start()

    def start_streaming(self):
        self.ssc.start()

    def stop_streaming(self):
        for loader in self.dstream_loaders:
            loader.stop()
        self.ssc.stop(stopSparkContext=False)

    def _loadBytes(self, datasetId=DATA_KEY, minTime=0, maxTime=10):
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

    def loadImages(self, datasetId=DATA_KEY, dtype='uint16', dims=None, minTime=0, maxTime=10):
        bytes = self._loadBytes(datasetId, minTime, maxTime)
        rdd =  bytes.map(lambda (k, v): (k, np.fromstring(v, dtype=dtype).reshape(dims)))
        return Images(rdd, dims=dims, dtype=dtype)

    def loadSeries(self, datasetId=DATA_KEY, dtype='uint16', minTime=0, maxTime=10):
        bytes = self._loadBytes(datasetId, minTime, maxTime)
        keyed_rdd = bytes.flatMap(lambda (k, v): (k, map(lambda (idx, vi): (idx, (k, vi)),
                                                         list(enumerate(np.fromstring(v, dtype=dtype))))))
        rdd = keyed_rdd.groupByKey().map(lambda (k, v): (k, map(lambda (ki, vi): vi, sorted(v))))
        return Series(rdd, dtype=dtype)

    def _loadBytesDStream(self, datasetId=DATA_KEY):
        """
        """
        jvm = self._sc._jvm
        java_import(jvm, "thunder_streaming.receivers.*")

        feeder_conf = self._feeder.conf
        ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())

        try:
            # TODO: are there closure problems with this approach? (why do Jascha/KafkaUtils do it differently?)
            dstream = DStream(
                self.ssc._jssc.receiverStream(jvm.HBaseReceiver(
                    ListConverter().convert(feeder_conf.get_sequence_names(), jvm._gateway_client),
                    settings.BASE_COL_FAM,
                    datasetId,
                    settings.MAX_KEY,
                    self.batch_time)),
                self.ssc, ser)
            return dstream.map(lambda kv: (kv[0], bytes(kv[1])))
        except Py4JJavaError as e:
            print "Could not create the synchronized DStream."
            raise e

    def loadSeriesDStream(self, datasetId=DATA_KEY, dtype='uint16'):
        bytes = self._loadBytesDStream(datasetId)
        return StreamingSeries(bytes, dtype)

    def loadImagesDStream(self, datasetId=DATA_KEY, dtype='uint16'):
        bytes = self.loadBytesDStream(datasetId)
        return StreamingImages(bytes, dtype)

    # TODO: Load regressors