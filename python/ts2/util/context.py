from ts2.db.manager import HBaseManager
from ts2.data.dstream_loader import DStreamLoader
from ts2.util.utils import grouper
from Queue import Queue
from pyspark.streaming import StreamingContext

class ThunderStreamingContext(object):
    """
    The streaming context is responsible for creating DStreams and RDDs based on data stored in a time-ordered
    database.
    """

    DATA_KEY = 'data'
    REGRESSOR_KEY = 'regressor'

    def __init__(self, tsc, sc, feeder, batch_time=5):
        self._tsc = tsc
        self._sc = sc
        self._feeder = feeder
        self.ssc = StreamingContext(sc, batch_time)

        self.rows_per_partition = 5

        self.dstream_loaders = []
        self._poll_time = 3

    def set_partition_size(self, rows_per_partition):
        """
        Sets the number of rows to include in each partition

        :param batch_size:
        :return:
        """
        self.rows_per_partition = rows_per_partition

    def start(self):
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
            # TODO Convert the byte array into N chunks of bytes
            return [chunk for chunk in grouper(keyed_byte_arrs, last - first)]
        chunk_iter = grouper(range(minTime, maxTime), self.rows_per_partition)
        chunk_rdd = self._sc.parallelize([(group[0], group[-1] + 1) for group in chunk_iter])
        return chunk_rdd.map(lambda (first, last): _lb(first, last))

    def loadImages(self, datasetId=DATA_KEY, minTime=0, maxTime=-1):
        pass

    def loadSeries(self, datasetId=DATA_KEY, minTime=0, maxTime=-1):
        pass

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

    # TODO: Load regressors