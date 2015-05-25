from threading import Thread
from ts2.util.utils import grouper
import time

class DStreamLoader(Thread):
    """
    The DStreamLoader is responsible for periodically creating an RDD from the data inserted into the HBase
    database during a batch interval. It then inserts this RDD into a Queue which is converted into a DStream
    using ssc.queueStream(...)
    """

    def __init__(self, tssc, datasetId, queue, poll_time=5, rows_per_batch=10, rows_per_query=1):
        Thread.__init__(self)
        self._poll_time = poll_time
        self._rows_per_query = rows_per_query
        self._rows_per_batch = rows_per_batch
        self._datasetId = datasetId
        self._queue = queue
        self._tssc = tssc

        self.last_idx = 0

        self._stopped = False

    def stop(self):
        self._stopped = True

    def _get_last_complete_key(self, keys):
        last_key = self.last_idx
        if len(keys) == 0:
            return self.last_idx
        else:
            for key in keys:
                if (key - last_key) == 1:
                    last_key = key
                else:
                    break
            return last_key

    def run(self):
        while not self.stopped:
            minTime = self.last_idx + 1
            maxTime = minTime + self._rows_per_batch
            bytes_rdd = self._tssc.loadBytes(self._datasetId, minTime=minTime, maxTime=maxTime)
            keys = sorted(bytes_rdd.keys().collect())
            self.last_idx = self._get_last_complete_key(keys)
            self.last_idx = max(bytes_rdd.keys().collect())
            self._queue.put(bytes_rdd)
            time.sleep(self._poll_time)
