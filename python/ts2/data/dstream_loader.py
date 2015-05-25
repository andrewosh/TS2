from threading import Thread
import time

class DStreamLoader(Thread):
    """
    The DStreamLoader is responsible for periodically creating an RDD from the data inserted into the HBase
    database during a batch interval. It then inserts this RDD into a Queue which is converted into a DStream
    using ssc.queueStream(...)
    """

    def __init__(self, tssc, datasetId, queue, poll_time=5):
        Thread.__init__(self)
        self._poll_time = poll_time
        self._datasetId = datasetId
        self._queue = queue
        self._tssc = tssc

        self.last_idx = 0

        self._stopped = False

    def stop(self):
        self._stopped = True

    def run(self):
        while not self.stopped:
           bytes_rdd = self._tssc.loadBytes(self._datasetId, self.last_idx)
           self._queue.put(bytes_rdd)
           time.sleep(self._poll_time)
