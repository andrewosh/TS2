import numpy as np

class StreamingData(object):
    """
    The base wrapper class for a (K, ndarray) DStream. Supports basic operations, such as applyKeys and
    applyValues.
    """

    @property
    def _constructor(self):
        return StreamingData

    def __init__(self, dstream, dtype='uint16'):
        self.dstream = dstream.map(lambda (k, v): (k, np.fromstring(v, dtype=dtype)))

    def keys(self):
        return self.dstream.map(lambda (k, v): k)

    def values(self):
        return self.dstream.map(lambda (k, v): v)

    def applyKeys(self, func):
        return self._constructor(self.dstream.mapKeys(func), dtype=dtype)

    def applyValues(self, func):
        return self._constructor(self.dstream.mapValues(func), dtype=dtype)