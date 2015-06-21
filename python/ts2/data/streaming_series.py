
from ts2.data.streaming_data import StreamingData
import numpy as np

class StreamingSeries(StreamingData):
    """
    A StreamingData object where for every key/value entry in the DStream, each element (of size dtype) in ndarray
    value is the entry in a series corresponding to the given key.

    i.e. The first two entries of the StreamingData DStream might be [(0, [0, 10, 20]), (1, [10, 20, 30])], which would
    correspond to a StreamingSeries with entries [(0, [0, 10]), (1, [10, 20]), (2, [20, 30])] (the keys are assumed
    to be linear)
    """

    def __init__(self, dstream, dtype='uint16'):
        super(StreamingSeries, self).__init__(dstream, dtype=dtype)
        self.dstream = self.dstream.map(lambda (k, v): (k, list(enumerate(v))))

    def mean(self):
        # TODO this can be generalized
        def updateFunc(newValues, stateDict):
            if stateDict is None:
                stateDict = {}
            count = stateDict.get('count', 0)
            mean = stateDict.get('mean', 0.0)
            newCount = count + len(newValues)
            stateDict['mean'] = (count * mean + len(newValues) * np.mean(newValues)) / newCount
            stateDict['count'] = newCount
            return stateDict
        return self.values().updateStateByKey(updateFunc).map(lambda state: state['mean'])

