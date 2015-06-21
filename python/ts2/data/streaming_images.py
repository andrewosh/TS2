

class StreamingImages(StreamingData):
    """
    A StreamingData object where the value of each key/value pair in the DStream has been reshaped according to
    the dimensions (dims) of the images in the stream.
    """

    def __init__(self, dstream, dims=None, dtype='uint16'):
        super(StreamingImages, self).__init__(dstream, dtype=dtype)
        self.dstream = self.dstream.map(lambda (k, v): v.reshape(dims))