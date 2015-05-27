import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver


class HBaseReceiver[T](host: String,
                       port: Int,
                       colName: String,
                       batchTime: Int)
  extends Receiver[T](storageLevel=StorageLevel.MEMORY_AND_DISK) {

  var conn:

  // Create a connection to the HBase DB
  def initialize(): Unit = {

  }

  override def onStart(): Unit = {
    initialize()

  }

  override def onStop(): Unit = ???
}