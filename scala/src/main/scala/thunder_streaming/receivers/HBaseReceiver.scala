package thunder_streaming.receivers

import java.util

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Scan, HTable}
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import scala.util.control.Breaks._

import scala.collection.JavaConversions._

import org.apache.spark.streaming.dstream.PluggableInputDStream

class HBaseReceiver(reqCols: util.ArrayList[String],
                    family: String,
                    dataSet: String,
                    maxKey: Long,
                    period: Int)
  extends Receiver[(String, Array[Byte])](storageLevel=StorageLevel.MEMORY_AND_DISK) {

  val DATA_TABLE = "data"

  val conf = HBaseConfiguration.create()
  val admin = new HBaseAdmin(conf)
  val table = new HTable(conf, DATA_TABLE)

  var filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
  val filters = reqCols.toList.map{ col =>
    val newFilter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(col),
                             CompareOp.GREATER, Bytes.toBytes("0"))
    newFilter.setFilterIfMissing(true)
    newFilter
  }
  filters.foreach(f => filterList.addFilter(f))

  var batchScan = new Scan().setFilter(filterList)

  // minRow is updated after each batch to reflect the last complete row successfully stored
  var minRow = 0

  /**
   * Launch the HBase reader thread
   */
  override def onStart(): Unit = {
    // Start the thread that receives completed rows from the HBase database
    new Thread("HBase Receiver") {
      override def run() { receive() }
    }.start()
  }

  override def onStop(): Unit = {}

  /**
   * Apply batchScan to 'data' table in HBase and
   */
  def receive(): Unit = {
    while (!isStopped()) {
      batchScan.setStartRow(Bytes.toBytes(getPaddedKey(minRow.toString)))
      val resultScanner = table.getScanner(batchScan)
      val res = resultScanner.next()
      while(res != null) {
        val row = Bytes.toString(res.getRow())
        res.getRow
        val rowVal = row.toInt
        if (rowVal - minRow > 1) {
          // If the difference between the last valid row and this row is >1, then stop the current
          // iteration
          break
        }
        if (rowVal > minRow) {
          minRow = rowVal
        }
        val cols = res.getValue(Bytes.toBytes(family), Bytes.toBytes(dataSet))
        store((row, cols))
      }
      Thread.sleep(period * 1000)
    }
  }

  /**
   * Pad a Long key with the appropriate number of 0s, considering the maximum key value (maxKey)
   */
  def getPaddedKey(keyStr: String): String = {
    ('0' * (math.log10(maxKey) + 1).toInt - keyStr.length) + keyStr
  }
}

object HBaseReceiver {
  def apply(sc: StreamingContext,
            reqCols: util.ArrayList[String],
            family: String,
            dataSet: String,
            maxKey: Long,
            period: Int): PluggableInputDStream = {
    PluggableInputDStream(sc, new HBaseReceiver(reqCols, family, dataSet, maxKey, period))
  }
}
