package thunder_streaming.receivers

import java.util

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Scan, HTable}
import org.apache.hadoop.hbase.filter.{SingleColumnValueFilter, FilterList}
import scala.util.control.Breaks._

import scala.collection.JavaConversions._

class HBaseReceiver(reqCols: util.ArrayList[String],
                    family: String,
                    dataSet: String,
                    maxKey: Long,
                    period: Int)
  extends Receiver[(String, Array[Byte])](storageLevel=StorageLevel.MEMORY_AND_DISK) {

  val DATA_TABLE = "data"
  var receiverThread: Thread = _
  var stopped: Boolean = false

  /**
   * Launch the HBase reader thread
   */
  override def onStart(): Unit = {
    // Start the thread that receives completed rows from the HBase database
    receiverThread = new Thread("HBase Receiver") {
      override def run() { receive() }
    }
    receiverThread.start()
  }

  override def onStop(): Unit = {
    stopped = true
  }

  /**
   * Apply batchScan to 'data' table in HBase and
   */
  def receive(): Unit = {
    val conf = HBaseConfiguration.create()
    val admin = new HBaseAdmin(conf)
    val table = new HTable(conf, DATA_TABLE)

    var filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    val filters = reqCols.toList.map{ col =>
      new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(col), CompareOp.NO_OP, Bytes.toBytes("0"))
    }
    filters.foreach(f => filterList.addFilter(f))

    var batchScan = new Scan().setFilter(filterList)

    // minRow is updated after each batch to reflect the last complete row successfully stored
    var minRow = 0

    while (!stopped) {
      val startRow = getPaddedKey(minRow.toString)
      println("startRow: %s".format(startRow))
      batchScan.setStartRow(Bytes.toBytes(startRow))
      val resultScanner = table.getScanner(batchScan)
      val res = resultScanner.next()
      while(res != null) {
        val row = Bytes.toString(res.getRow())
        println("Got row: %s".format(row))
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
        println("Storing row of length: %d".format(cols.length))
        store((row, cols))
      }
      Thread.sleep(period * 1000)
    }
  }

  /**
   * Pad a Long key with the appropriate number of 0s, considering the maximum key value (maxKey)
   */
  def getPaddedKey(keyStr: String): String = {
    ("0" * ((math.log10(maxKey)).toInt - keyStr.length)) + keyStr
  }
}
