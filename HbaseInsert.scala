package main

import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ HBaseAdmin, HTable, Put }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

object HbaseInsert {

  def Insert(data: Array[(String, String, String, String, String, String, String)] , tableName: String, Column_Family: String) {

    val hConf = HbaseConnector.hbaseConf()
    hConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    println(HbaseConnector.VerifTable(hConf, tableName, Column_Family))

    val insert = data.map { m =>
      
      println(m)

      if (m == ("", "", "", "", "", "", "")) {
        print(" #################### Sorry ##############################################")
        print(" ############################################################################")
      } else {
        print(" ####################  Starting map Insert ##############################################")

        val hTable = new HTable(hConf, tableName)
        val LogC0 = m._1
        val LogC1 = m._2
        val LogC2 = m._3
        val LogC3 = m._4
        val LogC4 = m._5
        val LogC5 = m._6
        val LogC6 = m._7
        
        val thePut = new Put(new String("row" + "-" + LogC0 + "-" + LogC1 + "-" + LogC2 + "-" + LogC3
          + "-" + LogC4 + "-" + LogC5 + "-" + LogC6).getBytes())
        thePut.add(Bytes.toBytes(Column_Family), Bytes.toBytes("LogC0"), Bytes.toBytes(LogC0))
        thePut.add(Bytes.toBytes(Column_Family), Bytes.toBytes("LogC1"), Bytes.toBytes(LogC1))
        thePut.add(Bytes.toBytes(Column_Family), Bytes.toBytes("LogC2"), Bytes.toBytes(LogC2))
        thePut.add(Bytes.toBytes(Column_Family), Bytes.toBytes("LogC3"), Bytes.toBytes(LogC3))
        thePut.add(Bytes.toBytes(Column_Family), Bytes.toBytes("LogC4"), Bytes.toBytes(LogC4))
        thePut.add(Bytes.toBytes(Column_Family), Bytes.toBytes("LogC5"), Bytes.toBytes(LogC5))
        thePut.add(Bytes.toBytes(Column_Family), Bytes.toBytes("LogC6"), Bytes.toBytes(LogC6))

       

        print(" #################### sending put ##############################################")
        hTable.put(thePut)
      }
    }

  }

}
