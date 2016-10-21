package main

import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.KeyValue

object HbaseRead {

  def Read(sc: SparkContext, Hconf: Configuration, tableNAme: String): RDD[(ImmutableBytesWritable, Result)] = {

    // LIRE DEPUIS HBASE
    Hconf.set(TableInputFormat.INPUT_TABLE, tableNAme)
    val hBaseRDD = sc.newAPIHadoopRDD(Hconf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hBaseRDD.collect().foreach(println)

    return hBaseRDD
  }

  def GetResult(sc: SparkContext, Hconf: Configuration, tableNAme: String): RDD[Result] = {

    val hBaseRDD = Read(sc, Hconf, tableNAme)
    val resultRDD = hBaseRDD.map(tuple => tuple._2)
    return resultRDD

  }
  def GetColumn(sc: SparkContext, Hconf: Configuration, tableNAme: String, Column_Family: String, Column: String): RDD[KeyValue] = {
    val resultRDD = GetResult(sc, Hconf, tableNAme)
    val Rddcolumn = resultRDD.map { x => x.getColumnLatest(Bytes.toBytes(Column_Family), Bytes.toBytes(Column)) }
    return Rddcolumn
  }
}
