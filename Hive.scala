package main

import org.apache.spark.sql.hive

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext


//schema de données
case class fx_log(logC0: String, logC1: String, logC2: String, logC3: String, logC4: String, logC5: String, logC6: String)

object Hive {

  
// fonction qui permet d'inserer des  données  dans la  tables Hive 
  
  def Insert(Database: String, Table: String, stockdata: RDD[(String, String, String, String, String, String, String)], sc: SparkContext) {

    val hivecontext = new HiveContext(sc)
    import hivecontext.implicits._
    import hivecontext.sql
    val stockdataDF = stockdata.map { row => fx_log(row._1, row._2, row._3, row._4, row._5, row._6, row._7) }.toDF()
    
    stockdataDF.registerTempTable(Table)
    val results = hivecontext.sql("SELECT * FROM " + Table)

    hivecontext.sql("CREATE DATABASE IF NOT EXISTS " + Database)
    hivecontext.sql("CREATE TABLE IF NOT EXISTS " + Database + "." + Table + "(LogDate STRING, LogLevel STRING, LogMessage STRING, LogProcessId STRING, LogMachineName STRING, LogUser STRING, LogAppUser STRING, LogDomaine STRING, LogOperation STRING, LogType STRING, LogGuid STRING) stored as orc ")

    results.map(t => "Stock Entry: " + t.toString).collect().foreach(println)
     print(" ####################  Starting  Insert To Hive Table ##############################################")
    results.saveAsTable(Database.trim() + "." + Table.trim(), SaveMode.Append)

  }

  // fonction qui permet de lire depuis des tables hive 
  def Read(Database: String, Table: String, sc: SparkContext): Array[Row] =
    {
      val hivecontext = new HiveContext(sc)
      import hivecontext.implicits._
      val results = hivecontext.sql("SELECT * FROM " + Database.trim() + "." + Table.trim()).collect()
      return results
    }

}
