

package main

import java.sql.Connection
import java.sql.DriverManager

import org.apache.spark.sql.hive

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext


object HiveConnector {

	def main(args: Array[String]){
		// set sparkConf 
		val sparkConf = new SparkConf().setAppName("process").setMaster("local")      
				val sc = new SparkContext(sparkConf)                                          
		val hiveContext = new HiveContext(sc)                                         

		/* Set HDFS                                                                   
    System.setProperty("HADOOP_USER_NAME", "hdfs")                                
    val hdfsconf = SparkHadoopUtil.get.newConfiguration(sc.getConf)               
    hdfsconf.set("fs.defaultFS", "hdfs://sandbox.hortonworks.com:8020")           
    val hdfs = FileSystem.get(hdfsconf) */


		val url = "jdbc:hive2://sldifrdwbhn01.fr.intranet:10001/eag_fxticks"
		val username ="adeagproc"
		val password ="C8hm3Sv*$a0p6iq"

		var connection:Connection = null
		try {
			// Class.forName(driver)
			connection = DriverManager.getConnection(url,username,password)

					val statement = connection.createStatement()
					val resultSet = statement.executeQuery("select c0 from mydata")
					while (resultSet.next()){
						val host = resultSet.getString("c0")
								println(host)
					}
		} catch {
		case e => e.printStackTrace // TODO: handle error
		}
		connection.close()

	}
}
