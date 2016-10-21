

package main
import org.apache.hadoop.hive
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import java.io.File
import org.apache.spark.sql.hive.HiveContext


    object HiveSelect {
  
   def testSelect {
     val conf = new SparkConf().setAppName("Simple Application")
				.setMaster("local[*]")
				val sc = new SparkContext(conf)
     	val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)	
     import sqlContext.implicits._ 
     import sqlContext.sql
     
      val resultat = sqlContext.sql("Select * from mydata")
       resultat.collect().foreach(println)
    }
}
