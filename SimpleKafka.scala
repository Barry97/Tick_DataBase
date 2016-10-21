package main

//SPARK
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.kafka.common._
import scala.collection.JavaConversions._
import java.util.UUID
import java.io._
import joptsimple._
import kafka.message._
import kafka.serializer._
import kafka.producer.KeyedMessage
import kafka.consumer.Consumer
import org.apache.hadoop.security.UserGroupInformation
import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerRecord
import collection.JavaConverters._
import java.util.HashMap
import java.util.Calendar
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object SimpleKafka extends Serializable {

  def main(args: Array[String]) {

    //spark Conf

    val conf = new SparkConf().setAppName("fxticks_Kafka")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.client.Result]))
    //.setMaster("local[*]")
    //.set("spark.executor.memory", "6g")
    //.set("spark.driver.memory", "6g")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //Kerberos pour Kafka
    System.setProperty("java.security.krb5.realm", "CIB.NET")
    System.setProperty("java.security.krb5.kdc", "swpifrdccib02.fr.intranet:swpifrdccib26.fr.intranet")
    System.setProperty("java.security.auth.login.config", "kafka_jasss.conf")
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "true")
    // a voir cette histoire de keytab
    UserGroupInformation.loginUserFromKeytabAndReturnUGI("adeagproc@CIB.NET", "/users/adeagproc/adeagproc.keytab")

    val brokers = "100.78.161.163:6667"
    val zookeeperConnect = "sldifrdwbhn01.fr.intranet:2181"
    val groupID = "sparkgroup1"
    val batchSize = 200
    val messageSendMaxRetries = 3
    val requestRequiredAcks = 0
    val clientId = UUID.randomUUID().toString
    val topic = List("spark_kafka1")    
    val producer = new KafkaProducer(topic(0), brokers, clientId, true, true, 10000, 3, -1)
    //produceFromHDFSFile
    val path = "/tmp/flume/Launcher.log"
    producer.ProduceFromHDFSFile( sc, path, "partition2")
    //produceFromHbase
    val tableName = "eag_fxticks_spark_New"
    producer.ProduceFromHbase(sc, tableName, "partition2")
    //produceFromHive
    val Database = "eagfxtikcs"
    val table = "kafka_spark"
    producer.ProduceFromHive(sc, Database, table, "partition2")  
    // creation d'un consumer 
    //val consumer = new KafkaConsumer(topic, groupID, zookeeperConnect, brokers, false)

    /* //envoie vers Hbase
    val tableName = "eag_fixticks"
    val Column_Family = "TEMP"
   // consumer.ConsumetoHbase(tableName, Column_Family)
    
    //envoie vers Hive 
    val Database = " eag_fx_ticks"
    val table = "kafka_spark_New"
   // consumer.ConsumetoHive(sc,Database,table)*/

    //envoie vers HDFS   
    //consumer.ConsumetoHDFS(sc, "hdfs://sldifrdwbhn01.fr.intranet:8020/tmp/BARRY/kafka")   
  }
}
