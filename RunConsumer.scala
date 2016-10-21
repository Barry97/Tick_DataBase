package main

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
import com.typesafe.config.{ Config, ConfigFactory }
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*
 * cet Object prend en parametres 
 * args 0 = type de consumer : == 0  ConsumetoHbase
 * 																	 {	args 3 = tableName 
 * 																			args 4 = Column_Family
 * 																	  }
 * 													  : == 1  ConsumetoHive
 * 																	 {	args 3 = Database 
 * 																			args 4 = table
 * 																	  }
 * 													 : == 2   ConsumetoHDFS
 * 																	 {	args 3 = path 
 * 																			
 * 																	  }
 * 													 : == 3   ConsumePrint
 * 																	 
 * 
 * 
 * args 1 = topic
 * args 2 = GroupId
 * 
 */

object RunConsumer {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //spark Conf

    val conf = new SparkConf().setAppName("fx_Kafka_Consumer")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.client.Result]))
    //.setMaster("local[*]")
    //.set("spark.executor.memory", "6g")
    //.set("spark.driver.memory", "6g")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val input = new FileInputStream("/users/mghorbel/indixis.conf")
    val gconf = new Properties()

    gconf.load(input)

    //Kerberos pour Kafka
    System.setProperty("java.security.krb5.realm", gconf.getProperty("java_security_krb5_realm"))
    System.setProperty("java.security.krb5.kdc", gconf.getProperty("java_security_krb5_kdc"))
    System.setProperty("java.security.auth.login.config", gconf.getProperty("java_security_auth_login_config"))
    System.setProperty("javax.security.auth.useSubjectCredsOnly", gconf.getProperty("javax_security_auth_useSubjectCredsOnly"))
    UserGroupInformation.loginUserFromKeytabAndReturnUGI(gconf.getProperty("UserFromKey"), gconf.getProperty("UserFromKeyLink"))

    val brokers = gconf.getProperty("brokers")
    val zookeeperConnect = gconf.getProperty("zookeeperConnect")
    val groupID = args(2)
    val batchSize = 200
    val messageSendMaxRetries = 3
    val requestRequiredAcks = 0
    val clientId = UUID.randomUUID().toString
    val topic = List(args(1))

    /*le type de Consumer ==> 0 : ConsumetoHbase
     * 												1 : ConsumetoHive
     * 												2 : ConsumetoHDFS 
     * 												3 :	ConsumePrint
     */
    val ConsumerType = args(0).toInt

    // creation d'un consumer 
    val consumer = new KafkaConsumer(topic, groupID, zookeeperConnect, brokers, false)

   
    
    if (ConsumerType == 0) {
      //envoie vers Hbase
      val tableName = args(3) //"indixis_log_kafka_spark_New"
      val Column_Family = args(4) // "indixis_log_familly"
      consumer.ConsumetoHbase(tableName, Column_Family)
    }

    if (ConsumerType == 1) {
      //envoie vers Hive 
      val Database = args(3) //" log_indixis"
      val table = args(4) //"kafka_spark_New"
      consumer.ConsumetoHive(sc, Database, table)
    }

    if (ConsumerType == 2) {
      //envoie vers HDFS 
      val path = args(3) + topic(0) ///tmp/kafka_data/kafka
      consumer.ConsumetoHDFS(sc, "hdfs://sldifrdwbhn01.fr.intranet:8020" + path)
    }

    if (ConsumerType == 3) {
      //afficher les donnees 
      consumer.ConsumePrint()

    }
  }
}
