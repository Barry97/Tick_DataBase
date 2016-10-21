package main
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
//import org.apache.kafka.common._
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
//import org.apache.kafka.clients.consumer.ConsumerRecord
import collection.JavaConverters._
import java.util.HashMap
import java.util.Calendar
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.typesafe.config._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.expressions.aggregate.Final

/*
 * cet Object prend en parametres 
 * args 0 = type de producer : == 0  ProduceFromHDFSFile
 * 																	 {	args 2 = path 
 * 																			args 3 = partition
 * 																	  }
 * 													 : == 1  ProduceFromHbase
 * 																	 {	args 2 = tableName 
 * 																			args 3 = partition
 * 																	  }
 * 													 : == 2  ProduceFromHive 
 * 																	 {	args 2 = Database 
 * 																			args 3 = table
 * 																			args 4 = partition
 * 																	  }
 * 
 * 
 * args 1 = topic
 * 
 * 
 */

object RunProducer {
  
	def main(args: Array[String]) {
	  
		// pour desactiver les log par defaut de spark (org, akka)
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
		//spark Conf
		val conf = new SparkConf().setAppName("TicksDatabase")
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		conf.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.client.Result]))
		.setMaster("local[*]")
		.set("spark.executor.memory", "6g")
		.set("spark.driver.memory", "6g")
		val sc = new SparkContext(conf)
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._
		// le path du fichier de config 
		
		val input = new FileInputStream("/home/adeagproc/tick.conf")
		val gconf = new Properties()
		// chargement du fichier de config 
		gconf.load(input)
		//Kerberos pour Kafka
		System.setProperty("java.security.krb5.realm", gconf.getProperty("java_security_krb5_realm"))
		System.setProperty("java.security.krb5.kdc", gconf.getProperty("java_security_krb5_kdc"))
		System.setProperty("java.security.auth.login.config", gconf.getProperty("java_security_auth_login_config"))
		System.setProperty("javax.security.auth.useSubjectCredsOnly", gconf.getProperty("javax_security_auth_useSubjectCredsOnly"))
		// UserGroupInformation.loginUserFromKeytabAndReturnUGI(gconf.getProperty("UserFromKey"), gconf.getProperty("UserFromKeyLink"))
		val brokers = gconf.getProperty("brokers")
		val zookeeperConnect = gconf.getProperty("zookeeperConnect")
		//val groupID = "sparkgroup1"
		val batchSize = 200
		val messageSendMaxRetries = 3
		val requestRequiredAcks = 0
		val clientId = UUID.randomUUID().toString
		val topic = List("topic-eag-fxticks")
		//le type de producer ==> 0 : ProduceFromHDFSFile	
		//val producerType = args(0).toInt
		// appel de la classe KafkaProducer avec ses parametres
		val producer = new KafkaProducer(topic(0), brokers, clientId, true, true, 10000, 3, -1)
		// appel de la methode pour la lecture du fichier depuis hdfs
		producer.ProduceFromHDFSFile(sc,"hdfs://sldifrdwbhd01.fr.intranet:8020/tmp/BARRY/simple1.txt", "1")
		/* if (producerType == 0) {
      //produceFromHDFSFile
      val path = args(2)
      producer.ProduceFromHDFSFile(sc, path, args(3))
    }
    /* if (producerType == 1) {
      //produceFromHbase
      val tableName = args(2)
      producer.ProduceFromHbase(sc, tableName, args(3))
    }
    if (producerType == 2) {
      //produceFromHive
      val Database = args(2)
      val table = args(3)
      producer.ProduceFromHive(sc, Database, table, args(4))
    }*/*/
	}
}


