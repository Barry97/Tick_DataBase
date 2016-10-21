package main

import org.apache.kafka.common._
import scala.collection.JavaConversions._
import java.util.UUID
import java.io._
import joptsimple._
import kafka.message._
import kafka.serializer._
import java.util.Properties
import collection.JavaConverters._
import kafka.consumer._
import kafka.consumer.ConsumerConfig
import kafka.consumer.Consumer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{ HColumnDescriptor, HTableDescriptor }
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import com.typesafe.config._
import org.apache.spark.streaming.Seconds
import org.apache.hadoop.hbase.client.{ HBaseAdmin, HTable, Put }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.SQLContext
import java.util.Calendar
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.net.URI


case class KafkaConsumer(
    topic: List[String],
    /**
 * topic
 * The high-level API hides the details of brokers from the consumer and allows consuming off the cluster of machines
 * without concern for the underlying topology. It also maintains the state of what has been consumed. The high-level API
 * also provides the ability to subscribe to topics that match a filter expression (i.e., either a whitelist or a blacklist
 * regular expression).  This topic is a whitelist only but can change with re-factoring below on the filterSpec
 */
    groupId: String,
    /**
 * groupId
 * A string that uniquely identifies the group of consumer processes to which this consumer belongs. By setting the same
 * group id multiple processes indicate that they are all part of the same consumer group.
 */
    zookeeperConnect: String,
    /**
 * Specifies the zookeeper connection string in the form hostname:port where host and port are the host and port of
 * a zookeeper server. To allow connecting through other zookeeper nodes when that zookeeper machine is down you can also
 * specify multiple hosts in the form hostname1:port1,hostname2:port2,hostname3:port3. The server may also have a zookeeper
 * chroot path as part of it's zookeeper connection string which puts its data under some path in the global zookeeper namespace.
 * If so the consumer should use the same chroot path in its connection string. For example to give a chroot path of /chroot/path
 * you would give the connection string as hostname1:port1,hostname2:port2,hostname3:port3/chroot/path.
 */
    brokerList: String,
    /**
 * brokerList
 * This is for bootstrapping and the producer will only use it for getting metadata (topics, partitions and replicas).
 * The socket connections for sending the actual data will be established based on the broker information returned in
 * the metadata. The format is host1:port1,host2:port2, and the list can be a subset of brokers or a VIP pointing to a
 * subset of brokers.
 */
    readFromStartOfStream: Boolean = true /** 
  * What to do when there is no initial offset in Zookeeper or if an offset is out of range: 
  * 1) smallest : automatically reset the offset to the smallest offset 
  * 2) largest : automatically reset the offset to the largest offset 
  * 3) anything else: throw exception to the consumer. If this is set to largest, the consumer may lose some 
       messages when the number of partitions, for the topics it subscribes to, changes on the broker. 
  ****************************************************************************************
  To prevent data loss during partition addition, set auto.offset.reset to smallest
  This make sense to change to true if you know you are listening for new data only as of
  after you connect to the stream new things are coming out.  you can audit/reconcile in
  another consumer which this flag allows you to toggle if it is catch-up and new stuff or
  just new stuff coming out of the stream.  This will also block waiting for new stuff so
  it makes a good listener.
  //readFromStartOfStream: Boolean = true
  readFromStartOfStream: Boolean = false
  ****************************************************************************************
  */ )
  {

  val props = new Properties()

  props.put("zookeeper.connect", zookeeperConnect)
  props.put("bootstrap.servers", brokerList)
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("security.protocol", "SASL_PLAINTEXT")
  props.put("group.id", groupId)

  val config = new ConsumerConfig(props)
  val consumer = new org.apache.kafka.clients.consumer.KafkaConsumer[String, String](props)

  // affichage de l'ensemble des messages d'un topic 
  def ConsumePrint() {

    consumer.subscribe(topic)

    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {
        printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value())
      }
    }
    consumer.close()
  }

  //insertion des messages kafka dans Hbase 
  def ConsumetoHbase(tableName: String, Column_Family: String) {

    consumer.subscribe(topic)

    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {
        val decom = record.value().split("\n")
          .map { x => x.replace("[|]", "$") }
          .map { x => x.split('$') }
          .map { x =>
            print(" ####################*****************##############################################")
            x.foreach { println }

            if (x.length == 7) {

              val LogC0 = x(0)
              val LogC1 = x(1)
              val LogC2 = x(2)
              val LogC3 = x(3)
              val LogC4 = x(4)
              val LogC5 = x(5)
              val LogC6 = x(6)
             (LogC0, LogC1, LogC2, LogC3, LogC4, LogC5, LogC6)

            } else
              ("", "", "", "", "", "", "")

          }

         HbaseInsert.Insert(decom, tableName, Column_Family)
         
      }

    }
    consumer.close()
  }

  // insertion des messages kafka dans une table Hive 
  def ConsumetoHive(sc: SparkContext, Database: String, Table: String) {

    consumer.subscribe(topic)

    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {
        println(record)
        val a = record.value().split("\n")
          .map { x => x.replace("[|]", "$") }
          .map { x => x.split('$') }
          .map { x =>
            print(" ####################*****************##############################################")
            x.foreach { println }

            if (x.length == 7) {

              val LogC0 = x(0)
              val LogC1 = x(1)
              val LogC2 = x(2)
              val LogC3 = x(3)
              val LogC4 = x(4)
              val LogC5 = x(5)
              val LogC6 = x(6)
              

              (LogC0, LogC1, LogC2, LogC3, LogC4, LogC5, LogC6)

            } else
              (null, null, null, null, null, null, null)

          }

          val data = sc.parallelize(a.toList)

        Hive.Insert(Database, Table, data, sc)
        

      }
    }
    consumer.close()
  }
// insertion des messages kafka dans HDFS
  def ConsumetoHDFS(sc: SparkContext, path: String) {

    consumer.subscribe(topic)

    var messages_total = ""

    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {
        val message = record.value().toString()
        println(message)
        messages_total + message

      }
      //timestamp           
      val timestamp: Long = System.currentTimeMillis
      System.setProperty("HADOOP_USER_NAME", "adeagproc")
      val paths = new Path(path + "_" + timestamp + ".txt")
      val configuration = new Configuration()
      configuration.set("fs.defaultFS", "hdfs://sldifrdwbhn01.fr.intranet:8020")
      val hdfs = FileSystem.get(configuration)
      val br = new BufferedWriter(new OutputStreamWriter(hdfs.create(paths, true)))
      br.write(messages_total)
      hdfs.close()

    }

    consumer.close()
  }

}
