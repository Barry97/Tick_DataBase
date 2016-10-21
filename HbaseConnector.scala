package main

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{ HColumnDescriptor, HTableDescriptor }
import com.typesafe.config.{ Config, ConfigFactory }
import java.io.File
import java.io.FileInputStream
import java.util.Properties

object HbaseConnector {

  //fonction pour la creation de la connexion Hbase avec la partie Kerberos 
  def hbaseConf(): Configuration = {

    val input = new FileInputStream("/adeagproc/adeagproc/ticks.conf")
    val gconf = new Properties()      
    gconf.load(input)
    gconf.getProperty("hbase_zookeeper_quorum")
    val Hconf = HBaseConfiguration.create()
    Hconf.addResource(new Path("/tmp/hbase/hbase-site.xml"))

    // Initialize hBase table if necessary
    Hconf.set("hbase.zookeeper.quorum", gconf.getProperty("hbase_zookeeper_quorum"))
    Hconf.set("hbase.zookeeper.property.clientPort", gconf.getProperty("hbase_zookeeper_property_clientPort"))
    Hconf.set("hbase.master", gconf.getProperty("hbase_master"))
    Hconf.set("hbase.cluster.distributed", gconf.getProperty("hbase_cluster_distributed"))
    Hconf.set("hbase.rootdir", gconf.getProperty("hbase_rootdir"))
    Hconf.set("hbase.zookeeper.property.maxClientCnxns", gconf.getProperty("hbase_zookeeper_property_maxClientCnxns"))
    Hconf.set("zookeeper.znode.parent", "/hbase-secure")
    Hconf.set("hbase.master.kerberos.principal", gconf.getProperty("hbase_master_kerberos_principal"))
    Hconf.set("hbase.regionserver.kerberos.principal",gconf.getProperty("hbase_regionserver_kerberos_principal"))
    Hconf.set("hbase.rpc.protection", gconf.getProperty("hbase_rpc_protection"))
    Hconf.set("hbase.security.authentication", gconf.getProperty("hbase_security_authentication"))
    Hconf.set("hbase.security.authorization", gconf.getProperty("hbase_security_authorization"))
    Hconf.set("hbase.client.keyvalue.maxsize", gconf.getProperty("hbase_client_keyvalue_maxsize"))
    Hconf.set("hbase.zookeeper.useMulti", gconf.getProperty("hbase_zookeeper_useMulti"))
    Hconf.set("phoenix.queryserver.kerberos.principal", gconf.getProperty("phoenix_queryserver_kerberos_principal"))
    Hconf.set("zookeeper.session.timeout", gconf.getProperty("zookeeper_session_timeout"))
    Hconf.set("hbase.security.exec.permission.checks", gconf.getProperty("hbase_security_exec_permission_checks"))
    Hconf.set("hbase.security.visibility.mutations.checkauth", gconf.getProperty("hbase_security_visibility_mutations_checkauth"))
    //Kerberos
    UserGroupInformation.setConfiguration(Hconf)
    return Hconf
  }
  // verification de l'existance de la table 
  
  def VerifTable(Hconf: Configuration, tableName: String, Column_Family: String): String = {

    val admin = new HBaseAdmin(Hconf)
    if (!admin.isTableAvailable(tableName)) {
      print("Creating  Table")
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor(Column_Family.getBytes()))
      admin.createTable(tableDesc)

      return "Table doesn't exist!! but created !!"

    } else {

      return "Table already exists!!"

    }

  }

}
