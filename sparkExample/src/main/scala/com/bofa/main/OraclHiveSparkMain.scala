package com.bofa.main

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.spark.sql.SparkSession

object OraclHiveSparkMain {
//$ spark-shell --jars /oraclejdbc.jar
  def main(args: Array[String]): Unit = {

    val userName=args(0)
    val connectionString=args(1) // connection string url
    val jckesFile=args(2)
    val passwordAlias=args(3)
    val outputTable=args(4)//output table name with db name ex:- db.tablename_1

    //Spark session object
    val spark=SparkSession.builder().appName("Import").enableHiveSupport().getOrCreate()
   //get password from jckesfile
    val password  = extractPwdFromJceks(jckesFile,passwordAlias)

    //Read table name  from database
    val tableDf = spark.sqlContext.read.format("jdbc").
      option("url", connectionString).
      option("dbtable", "(select a.* from owner_ops.data_object a, owner_ops.process_definition b , owner_ops.thread_target c where a.data_object_id=c.target_data_object_id and c.process_id=b.process_id) as temp").
      option("user",userName).
      option("password",password).
      option("driver", "oracle.jdbc.driver.OracleDriver").load

    //Table name + database name + query
    val tableArr:Array[(String,String,String)]=tableDf.rdd.map(row=> (row.getString(0),row.getString(1),row.getString(2))).collect()

for(tableDetail <- tableArr){

  val tableName=tableDetail._1 //tb1
  val dataBaseName=tableDetail._2 //db1
  val query =tableDetail._3  // eg: select * from db1.tb1 where logic
  //Read data from database
  val oracleDf = spark.sqlContext.read.format("jdbc").
    option("url", connectionString).
    option("dbtable", query).
    option("user",userName).
    option("password",password).
    option("driver", "oracle.jdbc.driver.OracleDriver").load


  // write data to hive
  oracleDf.write.mode("overwrite").saveAsTable(dataBaseName+"."+tableName+"_"+outputTable)
}





  }
  def extractPwdFromJceks(jceksfile:String, password_alias:String):String = {
    val conf:Configuration = new Configuration()
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jceksfile)

    val temp = conf.getPassword(password_alias).mkString("")
    var password = ""
    if (temp != null && !temp.isEmpty() && !temp.equalsIgnoreCase("")){
      password = temp
    }
    return password
  }
}
