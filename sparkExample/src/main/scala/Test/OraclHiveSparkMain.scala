package Test

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory

object OraclHiveSparkMain {
//$ spark-shell --jars /oraclejdbc.jar
  def main(args: Array[String]): Unit = {

    val inputTable=args(0)// table name
    val userName=args(1)
    val connectionString=args(2) // connection string url
    val jckesFile=args(3)
    val passwordAlias=args(4)
    val outputTable=args(5)//output table name with db name ex:- db.tablename_1

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

  val tableName=tableDetail._1
  val dataBaseName=tableDetail._2
  val query =tableDetail._3
  //Read data from database
  val oracleDf = spark.sqlContext.read.format("jdbc").
    option("url", connectionString).
    option("dbtable", tableName).
    option("user",userName).
    option("password",password).
    option("driver", "oracle.jdbc.driver.OracleDriver").load

  //Created view
oracleDf.createOrReplaceGlobalTempView(dataBaseName+"."+tableName)

  //Based on table logic pass query and get result
  val resDf=oracleDf.sqlContext.sql(query);

  // write data to hive
  resDf.write.mode("overwrite").saveAsTable(dataBaseName+"."+tableName+"_"+outputTable)
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
