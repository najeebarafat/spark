package Test

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory

object OraclHiveSparkMain {
//$ spark-shell --jars /CData/CData JDBC Driver for Oracle/lib/cdata.jdbc.oracleoci.jar
  def main(args: Array[String]): Unit = {
    val inputTable=args(0)// table name
    val userName=args(1)
    val connectionString=args(2) // connection string url
    val jckesFile=args(3)
    val passwordAlias=args(4)
    val outputTable=args(5)//output table name with db name ex:- db.tablename


    val password  = extractPwdFromJceks(jckesFile,passwordAlias)

    //Spark session object
    val spark=SparkSession.builder().appName("Import").enableHiveSupport().getOrCreate()

    //Read data from database
    val oracleDf = spark.sqlContext.read.format("jdbc").
      option("url", connectionString).
      option("dbtable", inputTable).
      option("user",userName).
      option("password",password).
      option("driver", "cdata.jdbc.oracleoci.OracleOCIDriver").load

    // write data to hive
    oracleDf.write.mode("overwrite").saveAsTable(outputTable)

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
