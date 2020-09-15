package Test

import org.apache.spark.sql.SparkSession

object OraclHiveSparkMain {
//$ spark-shell --jars /CData/CData JDBC Driver for Oracle/lib/cdata.jdbc.oracleoci.jar
  def main(args: Array[String]): Unit = {
    val inputTable=args(0)// table name
    val connectionString=args(1) // connection string url
    val outputTable=args(2)//output table name with db name ex:- db.tablename

    //Spark session object
    val spark=SparkSession.builder().appName("Import").enableHiveSupport().getOrCreate()

    //Read data from database
    val oracleDf = spark.sqlContext.read.format("jdbc").
      option("url", connectionString).
      option("dbtable", inputTable).
      option("driver", "cdata.jdbc.oracleoci.OracleOCIDriver").load

    // write data to hive
    oracleDf.write.mode("overwrite").saveAsTable(outputTable)

  }

}
