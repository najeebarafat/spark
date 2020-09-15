package Test
import org.apache.spark.{SparkConf, SparkContext}
object Example extends  App {
println("abc")
val sc=new SparkContext(config = new SparkConf().setAppName("b").setMaster("local"))
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
  val rdd = sc.parallelize(data)
rdd.count()
  //val df=spark.read.text("file:///Zee-Emirates\\edh-order\\ingestion\\spark\\OrderGenome\\pom.xml")

  //df.count()
}
