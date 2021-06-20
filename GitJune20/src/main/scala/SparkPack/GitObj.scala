package SparkPack
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object GitObj {
  def main(aregs:Array[String]):Unit={
			val conf= new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					val spark =SparkSession.builder().getOrCreate()
					sc.setLogLevel("Error")
					val url = "https://randomuser.me/api/0.8/?results=50"
					val result = scala.io.Source.fromURL(url).mkString
					val rdd1 = sc.parallelize(List(result))
					val df = spark.read.json(rdd1)
					df.printSchema()


					val middata = df.select(col("nationality"), col("results") ,col("seed"),col("version"))
					.withColumn("results", explode(col("results")))
					middata.printSchema()


					val finaldata = middata.select(
					    col("nationality")
						    ,col("results.user.cell")
						    ,col("results.user.dob")
						    ,col("results.user.email")
						    ,col("results.user.gender")
						    ,col("results.user.location.city")
						    ,col("results.user.location.state")
						    ,col("results.user.location.street")
						    ,col("results.user.location.zip")
						    ,col("results.user.md5")
						    ,col("results.user.name.first")
						    ,col("results.user.name.last")
						    ,col("results.user.name.title")
						    ,col("results.user.password")
						    ,col("results.user.phone")
						    ,col("results.user.picture.large")
						    ,col("results.user.picture.medium")
						    ,col("results.user.picture.thumbnail")
						    ,col("results.user.registered")
						    ,col("results.user.salt")
						    ,col("results.user.sha1")
						    ,col("results.user.sha256")
						    ,col("results.user.username")
						    ,col("seed")
						    ,col("version")
)


					finaldata.show()
					finaldata.printSchema()

					println(finaldata.count())
	}
}