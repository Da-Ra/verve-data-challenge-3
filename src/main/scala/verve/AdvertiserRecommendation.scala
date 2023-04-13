package verve

import org.apache.spark.sql.types.{StringType, StructField, StructType, DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object AdvertiserRecommendation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AdvertiserRecommendation")
      .master("local[1]")
      .getOrCreate()

    spark.conf.set("spark.sql.legacy.json.allowEmptyString.enabled", true)

    val impressionsSchema = StructType(Seq(
      StructField("app_id", IntegerType, nullable = true),
      StructField("advertiser_id", IntegerType, nullable = true),
      StructField("country_code", StringType, nullable = true),
      StructField("id", StringType, nullable = false)
    ))
    

    val clicksSchema = StructType(Seq(
      StructField("impression_id", StringType, nullable = false),
      StructField("revenue", DoubleType, nullable = false)
    ))

    val impressionsDF = loadJsonFile(spark, "impressions.json", impressionsSchema)
    val clicksDF = loadJsonFile(spark, "clicks.json", clicksSchema)
    val joinedDF = joinImpressionsAndClicks(impressionsDF, clicksDF)
    val groupedDF = groupByAppAndCountry(joinedDF)
    val topAdvertisersDF = getTopAdvertisers(groupedDF)

    writeJsonFile(topAdvertisersDF, "top_advertisers")

    spark.stop()
  }

  def loadJsonFile(spark: SparkSession, fileName: String, schema: StructType): DataFrame = {
    spark.read.option("multiLine", true).option("mode", "DROPMALFORMED").schema(schema).json(s"./src/main/resources/$fileName")
  }

  def joinImpressionsAndClicks(impressionsDF: DataFrame, clicksDF: DataFrame): DataFrame = {
    impressionsDF.join(clicksDF, col("id") === col("impression_id"))
  }

  def groupByAppAndCountry(df: DataFrame): DataFrame = {
    df.groupBy("app_id", "country_code", "advertiser_id")
      .agg(avg("revenue").alias("avg_revenue_per_impression"))
  }

  def getTopAdvertisers(df: DataFrame): DataFrame = {
    df.select(col("*"),
      dense_rank().over(
        Window.partitionBy("app_id", "country_code")
          .orderBy(desc("avg_revenue_per_impression"))
      ).alias("rank")
    ).where("rank <= 5")
      .groupBy("app_id", "country_code")
      .agg(collect_list("advertiser_id").alias("recommended_advertiser_ids"))
      .select(col("app_id"), col("country_code"), col("recommended_advertiser_ids"))
  }

  def writeJsonFile(df: DataFrame, fileName: String): Unit = {   
    df.write.option("pretty", true).json(s"./src/main/scala/outputs/$fileName")
  }

}
