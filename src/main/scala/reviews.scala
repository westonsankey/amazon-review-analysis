import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SQLContext
import java.time.Instant
import java.sql.Timestamp;


object reviews {

  def cleanTable(rawTable: DataFrame, spark: SparkSession) : DataFrame = {

    import spark.implicits._

    //Drop unnecessary columns
    val dropped = rawTable.drop()
    val dfDropped = rawTable.drop('reviewText).drop('summary).drop('reviewTime).drop('reviewerName)

    //Convert unixReviewTime to a DateTime - converts to local time zone, not sure how to keep GMT
    val unixTimestampToDateTimeFunction = (timestamp: Long) =>  {
      Timestamp.from(Instant.ofEpochSecond(timestamp))
    }

    val timestampConvesionUDF = udf(unixTimestampToDateTimeFunction)

    val dfFixedDate = dfDropped.withColumn("reviewDateTime", timestampConvesionUDF('unixReviewTime)).drop('unixReviewTime)
    dfFixedDate
  }

  def reviewsByUserDist(df: DataFrame, spark: SparkSession): DataFrame = {

    import spark.implicits._
    val numReviews = df.select('reviewerID)
      .groupBy('reviewerID)
      .count.withColumnRenamed("count", "reviewCount")
      .groupBy('reviewCount)
      .count
      .sort("reviewCount")

    numReviews
  }

  def ratingDist(df: DataFrame, spark: SparkSession): DataFrame = {

    import spark.implicits._
    val ratingDist = df.select('overall)
      .groupBy('overall)
      .count
      .sort('overall)

    ratingDist
  }

  def writeResultsCsv(df: DataFrame, fileName: String) : Unit = {
    df.repartition(1)
      .write.format("com.databricks.spark.csv")
      .save(s"${fileName}.csv")
  }

  def main(args: Array[String]): Unit = {

    //Configure logging and get source file path
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sourceFile = sys.env("SOURCE_FILE")

    //Create Spark Session and load raw JSON into DataFrame
    val spark = SparkSession.builder().appName("Amazon Reviews").master("local[*]").getOrCreate()
    val rawData = spark.read.json(s"${sourceFile}")

    //Clean table
    val df = cleanTable(rawData, spark)

    //Get
    val ratingDF = ratingDist(df, spark)
    val reviewsDF = reviewsByUserDist(df, spark)

    writeResultsCsv(ratingDF, "amazon-ratingdistribution")
    writeResultsCsv(ratingDF, "amazon-reviewsbyuser")
  }
}