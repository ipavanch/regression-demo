
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import _root_.util.UdfUtils._
import _root_.util.DataSchema._

object SparkStreamExample {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(1))

    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("spark session example")
      .getOrCreate()

    val finalStream = stream(sparkSession)

    finalStream
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }

  def stream(sparkSession: SparkSession) : DataFrame = {

    import sparkSession.implicits._

    // Read all the csv files atomically in a directory
    val baseStream = sparkSession
      .readStream
      .option("header","true")
      .option("mode", "DROPMALFORMED")
      .schema(inputSchema)      // Specify schema of the csv files
      .csv("dataset")    // Equivalent to format("csv").load("/path/to/directory")

    val finalDf= baseStream
      .withColumn("eventTime", lit(current_timestamp()))                                // construct sample event time column (better alternative would be file creation timestamp
      .select($"Area", $"Item", $"Element", $"year", $"quantity", $"eventTime")                          // select columns we care about
      .groupBy(window($"eventTime", "5 minutes"), $"Area", $"Item")    // group by in a window
      .agg(collect_list(struct("year", "quantity")) as "year_quantity")       // create column with a sequence of tupels
      .withColumn("Growth Rate", growthRateUdf(sortUdf(col("year_quantity")))) // compute growth rate with OLS on a sorted list of quantities
      .drop("year_quantity")                                                            // drop aux column
      .orderBy($"Growth Rate".desc)

    finalDf
  }


}



