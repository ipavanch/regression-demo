
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import _root_.util.DataSchema.inputSchema
import _root_.util.UdfUtils._

object SparkBatchExample {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("spark session example")
      .getOrCreate()

    val finalDf = compute(sparkSession)
    finalDf.show()

  }

  def compute(sparkSession: SparkSession) : DataFrame = {

    import sparkSession.implicits._

    val baseDf = sparkSession
                  .read
                  .option("header","true")
                  .option("mode", "DROPMALFORMED")
                  .schema(inputSchema)
                  .csv("dataset/food_small.csv")

    val finalDf= baseDf
           .select($"Area", $"Item", $"Element", $"year", $"quantity")                          // select what we care about
           .groupBy($"Area", $"Item", $"year")                                                  // first group-by is needed, since "Element" is not unique per ("Area", "Item", "year")
           .agg(sum($"quantity").as("quantity"))                                                // compute total quantity of elements of ("Area", "Item", "year") tuples
           .groupBy($"Area", $"Item")                                                           // second group by for final grouping
           .agg(collect_list(struct("year", "quantity")) as "year_quantity")       // create column with a sequence of tupels
           .withColumn("Growth Rate", growthRateUdf(sortUdf(col("year_quantity")))) // compute growth rate with OLS on a sorted list of quantities
           .drop("year_quantity")                                                            // drop aux column
           .orderBy($"Growth Rate".desc)                                                              // sort by "Growth Rate"

    finalDf
  }
}



