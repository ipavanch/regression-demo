package util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UdfUtils {

  /*
     Udf to extract data from Row, sort by needed column (date) and return value
   */
  def sortUdf: UserDefinedFunction = udf((rows: Seq[Row]) => {
    rows.map { case Row(year: Int, quantity: Double) => (year, quantity) }
      .sortBy { case (year, quantity) => year }
      .map { case (year, quantity) => quantity }
  })

  /*
     Udf to compute growth rate with OLS
  */
  def growthRateUdf: UserDefinedFunction  = udf { input: Seq[Double] => {

    // convert to doubles and take logs
    val logVals: Seq[Double] = input.map(Math.log(_))

    // years are just increasing var
    val countVals : Seq[Double] = (1 to logVals.length).map(_.toDouble)

    // create list of (x, y) pairs for OLS
    val pairs = countVals zip logVals

    var mean_x: Double = 0
    var mean_y: Double = 0
    var mean_x_y: Double = 0
    var x: Double = 0
    val len = countVals.size
    var m: Double = 0
    var b: Double = 0

    pairs.foreach { i =>
      mean_x += i._1
      mean_y += i._2
      mean_x_y += i._1 * i._2
      x += i._1 * i._1
    }

    mean_x=mean_x/len
    mean_y=mean_y/len
    mean_x_y=mean_x_y/len
    m=(mean_x_y-(mean_x*mean_y))/((x/len)-(mean_x*mean_x))
    b=mean_y-(m*mean_x)
    m

  }
  }

}
