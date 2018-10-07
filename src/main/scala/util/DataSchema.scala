package util

import org.apache.spark.sql.types.StructType

object DataSchema {

  val inputSchema: StructType = new StructType()
    .add("Area Abbreviation", "string")
    .add("Area Code", "integer")
    .add("Area", "string")
    .add("Item Code", "integer")
    .add("Item", "string")
    .add("Element Code", "integer")
    .add("Element", "string")
    .add("Unit", "string")
    .add("latitude", "double")
    .add("longitude", "double")
    .add("year", "integer")
    .add("quantity", "double")

}
