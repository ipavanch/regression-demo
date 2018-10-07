## Notes on Streaming Conversion
* I created two custom UDF functions to facilitate as much code reuse as possible while porting batch to stream mode.
* Fist pre-aggregation was omitted in stream mode (it does not impact results in this particular case), since multiple levels of aggregation are not currently supported in Spark Streaming. 
  It could be translated into another chained UDF transform if 100% consistency is required.  
* Output mode is set to 'Complete' due to sorting requirenment on final table   
* Streaming watermarks were not defined (they will be ignored anyway) since we operate in Complete mode
* Event time for a real-world example should be derived off an actual file date - we substituted it with processing timestamp for the purposes of the demo. 
    
The following is a batch transform:

     val finalDf= baseDf
               .select($"Area", $"Item", $"Element", $"year", $"quantity")              // select what we care about
               .groupBy($"Area", $"Item", $"year")                                      // first group-by is needed, since "Element" is not unique per ("Area", "Item", "year")
               .agg(sum($"quantity").as("quantity"))                                    // compute total quantity of elements of ("Area", "Item", "year") tuples
               .groupBy($"Area", $"Item")                                               // second group by for final grouping
               .agg(collect_list(struct("year", "quantity")) as "year_quantity")        // create column with a sequence of tupels
               .withColumn("Growth Rate", growthRateUdf(sortUdf(col("year_quantity")))) // compute growth rate with OLS on a sorted list of quantities
               .drop("year_quantity")                                                   // drop aux column
               .orderBy($"Growth Rate".desc)                                            // sort growth rates

The following is a streaming transform:

     val finalDf= baseStream
              .withColumn("eventTime", lit(current_timestamp()))                       // construct sample event time column (better alternative would be file creation timestamp
              .select($"Area", $"Item", $"Element", $"year", $"quantity", $"eventTime")// select columns we care about
              .groupBy(window($"eventTime", "5 minutes"), $"Area", $"Item")            // group by in a window
              .agg(collect_list(struct("year", "quantity")) as "year_quantity")        // create column with a sequence of tupels
              .withColumn("Growth Rate", growthRateUdf(sortUdf(col("year_quantity")))) // compute growth rate with OLS on a sorted list of quantities
              .drop("year_quantity")                                                   // drop aux column
              .orderBy($"Growth Rate".desc)                                            // sort growth rates

## Build Jar for Spark

    sbt assembly

## Submit Spark Batch Example


    spark-submit --class SparkBatchExample target/scala-2.11/regression-demo-assembly-1.0.jar


#### Sample Output

     +---------------+--------------------+-------------------+
     |           Area|                Item|        Growth Rate|
     +---------------+--------------------+-------------------+
     |China, mainland|                Beer|0.14892198610785076|
     |China, mainland| Oranges, Mandarines|0.11424263130971123|
     |China, mainland| Apples and products|0.10104608632165121|
     |China, mainland| Alcoholic Beverages|0.09575903865117427|
     |China, mainland|     Molluscs, Other|0.09033641076816891|
     |China, mainland|Cassava and products| 0.0872411814045086|
     |China, mainland|Fruits - Excludin...|0.08574357853585529|
     |China, mainland|     Freshwater Fish|0.08174428606294781|
     |China, mainland|Aquatic Products,...|0.07886568461172407|
     |China, mainland|      Aquatic Plants|0.07732679571883032|
     |China, mainland|        Poultry Meat| 0.0767219579438259|
     |          India|Tomatoes and prod...|0.07545712702845597|
     |China, mainland|Milk - Excluding ...|0.07292368978591832|
     |China, mainland|       Fruits, Other|0.07164462796547556|
     |China, mainland|                Eggs|0.07145679376915558|
     |China, mainland|       Fish, Seafood|0.06536485533321869|
     |China, mainland|                Meat|0.06413270203427049|
     |         Brazil|                Beer|0.06316893064877271|
     |China, mainland|   Vegetables, Other|0.06056896098994877|
     |China, mainland|          Vegetables|0.05907498400090992|
     +---------------+--------------------+-------------------+
     only showing top 20 rows

## Spark Streaming 
Spark streaming supports only one level of aggregations, so 
 
 
#### Submit Spark Streaming

    spark-submit --class SparkStreamExample target/scala-2.11/regression-demo_2.11-1.0.jar
    
    -----------------+--------------------+--------------------+--------------------+
    |              window|                Area|                Item|         Growth Rate|
    +--------------------+--------------------+--------------------+--------------------+
    |[2018-10-07 09:50...|     China, mainland|                Beer| 0.14892198610785076|
    |[2018-10-07 09:50...|     China, mainland| Oranges, Mandarines| 0.11424263130971123|
    |[2018-10-07 09:50...|     China, mainland| Apples and products| 0.10104608632165121|
    |[2018-10-07 09:50...|     China, mainland| Alcoholic Beverages| 0.09575903865117427|
    |[2018-10-07 09:50...|     China, mainland|     Molluscs, Other| 0.09033641076816891|
    |[2018-10-07 09:50...|     China, mainland|Cassava and products|  0.0872411814045086|
    |[2018-10-07 09:50...|     China, mainland|Fruits - Excludin...| 0.08574357853585529|
    |[2018-10-07 09:50...|     China, mainland|     Freshwater Fish| 0.08174428606294781|
    |[2018-10-07 09:50...|     China, mainland|Aquatic Products,...| 0.07886568461172407|
    |[2018-10-07 09:50...|     China, mainland|      Aquatic Plants| 0.07732679571883032|
    |[2018-10-07 09:50...|     China, mainland|        Poultry Meat|  0.0767219579438259|
    |[2018-10-07 09:50...|               India|Tomatoes and prod...| 0.07545712702845597|
    |[2018-10-07 09:50...|     China, mainland|       Fruits, Other| 0.07164462796547556|
    |[2018-10-07 09:50...|     China, mainland|       Fish, Seafood| 0.06536485533321869|
    |[2018-10-07 09:50...|     China, mainland|                Meat| 0.06413270203427049|
    |[2018-10-07 09:50...|              Brazil|                Beer| 0.06316893064877271|
    |[2018-10-07 09:50...|     China, mainland|             Pigmeat| 0.05846244431835814|
    |[2018-10-07 09:50...|              Brazil| Alcoholic Beverages|0.056807534079711466|
    |[2018-10-07 09:50...|Iran (Islamic Rep...|Fruits - Excludin...| 0.05574543467132542|
    |[2018-10-07 09:50...|               India|Potatoes and prod...|0.055547994758002796|
    +--------------------+--------------------+--------------------+--------------------+
    
