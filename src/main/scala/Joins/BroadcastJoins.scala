package Joins

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object BroadcastJoins {

  val spark = SparkSession.builder()
    .appName("Broadcast Joins")
    .master("local[*]")
    .getOrCreate()
  // Deactivate auto broadcast for explanation
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


  val sc = spark.sparkContext

  /** Data Creation * */

  val rows = sc.parallelize(List(
    Row(0, "zero"),
    Row(1, "first"),
    Row(2, "second"),
    Row(3, "third")
  ))

  val rowsSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("order", StringType)
  ))

  // Very small table
  val lookupTable: DataFrame = spark.createDataFrame(rows, rowsSchema)

  // large table
  val table = spark.range(1, 100000000) // column is "id"



  // Brute force join
  val joined = table.join(lookupTable, "id")

  // Optimized join
  val joinedSmart = table.join(broadcast(lookupTable), "id")

  // deactivate auto-broadcast

  def main(args: Array[String]): Unit = {
    //Recommended to test one at a time
//    joined.show()
    joinedSmart.show()

    Thread.sleep(1000000)
  }
}
