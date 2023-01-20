package Joins

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object CoPartition {

  val spark = SparkSession.builder()
    .appName("CoPartitioning")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // deactivate broadcast joins
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


  // Data generation
  val firstTable = spark.range(1, 10000000).repartition(10)
  val secondTable = spark.range(1, 5000000).repartition(7)

  // Non optimized
  val join1 = firstTable.join(secondTable, "id")


  // Optimized
  val altSecond = secondTable.repartition($"id")
  val altFirst = firstTable.repartition($"id")
  // join on co-partitioned DFs
  val join2 = altFirst.join(altSecond, "id")


  def main(args: Array[String]): Unit = {
    // Recommended to test one at a time
    join1.show()
    join2.show()
    Thread.sleep(10000000)
  }
}
