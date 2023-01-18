package Joins

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object CoPartition {

  val spark = SparkSession.builder()
    .appName("Pre-partitioning")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  // deactivate broadcast joins
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  /*
    addColumns(initialTable, 3) => dataframe with columns "id", "newCol1", "newCol2", "newCol3"
    Simulated operation on a joined dataframe
   */
  def addColumns[T](df: Dataset[T], n: Int): DataFrame = {
    val newColumns = (1 to n).map(i => s"id * $i as newCol$i")
    df.selectExpr(("id" +: newColumns): _*)
  }

  // Data generation
  val initialTable = spark.range(1, 10000000).repartition(10)
  val narrowTable = spark.range(1, 5000000).repartition(7)

  // Non optimized
  val wideTable = addColumns(initialTable, 30)
  val join1 = wideTable.join(narrowTable, "id")


  // Optimized
  val altNarrow = narrowTable.repartition($"id") // use a HashPartitioner
  val altInitial = initialTable.repartition($"id")
  // join on co-partitioned DFs
  val prejoin2 = altInitial.join(altNarrow, "id")
  val join2 = addColumns(prejoin2, 30)


  def main(args: Array[String]): Unit = {
    join1.show()
    //join2.show()
    Thread.sleep(10000000)
  }
}
