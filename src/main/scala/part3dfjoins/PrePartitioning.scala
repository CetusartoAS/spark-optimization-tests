package part3dfjoins

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object PrePartitioning {

  val spark = SparkSession.builder()
    .appName("Pre-partitioning")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  // deactivate broadcast joins
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  /*
    addColumns(initialTable, 3) => dataframe with columns "id", "newCol1", "newCol2", "newCol3"
   */
  def addColumns[T](df: Dataset[T], n: Int): DataFrame = {
    val newColumns = (1 to n).map(i => s"id * $i as newCol$i")
    df.selectExpr(("id" +: newColumns): _*)
  }

  // don't touch this
  val initialTable = spark.range(1, 10000000).repartition(10) // RoundRobinPartitioning(10)
  val narrowTable = spark.range(1, 5000000).repartition(7) // RoundRobinPartitioning(7)

  // scenario 1
  val wideTable = addColumns(initialTable, 30)
  val join1 = wideTable.join(narrowTable, "id")
  join1.explain()


  val altNarrow = narrowTable.repartition($"id") // use a HashPartitioner
  val altInitial = initialTable.repartition($"id")
  // join on co-partitioned DFs
  val join2 = altInitial.join(altNarrow, "id")
  val result2 = addColumns(join2, 30)
  result2.explain()
  // println(result2.count()) // 6s



  def main(args: Array[String]): Unit = {

  }
}
