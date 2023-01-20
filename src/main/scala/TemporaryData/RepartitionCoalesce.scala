package part4partitioning

import org.apache.spark.sql.SparkSession

object RepartitionCoalesce {

  val spark = SparkSession.builder()
    .appName("Repartition and Coalesce")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  val numbers = sc.parallelize(1 to 10000000)


  val repartitionedNumbers = numbers.repartition(2)
  repartitionedNumbers.count()

  val coalescedNumbers = numbers.coalesce(2) // for a smaller number of partitions
  coalescedNumbers.count()


  def main(args: Array[String]): Unit = {
    Thread.sleep(10000000)
  }
}