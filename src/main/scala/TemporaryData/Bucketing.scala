package TemporaryData

import org.apache.spark.sql.{SaveMode, SparkSession}

object Bucketing {

  val spark = SparkSession.builder()
    .appName("Bucketing")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  // deactivate broadcasting
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


  // bucketing for groups
  val flightsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/data/flights")
    .repartition(2)

  val mostDelayed = flightsDF
    .filter("origin = 'DEN' and arrdelay > 1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last)


  /** Bucketed solution **/


  flightsDF.write
    .partitionBy("origin")
    .bucketBy(4, "dest", "carrier")
    .mode(SaveMode.Overwrite)
    .saveAsTable("flights_bucketed")

  val flightsBucketed = spark.table("flights_bucketed")
  val mostDelayed2 = flightsBucketed
    .filter("origin = 'DEN' and arrdelay > 1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last)


  def main(args: Array[String]): Unit = {

    //Recommended to test one at a time
   //mostDelayed.show()
    mostDelayed2.show()

    Thread.sleep(100000000)
  }

}
