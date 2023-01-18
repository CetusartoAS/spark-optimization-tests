package Joins

import generator.DataGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object SkewedJoins {

  val spark = SparkSession.builder()
    .appName("Skewed Joins")
    .master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1) // deactivate broadcast joins
    .getOrCreate()


  import spark.implicits._

  val laptops = Seq.fill(40000)(DataGenerator.randomLaptop()).toDS
  val laptopOffers = Seq.fill(100000)(DataGenerator.randomLaptopOffer()).toDS


  // Bruteforce Method
  val joined = laptops.join(laptopOffers, Seq("make", "model"))
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))


  //Salting method

  val skewFactor = 5
  val laptops2 = laptops.withColumn("salt", explode(lit((1 to skewFactor).toArray))) // Exploded table
  val laptopOffers2 = laptopOffers.withColumn("salt", (rand() * skewFactor + 1).cast("int")) // Salt assigned randomly (100% of the times will match with their correct make and model)
  val joined2 = laptops2.join(laptopOffers2, Seq("make", "model", "salt"))
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))

  def main(args: Array[String]): Unit = {
    //joined.show()
    //joined2.show()
    laptops2.groupBy("salt").agg(count("*")).show()
    laptopOffers2.groupBy("salt").agg(count("*")).show()

    Thread.sleep(10000000)
  }
}
