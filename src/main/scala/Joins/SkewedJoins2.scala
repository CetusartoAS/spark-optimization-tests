package Joins

import generator.DataGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SkewedJoins2 {

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
    //show skewness

    laptops.groupBy("make", "model").agg(count("*").as("count")).orderBy(col("count").desc).show()
    println("Laptops" + "\n\n\n\n\n\n\n\n\n")

    laptopOffers.groupBy("make", "model").agg(count("*").as("count")).orderBy(col("count").desc).show()
    println("Laptops Offers" + "\n\n\n\n\n\n\n\n\n")


    laptops2.groupBy("make", "model", "salt").agg(count("*").as("count")).orderBy(col("count").desc).show()
    println("Laptops 2" + "\n\n\n\n\n\n\n\n\n")

    laptopOffers2.groupBy("make", "model", "salt").agg(count("*").as("count")).orderBy(col("count").desc).show()
    println("Laptops Offers 2" + "\n\n\n\n\n\n\n\n\n")


    //    joined.show()
    //    joined2.show()
    spark.stop()
    Thread.sleep(10000)
  }
}
