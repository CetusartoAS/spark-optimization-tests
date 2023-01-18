package part3dfjoins

import generator.DataGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SkewedJoins {

  val spark = SparkSession.builder()
    .appName("Skewed Joins")
    .master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1) // deactivate broadcast joins
    .getOrCreate()


  import spark.implicits._

  /*
    An online store selling gaming laptops.
    2 laptops are "similar" if they have the same make & model, but proc speed within 0.1

    For each laptop configuration, we are interested in the average sale price of "similar" models.

    Acer Predator 2.9Ghz aylfaskjhrw -> average sale price of all Acer Predators with CPU speed between 2.8 and 3.0 GHz
   */

  val laptops = Seq.fill(40000)(DataGenerator.randomLaptop()).toDS
  val laptopOffers = Seq.fill(100000)(DataGenerator.randomLaptopOffer()).toDS

  val joined = laptops.join(laptopOffers, Seq("make", "model"))
    .filter(abs(laptopOffers.col("procSpeed") - laptops.col("procSpeed")) <= 0.1)
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))

  val laptops2 = laptops.withColumn("procSpeed", explode(array($"procSpeed" - 0.1, $"procSpeed", $"procSpeed" + 0.1)))
  val joined2 = laptops2.join(laptopOffers, Seq("make", "model", "procSpeed"))
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))

  def main(args: Array[String]): Unit = {
//    joined.show()
//    joined2.show()
    Thread.sleep(10000000)
  }
}
