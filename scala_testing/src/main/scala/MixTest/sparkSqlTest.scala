package MixTest

import org.apache.spark.sql.SparkSession

object sparkSqlTest{
    def main(args: Array[String]) : Unit = {
        val spark = SparkSession.builder
            .master("local")
            .appName("Spark Session _SQL test ")
            .getOrCreate()

        import spark.implicits._  // beware of the "spark" here, it is the SparkSession we just created

        val df = spark.read.option("header","true").csv("data/NW-Orders.csv")

        df.cache()
        df.printSchema()
        df.take(5).foreach(println)
        df.select("OrderID", "CustomerID").take(5).foreach(println)
        df.select($"OrderID"+1, $"CustomerID").take(5).foreach(println)

        df.filter($"EmpliyeeID" < 5).show()

        df.createOrReplaceTempView("NWOrders")
        val sqlDF = spark.sql("SELECT * from NWOrders where EmpliyeeID=5 and ShipCuntry='USA'")
        sqlDF.show()

        spark.stop()
    }
}
