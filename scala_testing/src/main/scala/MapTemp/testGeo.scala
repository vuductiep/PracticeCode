import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object testGeo {

    var inputFile = "E:\\project\\Mapmatching\\rawdata\\MOCT_LINK.shp" // road shape file
    var inputPath = "E:\\project\\Mapmatching\\rawdata\\"
    var outputFile = "E:\\project\\Mapmatching\\preprocessed_outdata\\spark_vertex_index"
    val header = "segment_id,link_id,vtx_1,vty_1,vtx_2,vty_2,mesh1,mesh2\n"
    val header1 = "segment_id,link_id,\n"
    val header2 = "vtx_1,vty_1,vtx_2,vty_2,mesh1,mesh2\n"

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .master("local[2]")
            .appName("Spark test geo ")
            .getOrCreate()

        import spark.implicits._

        try {
            if ((!args(0).isEmpty) & (!args(1).isEmpty)) {
                inputFile = args(0)
                outputFile = args(1)
            }
        } catch {
            case ex: ArrayIndexOutOfBoundsException => {
                println("No arguments found, use default input, output path")
            }
        }


        val rawData = spark.read.format("magellan").load(inputPath)
        rawData.printSchema()

        val geoData = rawData.select($"polyline", $"metadata").where("polyline IS NOT NULL")

        geoData.createGlobalTempView("RawGeoData")
        val geoDataPart = spark.sql("select * from global_temp.RawGeoData")

        val res = geoDataPart.map(row => {
            val poly = row.getAs[magellan.PolyLine]("polyline")
            val meta = row.getAs[Map[String,String]]("metadata")
            val linkId = meta.get("LINK_ID").getOrElse("").toString

            val xArr = poly.xcoordinates
            val yArr = poly.ycoordinates

            var lineBuffer = new String()

            val maxEntry = xArr.length - 1
            for (i <- 1 to maxEntry) {
                val vt1_x = xArr(i - 1)
                val vt1_y = yArr(i - 1)
                val mesh1 = getMesh( vt1_x, vt1_y)
                val vt2_x = xArr(i)
                val vt2_y = yArr(i)
                val mesh2 = getMesh( vt2_x, vt2_y)

                var line  = new String()
                if (i == maxEntry)
                    line ="%s,%3.6f,%3.6f,%3.6f,%3.6f,%d,%d".format(linkId, vt1_x, vt1_y, vt2_x, vt2_y, mesh1, mesh2)
                else
                    line ="%s,%3.6f,%3.6f,%3.6f,%3.6f,%d,%d\n".format( linkId, vt1_x, vt1_y, vt2_x, vt2_y, mesh1, mesh2)

                lineBuffer + line
            }
            lineBuffer
        })


        res.rdd.saveAsTextFile(outputFile + new SimpleDateFormat("yyyy-mm-dd hh-mm-ss").format( System.currentTimeMillis() ))

        spark.stop()
    }

    def getMesh(x: Double, y: Double): Int = {
        val n_x = Math.floor(x * 100.0) / 100.0
        val n_y = Math.floor(y * 100.0) / 100.0
        ((n_y - 33.0) * 5300.0 + (n_x - 125.8) * 100.0).toInt
    }

    def addHeaderToRdd(sparkCtx: SparkContext, lines: RDD[String], header: String): RDD[String] = {
        val headerRDD = sparkCtx.parallelize(List((-1L, header)))     // We index the header with -1, so that the sort will put it on top.
        val pairRDD = lines.zipWithIndex()
        val pairRDD2 = pairRDD.map(t => (t._2, t._1))
        val allRDD = pairRDD2.union(headerRDD)
        val allSortedRDD = allRDD.sortByKey()
        return allSortedRDD.values
    }
}