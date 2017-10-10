import java.io.File
import java.net.URL
import java.text.SimpleDateFormat
import java.util.{HashMap, Map}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.geotools.data.DataStoreFinder
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter


package object MapTemp

object PreprocessingSpark{

    var inputFile = "E:\\project\\Mapmatching\\rawdata\\MOCT_LINK.shp" // road shape file
    var outputFile = "E:\\project\\Mapmatching\\preprocessed_outdata\\spark_vertex_index"
    val header = "segment_id,link_id,vtx_1,vty_1,vtx_2,vty_2,mesh1,mesh2\n"
    val header1 = "segment_id,link_id,\n"
    val header2 = "vtx_1,vty_1,vtx_2,vty_2,mesh1,mesh2\n"

    def main (args: Array[String]): Unit ={
        val spark = SparkSession.builder
            .master("local[2]")
            .appName("Spark Preprocessing Geodata ")
            .getOrCreate()

        try {
            if ( (! args(0).isEmpty) & (! args(1).isEmpty) ) {
                inputFile = args(0)
                outputFile = args(1)
            }
        } catch {
            case ex : ArrayIndexOutOfBoundsException => {
                println("No arguments found, use default input, output path")
            }
        }

        val linkRdd = spark.sparkContext.parallelize( header1 +: getLinkId(inputFile) ,1).zipWithIndex().map( t => (t._2, t._1))
//        linkRdd.saveAsTextFile(outputFile + "_linkid " + new SimpleDateFormat("yyyy-mm-dd hh-mm-ss").format( System.currentTimeMillis() ))


        val dataIndexRdd = getData(spark, inputFile)

        val finalRes = linkRdd.join(dataIndexRdd,1)

        finalRes.values.map( t => {t._1 + t._2} ).saveAsTextFile(outputFile + new SimpleDateFormat("yyyy-mm-dd hh-mm-ss").format( System.currentTimeMillis() ))

//        val rddResult = spark.sparkContext.parallelize(strResult,1)
//        val finalRes = addHeaderToRdd(spark.sparkContext, rddResult, header)
//        finalRes.saveAsTextFile(outputFile + new SimpleDateFormat("yyyy-mm-dd hh-mm-ss").format( System.currentTimeMillis() ))
    }

    def getData(spark: SparkSession, filepath: String)  = {
        val shapefileRDD = ShapefileReader.readToGeometryRDD (spark.sparkContext, inputFile)

        var vt1_x = .0
        var vt1_y = .0
        var vt2_x = .0
        var vt2_y = .0
        var segment_id = 0
        var mesh1 = 0
        var mesh2 = 0

        val header2Rdd = spark.sparkContext.parallelize(List(header2))
        val rddResult = shapefileRDD.rdd.map(lineString => {
            var link_id = ""
            var lineBuffer= new String()
            val coors = lineString.getCoordinates

            val maxEntry = coors.length - 1
            for (i <- 1 to maxEntry) {
                segment_id += 1
                vt1_x = coors(i - 1).x
                vt1_y = coors(i - 1).y
                mesh1 = getMesh(coors(i - 1).x, coors(i - 1).y)
                vt2_x = coors(i).x
                vt2_y = coors(i).y
                mesh2 = getMesh(coors(i).x, coors(i).y)

                var line = new String()
                if (i == maxEntry)
                    line = "%3.6f,%3.6f,%3.6f,%3.6f,%d,%d".format(vt1_x, vt1_y, vt2_x, vt2_y, mesh1, mesh2)
                else
                    line = "%3.6f,%3.6f,%3.6f,%3.6f,%d,%d\n".format(vt1_x, vt1_y, vt2_x, vt2_y, mesh1, mesh2)

                lineBuffer += line
            }
            lineBuffer
        })

        val rddHeaderResult = header2Rdd.union(rddResult)

        rddHeaderResult.zipWithIndex().map( t => (t._2, t._1))
    }

    def getLinkId(filepath: String)  = {
        val file = new File(filepath)
        val map : Map[String, URL]  = new HashMap()
        map.put("url", file.toURI.toURL)

        val dataStore = DataStoreFinder.getDataStore(map)
        val resTypeName = dataStore.getTypeNames()
        println(resTypeName(0))
        val typeName = resTypeName(0)

        val source = dataStore.getFeatureSource(typeName)
        val filter = Filter.INCLUDE
        val collection = source.getFeatures(filter)

        val featuresArray = collection.toArray
//        val featuresIter = collection.features()

        var segment_id = 0
        featuresArray.map( featureElement => {
            val feature =  featureElement.asInstanceOf[SimpleFeature]
            var link_id = ""
            val t = dataStore.getTypeNames()(0)
            val featureSource = dataStore.getFeatureSource(t)
            val schema = featureSource.getSchema
            val geomType = schema.getGeometryDescriptor.getType.getBinding.getName

            var lineBuffer = new String()

            geomType match {
                case "com.vividsolutions.jts.geom.LineString" | "com.vividsolutions.jts.geom.MultiLineString" => {
                    link_id = feature.getProperty("LINK_ID").getValue.toString
                    val geom = feature.getAttribute("the_geom").asInstanceOf[com.vividsolutions.jts.geom.MultiLineString]
                    val coors = geom.getCoordinates
                    val maxEntry = coors.length - 1
                    for (i <- 1 to maxEntry) {
                        segment_id += 1
                        var line = new String()
                        if (i == maxEntry)
                            line = "%d,%s,".format(segment_id, link_id)
                        else
                            line = "%d,%s,\n".format(segment_id, link_id)

                        lineBuffer += line
                    }
                }
            }
            lineBuffer
        } )
    }

    def getGeometryCollection(filepath: String) = {
        val file = new File(filepath)
        val map : Map[String, URL]  = new HashMap()
        map.put("url", file.toURI.toURL)

        val dataStore = DataStoreFinder.getDataStore(map)
        val resTypeName = dataStore.getTypeNames()
        println(resTypeName(0))
        val typeName = resTypeName(0)

        val source = dataStore.getFeatureSource(typeName)
        val filter = Filter.INCLUDE
        val collection = source.getFeatures(filter)
        collection
    }

    def readGeometry(filepath: String) : Array[String]  = {
        val file = new File(filepath)
        val map : Map[String, URL]  = new HashMap()
        map.put("url", file.toURI.toURL)

        val dataStore = DataStoreFinder.getDataStore(map)
        val resTypeName = dataStore.getTypeNames()
        println(resTypeName(0))
        val typeName = resTypeName(0)

        val source = dataStore.getFeatureSource(typeName)
        val filter = Filter.INCLUDE
        // ECQL.toFilter("BBOX(THE_GEOM, 10,20,30,40)")
        val collection = source.getFeatures(filter)
        var vt1_x = .0
        var vt1_y = .0
        var vt2_x = .0
        var vt2_y = .0
        var segment_id = 0
        var mesh1 = 0
        var mesh2 = 0

//        val features = collection.features
        val featuresArray = collection.toArray
//        println("Collection size = " + collection.size())
//        println(collection.getSchema)
//        val kaka = featuresArray(0).asInstanceOf[SimpleFeature]
//        println(kaka.getProperty("LINK_ID"))
//        println(  kaka.getAttribute("the_geom"))

        featuresArray.map( featureElement => {
            val feature =  featureElement.asInstanceOf[SimpleFeature]
            var link_id = ""
            val t = dataStore.getTypeNames()(0)
            val featureSource = dataStore.getFeatureSource(t)
            val schema = featureSource.getSchema
            val geomType = schema.getGeometryDescriptor.getType.getBinding.getName

            var lineBuffer = new String()

            geomType match {
                case "com.vividsolutions.jts.geom.LineString" | "com.vividsolutions.jts.geom.MultiLineString" => {
                    // do line thing
                    link_id = feature.getProperty("LINK_ID").getValue.toString
                    val geom = feature.getAttribute("the_geom").asInstanceOf[com.vividsolutions.jts.geom.MultiLineString]
                    val coors = geom.getCoordinates

                    for (i <- 1 to (coors.length - 1)) {
                        segment_id += 1
                        vt1_x = coors(i - 1).x
                        vt1_y = coors(i - 1).y
                        mesh1 = getMesh(coors(i - 1).x, coors(i - 1).y)
                        vt2_x = coors(i).x
                        vt2_y = coors(i).y
                        mesh2 = getMesh(coors(i).x, coors(i).y)
                        //                        val line = String.format("%d,%s,%3.6f,%3.6f,%3.6f,%3.6f,%d,%d\n", segment_id, link_id, vt1_x, vt1_y, vt2_x, vt2_y, mesh1, mesh2)
                        val line = "\n%d,%s,%3.6f,%3.6f,%3.6f,%3.6f,%d,%d".format(segment_id, link_id, vt1_x, vt1_y, vt2_x, vt2_y, mesh1, mesh2)
                        //                        println("%d,%s,%3.6f,%3.6f,%3.6f,%3.6f,%d,%d\n".format(segment_id, link_id, vt1_x, vt1_y, vt2_x, vt2_y, mesh1, mesh2))
                        //System.out.print(line);
                        //                        fVertexOutput.write(line)
                        lineBuffer += line
                    }
                }
            }
            //                System.out.print(": ");
            //                System.out.println(feature.getDefaultGeometryProperty().getValue());
            lineBuffer
        } )
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

class LinkEntry(val segment_id: Long, val link_id: String, val vtx_1: Double, val vty_1: Double,
                  val vtx_2: Double, val vty_2: Double, val mesh1: Long, val mesh2: Long)
