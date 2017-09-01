import java.io.{File, PrintWriter}
import java.sql.{Connection, DriverManager, SQLException}

import sys.process._
import scala.io.Source

object ProcessText2CSV {
    def main(args: Array[String]) : Unit = {
        val pathfile = "processed_segmentindex.sql.csv"
        //val pathfile = "testdata.csv"
        val file = new File(pathfile)
        val bufferedSource = Source.fromFile(pathfile)
        val pw = new PrintWriter(new File("processed_" + file.getName + ".csv" ))


        val mycon : Connection = get_mySqlConnect
        val sql0 = "CREATE TEMPORARY TABLE IF NOT EXISTS mapdulieu AS ( SELECT db.mapid as mapid, db.speedlimit as speedlimit, db.roadname as roadname FROM mapdata db);"
        val sql = "select speedlimit,roadname from mapdulieu where mapid=?;"
        val pst = mycon.prepareStatement(sql)

        println("Load temporary data into memory")
        val st = mycon.createStatement()
        st.executeUpdate(sql0)

        println("Start converting")
        pw.write("mapid, vtx_x, vtx_y, mesh, speedlimit, roadname\n")
        var count = 0
        //        for ( line <- bufferedSource.getLines() ) {
        for ( line <- bufferedSource.getLines().drop(38) ) {
            count = count + 1
            val split = line.trim.split(",")

            //val link_id = split(1).replace(" ","").toLong
            val link_id = split(1).trim.toLong

            pst.setLong(1,link_id)
            val rs = pst.executeQuery()
            rs.next()
            val speedlimit = rs.getString("speedlimit")
            val roadname = rs.getString("roadname")
            val newLine = split(1) + "," + split(2) + "," + split(3) + "," + split(6) + "," + speedlimit + "," + roadname + "\n"

            pw.write(newLine)
        }
        bufferedSource.close()
        pw.close()
        printf("Process %d lines done \n", count)
    }


    private def get_mySqlConnect (): Connection  = {
        var conn : Connection = null
        val url = "jdbc:mysql://localhost/mapdb?useSSL=false"
        val userId = "root"
        val userPass = "bigdata"
        val driver = "com.mysql.jdbc.Driver"
        //        val url = "jdbc:mysql://165.132.138.238:3306/db_mapmatching"
        //        val userId = "hduser"
        //        val userPass = "h"

        try {
            Class.forName(driver)
            conn = DriverManager.getConnection(url, userId, userPass)
        } catch {
            case e: ClassNotFoundException =>
                println("Driver Loading Failed " + e.getStackTrace)
            case e: SQLException =>
                println("SQLException : " + e.getMessage)
            case etc: Exception =>
                println(etc.getMessage)
                etc.printStackTrace()
        }
        conn
    }
}

