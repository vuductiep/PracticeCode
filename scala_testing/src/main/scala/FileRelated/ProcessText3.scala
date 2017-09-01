import java.io.{File, PrintWriter}
import java.sql.{Connection, DriverManager, SQLException}

import sys.process._
import scala.io.Source

object ProcessText3 {
    def main(args: Array[String]) : Unit = {
        val pathfile = "segmentindex.sql"
        //        val pathfile = "E:\\project\\Mapmatching\\test.txt"
        val file = new File(pathfile)
        val bufferedSource = Source.fromFile(pathfile)
        val pw = new PrintWriter(new File("processed_" + file.getName + ".csv" ))


        val mycon : Connection = get_mySqlConnect
        val sql = "select (speedlimit, roadname) from mapdata  where mapid=?"
        val pst = mycon.prepareStatement(sql)


        println("Start converting")
        pw.write("segment_id, link_id, vtx_1, vty_1, vtx_2, vty_2, mesh1, mesh2\n")
        var count = 0
        //        for ( line <- bufferedSource.getLines() ) {
        for ( line <- bufferedSource.getLines().drop(38) ) {
            count = count + 1
            val split = line.split(",")

            val link_id = split(1).toLong

            pst.setLong(1,link_id)
            val rs = pst.executeQuery()
            rs.next()
            val speedlimit = rs.getString("speedlimit")
            val roadname = rs.getString("roadname")
            val newLine = split(1) + "," + split(2) + "," + split(3) + "," + split(6) + "," + speedlimit + "," + roadname

            pw.write(newLine)
        }
        bufferedSource.close()
        pw.close()
        printf("Process %d lines done \n", count)
    }


    private def get_mySqlConnect (): Connection  = {
        var conn : Connection = null
        val url = "jdbc:mysql://localhost/test_db?useSSL=false"
        val userId = "tiep"
        val userPass = "1234"
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


