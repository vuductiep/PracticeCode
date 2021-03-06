import java.io.{File, PrintWriter}
import java.sql.{Connection, DriverManager, SQLException}

import scala.io.Source

object ProcessText {
    def main(args: Array[String]) : Unit = {
        val pathfile = "E:\\project\\Mapmatching\\segmentindex.sql"
//        val pathfile = "E:\\project\\Mapmatching\\test.txt"
        val file = new File(pathfile)
        val bufferedSource = Source.fromFile(pathfile)
        val pw = new PrintWriter(new File("E:\\project\\Mapmatching\\preprocessed_" + file.getName ))

        var count = 0
//        for ( line <- bufferedSource.getLines() ) {
        for ( line <- bufferedSource.getLines().drop(44) ) {
            count = count + 1
            val newLine = line.replace(";", ",\n").replace("INSERT INTO `vertexdata` VALUES", "")
            pw.write(newLine)
        }
        bufferedSource.close()
        pw.close()
        printf("Process %d lines done ", count)
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