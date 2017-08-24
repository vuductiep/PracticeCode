package MixTest

import java.sql.{Connection, DriverManager, SQLException}

object DBTest {
    def main(args: Array[String]) {
        println("Testing mysql db  with scala:")
        val mycon : Connection = get_mySqlConnect
        val sql = "Select * from method_info"
//        val sql = "select * from vertexdata limit 10"
        val stm = mycon.createStatement
        val rs = stm.executeQuery(sql)

        while ( {rs.next()}){
            val sc = rs.getString("id")
            println(sc)
        }
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

    private def get_mySqlConnect2 = {
        var conn : Connection = null
        val driver = "com.mysql.jdbc.Driver"
//        val url = "jdbc:mysql://localhost/bigdata?useSSL=false"
//        val userId = "tiep"
//        val userPass = "1234"
        val url = "jdbc:mysql://165.132.138.238:3306/db_mapmatching"
        val userId = "hduser"
        val userPass = "h"

        try {
            Class.forName(driver)
        } catch {
            case e: ClassNotFoundException =>
                println("Driver Loading Failed " + e.getStackTrace)
            case etc: Exception =>
                println(etc.getMessage)
                etc.printStackTrace()
        }

        try {
            conn = DriverManager.getConnection(url, userId, userPass)
        } catch {
            case e: SQLException =>
                println("SQLException : " + e.getMessage)
        }
        conn
    }
}

