package FileRelated

import java.io.{File, PrintWriter}
import java.sql.{Connection, DriverManager, SQLException}

import scala.io.Source

object ReadText {
    def main(args: Array[String]) : Unit = {
//        val pathfile = "C:\\Users\\tiep\\Downloads\\db_mapmatching.sql"
        val pathfile = "E:\\project\\Mapmatching\\db\\vertex_index_2.csv"
        val file = new File(pathfile)
        val bufferedSource = Source.fromFile(pathfile)
        val pw = new PrintWriter(new File("E:\\project\\Mapmatching\\preprocessed_" + file.getName ))

        var count = 0
        for ( line <- bufferedSource.getLines() ) {
            count = count + 1
            val newLine = line.trim
            pw.write(newLine+"\n")
//            if  ( line.contains("vertexindex") )      count = count + 1
        }
        bufferedSource.close()
        pw.close()
        printf("Process %d lines done ", count)
    }

}